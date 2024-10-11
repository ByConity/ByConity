/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "Storages/HDFS/ReadBufferFromByteHDFS.h"
#include <Interpreters/RemoteReadLog.h>

#if USE_HDFS

#include "Common/ProfileEvents.h"
#include "Common/Stopwatch.h"
#include "Common/Exception.h"
#include "common/sleep.h"

#include <fcntl.h>
#include <hdfs/hdfs.h>

namespace ProfileEvents
{
    extern const int HdfsFileOpen;
    extern const int HdfsFileOpenMicroseconds;
    extern const int ReadBufferFromHdfsRead;
    extern const int HDFSReadElapsedMicroseconds;
    extern const int ReadBufferFromHdfsReadBytes;
    extern const int HDFSSeek;
    extern const int HDFSSeekElapsedMicroseconds;

    extern const int HdfsSlowNodeCount;
    extern const int HdfsFailedNodeCount;
    extern const int HdfsGetBlkLocMicroseconds;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int UNKNOWN_FILE_SIZE;
    extern const int NETWORK_ERROR;
    extern const int CANNOT_OPEN_FILE;
}

static void ReadBufferFromHdfsCallBack(const hdfsEvent & event)
{
    // LOG_TRACE(getLogger("ReadBufferFromByteHDFS"), "get event {} & {}", event.eventType, event.value);
    switch (event.eventType)
    {
        case Hdfs::Event::HDFS_EVENT_SLOWNODE:
            ProfileEvents::increment(ProfileEvents::HdfsSlowNodeCount, 1);
            break;
        case Hdfs::Event::HDFS_EVENT_FAILEDNODE:
            ProfileEvents::increment(ProfileEvents::HdfsFailedNodeCount, 1);
            break;
        case Hdfs::Event::HDFS_EVENT_GET_BLOCK_LOCATION:
            ProfileEvents::increment(ProfileEvents::HdfsGetBlkLocMicroseconds, event.value / 1000);
            break;
//        case Hdfs::Event::HDFS_EVENT_CREATE_BLOCK_READER:
//            ProfileEvents::increment(ProfileEvents::HdfsCreateBlkReaderMicroseconds, event.value/1000);
//            break;
//        case Hdfs::Event::HDFS_EVENT_DN_CONNECTION: {
////            LOG_INFO(&Logger::get("ReadBufferFromHDFS"), "received event " << event.eventType);
//            ProfileEvents::increment(ProfileEvents::HdfsDnConnectionCount, 1);
//            ProfileEvents::increment(ProfileEvents::HdfsDnConnectionMicroseconds, event.value / 1000);
//            int64_t val = std::max(maxDnConnection, event.value / 1000);
//            ProfileEvents::increment(ProfileEvents::HdfsDnConnectionMax, val - maxDnConnection);
//            maxDnConnection = val;
//            break;
//        }
//        case Hdfs::Event::HDFS_EVENT_READ_PACKET: {
//            ProfileEvents::increment(ProfileEvents::HdfsReadPacketCount, 1);
//            ProfileEvents::increment(ProfileEvents::HdfsReadPacketMicroseconds, event.value / 1000);
//            int64_t val = std::max(maxReadPacket, event.value / 1000);
//            ProfileEvents::increment(ProfileEvents::HdfsReadPacketMax, val - maxReadPacket);
//            maxReadPacket = val;
//            break;
//        }
        default:
            break;
    }
}

static void doWithRetry(std::function<void()> func)
{
    static constexpr size_t max_retry = 3;
    size_t sleep_time_with_backoff_milliseconds = 100;
    for (size_t attempt = 0;; ++attempt)
    {
        bool last_attempt = (attempt + 1 == max_retry);
        try
        {
            func();
            return;
        }
        catch (...)
        {
            if (last_attempt)
                throw;

            sleepForMilliseconds(sleep_time_with_backoff_milliseconds);
            sleep_time_with_backoff_milliseconds *= 2;
        }
    }
}

struct ReadBufferFromByteHDFS::ReadBufferFromHDFSImpl
{
    bool pread {false};
    RemoteReadLog * remote_read_log;
    String remote_read_context;
    size_t read_until_position = 0;

    String hdfs_file_path;
    HDFSBuilderPtr builder;
    HDFSFSPtr fs;
    hdfsFile fin;

    size_t file_offset = 0;

    ReadBufferFromHDFSImpl(
        const String & hdfs_file_path_,
        const HDFSConnectionParams & hdfs_params_,
        size_t read_until_position_,
        const ReadSettings & settings_)
        : pread(settings_.byte_hdfs_pread)
        , remote_read_log(settings_.remote_read_log)
        , remote_read_context(settings_.remote_read_context)
        , read_until_position(read_until_position_)
    {
        Poco::URI uri(hdfs_file_path_);
        hdfs_file_path = uri.getPath();

        builder = hdfs_params_.createBuilder(uri);
        doWithRetry([this] {
            fs = createHDFSFS(builder.get());
        });

        Stopwatch watch;
        doWithRetry([this] {
            ProfileEvents::increment(ProfileEvents::HdfsFileOpen);
            fin = hdfsOpenFile(fs.get(), hdfs_file_path.c_str(), O_RDONLY, 0, 0, 0, ReadBufferFromHdfsCallBack);
            if (!fin)
                throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Fail to open hdfs file {}. Error: {}", hdfs_file_path, std::string(hdfsGetLastError()));
        });
        ProfileEvents::increment(ProfileEvents::HdfsFileOpenMicroseconds, watch.elapsedMicroseconds());
    }

    ~ReadBufferFromHDFSImpl()
    {
        hdfsCloseFile(fs.get(), fin);
        fin = nullptr;
    }

    size_t readImpl(char * to, size_t bytes)
    {
        size_t num_bytes_to_read = bytes;
        if (read_until_position)
        {
            if (read_until_position == file_offset)
                return 0;

            if (read_until_position < file_offset)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to read beyond right offset ({} > {})", file_offset, read_until_position - 1);

            num_bytes_to_read = std::min(bytes, read_until_position - file_offset);
        }

        Stopwatch watch;
        const auto request_time = std::chrono::system_clock::now();
        size_t total_bytes_read = 0;
        do
        {
            int bytes_read = 0;

            if (pread)
                bytes_read = hdfsPRead(fs.get(), fin, to + total_bytes_read, num_bytes_to_read - total_bytes_read);
            else
                bytes_read = hdfsRead(fs.get(), fin, to + total_bytes_read, num_bytes_to_read - total_bytes_read);

            if (bytes_read < 0)
            {
                throw Exception(
                    ErrorCodes::NETWORK_ERROR,
                    "Fail to read from HDFS file path: {}. Error: {}",
                    hdfs_file_path,
                    std::string(hdfsGetLastError()));
            }
            if (bytes_read == 0)
                break;
            total_bytes_read += bytes_read;
        } while (total_bytes_read < num_bytes_to_read);

        auto duration_us = watch.elapsedMicroseconds();
        if (remote_read_log)
            remote_read_log->insert(request_time, hdfs_file_path, file_offset, total_bytes_read, duration_us, remote_read_context);
        file_offset += total_bytes_read;

        ProfileEvents::increment(ProfileEvents::ReadBufferFromHdfsRead, 1);
        ProfileEvents::increment(ProfileEvents::ReadBufferFromHdfsReadBytes, total_bytes_read);
        ProfileEvents::increment(ProfileEvents::HDFSReadElapsedMicroseconds, duration_us);
        return total_bytes_read;
    }

    void seek(off_t offset)
    {
        Stopwatch watch;
        int status = hdfsSeek(fs.get(), fin, offset);
        if (status != 0)
            throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Fail to seek HDFS file: {}, error: {}", hdfs_file_path, std::string(hdfsGetLastError()));

        file_offset = offset;

        ProfileEvents::increment(ProfileEvents::HDFSSeek);
        ProfileEvents::increment(ProfileEvents::HDFSSeekElapsedMicroseconds, watch.elapsedMicroseconds());
    }

    off_t getPosition() const
    {
        return file_offset;
    }

    size_t getFileSize() const
    {
        auto * file_info = hdfsGetPathInfo(fs.get(), hdfs_file_path.c_str());
        if (!file_info)
            throw Exception(ErrorCodes::UNKNOWN_FILE_SIZE, "Cannot find out file size for: {}", hdfs_file_path);
        return file_info->mSize;
    }

    void setReadUntilPosition(size_t position)
    {
        if (position != static_cast<size_t>(read_until_position))
            read_until_position = position;
    }

    void setReadUntilEnd()
    {
        if (read_until_position)
            read_until_position = 0;
    }
};

ReadBufferFromByteHDFS::ReadBufferFromByteHDFS(
    const String & hdfs_file_path_,
    const HDFSConnectionParams & hdfs_params_,
    const ReadSettings & read_settings,
    char * existing_memory_,
    size_t alignment_,
    bool use_external_buffer_,
    off_t read_until_position_,
    std::optional<size_t> file_size_)
    : ReadBufferFromFileBase(use_external_buffer_ ? 0 : read_settings.remote_fs_buffer_size, existing_memory_, alignment_, file_size_)
    , hdfs_file_path(hdfs_file_path_)
    , hdfs_params(hdfs_params_)
    , read_until_position(read_until_position_)
    , settings(read_settings)
    , impl(nullptr)
    , total_network_throttler(settings.remote_throttler)
{
}

ReadBufferFromByteHDFS::~ReadBufferFromByteHDFS() = default;

void ReadBufferFromByteHDFS::initImpl()
{
    chassert(!impl);
    impl = std::make_unique<ReadBufferFromHDFSImpl>(hdfs_file_path, hdfs_params, read_until_position, settings);
}

IAsynchronousReader::Result ReadBufferFromByteHDFS::readInto(char * data, size_t size, size_t read_offset, size_t ignore_bytes)
{
    /**
     * Set `data` to current working and internal buffers.
     * Internal buffer with size `size`. Working buffer with size 0.
     */
    set(data, size);

    bool result = false;

    seek(read_offset, SEEK_SET);
    /**
     * Lazy seek is performed here.
     * In asynchronous buffer when seeking to offset in range [pos, pos + min_bytes_for_seek]
     * we save how many bytes need to be ignored (new_offset - position() bytes).
     */
    if (ignore_bytes)
    {
        ignore(ignore_bytes);
        result = hasPendingData();
        ignore_bytes = 0;
    }

    if (!result)
        result = next();

    /// Required for non-async reads.
    if (result)
    {
        assert(available());
        // nextimpl_working_buffer_offset = offset;
        return { working_buffer.size(), BufferBase::offset(), nullptr };
    }

    return {0, 0, nullptr};
}

bool ReadBufferFromByteHDFS::nextImpl()
{
    if (!impl)
        initImpl();
    int bytes_read = impl->readImpl(internal_buffer.begin(), internal_buffer.size());
    if (bytes_read)
    {
        working_buffer = internal_buffer;
        working_buffer.resize(bytes_read);

        if (total_network_throttler)
            total_network_throttler->add(bytes_read);
        return true;
    }

    return false;
}

off_t ReadBufferFromByteHDFS::seek(off_t offset_, int whence_)
{
    if (whence_ == SEEK_CUR)
        offset_ = getPosition() + offset_;
    else if (whence_ != SEEK_SET)
        throw Exception("Seek expects SEEK_SET or SEEK_CUR as whence", ErrorCodes::BAD_ARGUMENTS);
    assert(offset_ >= 0);

    if (offset_ == getPosition())
        return offset_;

    /// new position still inside working buffer
    /// impl->getPosition() is the file position of the working buffer end
    /// Therefore working buffer corresponds to the file range
    /// [impl->getPosition() - working_buffer.size(), impl->getPosition()]
    if (!impl)
        initImpl();
    if (!working_buffer.empty()
        && size_t(offset_) >= impl->getPosition() - working_buffer.size()
        && offset_ <= impl->getPosition())
    {
        pos = working_buffer.end() - (impl->getPosition() - offset_);
        assert(pos >= working_buffer.begin());
        assert(pos <= working_buffer.end());
        return getPosition();
    }

    resetWorkingBuffer();
    impl->seek(offset_);
    return getPosition();
}

off_t ReadBufferFromByteHDFS::getPosition()
{
    if (!impl)
        initImpl();
    return impl->getPosition() - available();
}

size_t ReadBufferFromByteHDFS::getFileSize()
{
    if (file_size)
        return *file_size;
    if (!impl)
        initImpl();

    file_size = impl->getFileSize();
    return *file_size;
}

String ReadBufferFromByteHDFS::getFileName() const
{
    return hdfs_file_path;
}

void ReadBufferFromByteHDFS::setReadUntilPosition(size_t position)
{
    if (!impl)
        initImpl();
    impl->setReadUntilPosition(position);
}

void ReadBufferFromByteHDFS::setReadUntilEnd()
{
    if (!impl)
        initImpl();
    impl->setReadUntilEnd();
}

size_t ReadBufferFromByteHDFS::getFileOffsetOfBufferEnd() const
{
    if (!impl)
        return 0; // file_offset=0 at the construction of ReadBufferFromHDFSImpl
    return impl->file_offset;
}

size_t ReadBufferFromByteHDFS::readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & progress_callback)
{
    if (n == 0)
        return 0;

    auto pooled_impl = impl_pool.get([this] (){
        return new ReadBufferFromHDFSImpl(hdfs_file_path, hdfs_params, 0, settings);
    });

    pooled_impl->seek(range_begin);
    size_t bytes_read = pooled_impl->readImpl(to, n);

    if (bytes_read && progress_callback)
        progress_callback(bytes_read);
    return bytes_read;
}

}

#endif
