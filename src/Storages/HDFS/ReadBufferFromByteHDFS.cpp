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

#include <Storages/HDFS/ReadBufferFromByteHDFS.h>

#if USE_HDFS

#include "Common/ProfileEvents.h"
#include "Common/Stopwatch.h"
#include "Storages/HDFS/HDFSCommon.h"
#include "Common/Exception.h"
#include "common/sleep.h"

#include <fcntl.h>
#include <hdfs/hdfs.h>

namespace ProfileEvents
{
    extern const int HdfsFileOpen;
    extern const int HdfsFileOpenMs;
    extern const int ReadBufferFromHdfsRead;
    extern const int HDFSReadElapsedMilliseconds;
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
    // LOG_TRACE(&Poco::Logger::get("ReadBufferFromByteHDFS"), "get event {} & {}", event.eventType, event.value);
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
    String hdfs_file_path;
    HDFSConnectionParams hdfs_params;

    HDFSBuilderPtr builder;
    HDFSFSPtr fs;
    hdfsFile fin;

    bool pread {false};
    size_t file_offset = 0;

    ReadBufferFromHDFSImpl(
        const String & hdfs_file_path_, bool pread_, const HDFSConnectionParams & hdfs_params_)
        : hdfs_params(hdfs_params_)
        , pread(pread_)
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
        ProfileEvents::increment(ProfileEvents::HdfsFileOpenMs, watch.elapsedMilliseconds());
    }

    ~ReadBufferFromHDFSImpl()
    {
        hdfsCloseFile(fs.get(), fin);
        fin = nullptr;
    }

    size_t readImpl(char * to, size_t bytes)
    {
        Stopwatch watch;
        size_t total_bytes_read = 0;
        do
        {
            int bytes_read = 0;

            if (pread)
                bytes_read = hdfsPRead(fs.get(), fin, to + total_bytes_read, bytes - total_bytes_read);
            else
                bytes_read = hdfsRead(fs.get(), fin, to + total_bytes_read, bytes - total_bytes_read);

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
        } while (total_bytes_read < bytes);

        file_offset += total_bytes_read;

        ProfileEvents::increment(ProfileEvents::ReadBufferFromHdfsRead, 1);
        ProfileEvents::increment(ProfileEvents::ReadBufferFromHdfsReadBytes, total_bytes_read);
        ProfileEvents::increment(ProfileEvents::HDFSReadElapsedMilliseconds, watch.elapsedMilliseconds());
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
};

ReadBufferFromByteHDFS::ReadBufferFromByteHDFS(
    const String & hdfs_file_path_,
    const HDFSConnectionParams & hdfs_params_,
    bool pread_,
    size_t buf_size_,
    char * existing_memory_,
    size_t alignment_,
    ThrottlerPtr total_network_throttler_)
    : ReadBufferFromFileBase(buf_size_, existing_memory_, alignment_)
    , impl(std::make_unique<ReadBufferFromHDFSImpl>(hdfs_file_path_, pread_, hdfs_params_))
    , total_network_throttler(total_network_throttler_)
{
}

ReadBufferFromByteHDFS::~ReadBufferFromByteHDFS() = default;

bool ReadBufferFromByteHDFS::nextImpl()
{
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

    /// new position still inside working buffer
    /// impl->getPosition() is the file position of the working buffer end
    /// Therefore working buffer corresponds to the file range
    /// [impl->getPosition() - working_buffer.size(), impl->getPosition()]
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
    return impl->getPosition() - available();
}

size_t ReadBufferFromByteHDFS::getFileSize()
{
    if (file_size)
        return *file_size;

    file_size = impl->getFileSize();
    return *file_size;
}

String ReadBufferFromByteHDFS::getFileName() const
{
    return impl->hdfs_file_path;
}


size_t ReadBufferFromByteHDFS::readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & progress_callback)
{
    if (n == 0)
        return 0;

    /// make a impl copy
    auto hdfs_impl = std::make_shared<ReadBufferFromHDFSImpl>(impl->hdfs_file_path, /*pread*/ true, impl->hdfs_params);
    hdfs_impl->seek(range_begin);
    size_t bytes_read = hdfs_impl->readImpl(to, n);
    if (bytes_read && progress_callback)
        progress_callback(bytes_read);
    return bytes_read;
}

}

#endif
