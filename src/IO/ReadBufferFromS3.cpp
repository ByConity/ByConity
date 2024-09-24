/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include <Common/config.h>

#if USE_AWS_S3

#    include <Interpreters/RemoteReadLog.h>
#    include <IO/ReadBufferFromIStream.h>
#    include <IO/ReadBufferFromS3.h>
#    include <IO/S3Common.h>
#    include <Common/Stopwatch.h>

#    include <aws/s3/S3Client.h>
#    include <aws/s3/model/GetObjectRequest.h>
#    include <aws/s3/model/HeadObjectRequest.h>
#    include <common/logger_useful.h>
#    include <common/scope_guard_safe.h>
#    include <utility>


namespace ProfileEvents
{
    extern const Event S3HeadObject;
    extern const Event S3GetObject;

    extern const Event DiskS3GetObject;
    extern const Event DiskS3HeadObject;

    extern const Event ReadBufferFromS3ReadCount;
    extern const Event ReadBufferFromS3ReadBytes;
    extern const Event ReadBufferFromS3ReadMicroseconds;
    extern const Event ReadBufferFromS3StreamInitMicroseconds;
    extern const Event ReadBufferFromS3Seeks;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}


ReadBufferFromS3::ReadBufferFromS3(
    std::shared_ptr<Aws::S3::S3Client> client_ptr_,
    const String & bucket_,
    const String & key_,
    const ReadSettings & read_settings_,
    UInt64 max_single_read_retries_,
    bool restricted_seek_,
    bool use_external_buffer_,
    off_t read_until_position_,
    std::optional<size_t> file_size_)
    : ReadBufferFromFileBase(use_external_buffer_ ? 0 : read_settings_.remote_fs_buffer_size, nullptr, 0, file_size_)
    , client_ptr(std::move(client_ptr_))
    , bucket(bucket_)
    , key(key_)
    , max_single_read_retries(max_single_read_retries_)
    , read_settings(read_settings_)
    , restricted_seek(restricted_seek_)
    , use_external_buffer(use_external_buffer_)
    , read_until_position(read_until_position_)
{
}

ReadBufferFromS3::~ReadBufferFromS3()
{
    try
    {
        S3::resetSessionIfNeeded(readAllRangeSuccessfully(), read_result);
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

IAsynchronousReader::Result ReadBufferFromS3::readInto(char * data, size_t size, size_t read_offset, size_t ignore_bytes)
{
    /**
     * Set `data` to current working and internal buffers.
     * Internal buffer with size `size`. Working buffer with size 0.
     */
    set(data, size);

    bool result = false;

    offset = read_offset;
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

bool ReadBufferFromS3::nextImpl()
{
    if (read_until_position)
    {
        if (read_until_position == offset)
            return false;

        if (read_until_position < offset)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to read beyond right offset ({} > {})", offset, read_until_position - 1);
        }
    }

    bool next_result = false;

    if (impl)
    {
        if (use_external_buffer)
        {
            /**
            * use_external_buffer -- means we read into the buffer which
            * was passed to us from somewhere else. We do not check whether
            * previously returned buffer was read or not (no hasPendingData() check is needed),
            * because this branch means we are prefetching data,
            * each nextImpl() call we can fill a different buffer.
            */
            impl->set(internal_buffer.begin(), internal_buffer.size());
            assert(working_buffer.begin() != nullptr);
            assert(!internal_buffer.empty());
        }
        else
        {
            /**
            * impl was initialized before, pass position() to it to make
            * sure there is no pending data which was not read.
            */
            impl->position() = position();
            assert(!impl->hasPendingData());
        }
    }

    size_t attempt = 0;
    size_t sleep_ms = 100;
    Stopwatch timer;
    const auto request_time = std::chrono::system_clock::now();
    while (true)
    {
        Stopwatch watch;
        ProfileEvents::increment(ProfileEvents::ReadBufferFromS3ReadCount);
        SCOPE_EXIT({
            ProfileEvents::increment(ProfileEvents::ReadBufferFromS3ReadMicroseconds, watch.elapsedMicroseconds());
        });
        try
        {
            if (!impl)
            {
                impl = initialize();

                if (use_external_buffer)
                {
                    impl->set(internal_buffer.begin(), internal_buffer.size());
                    assert(working_buffer.begin() != nullptr);
                    assert(!internal_buffer.empty());
                }
                else
                {
                    /// use the buffer returned by `impl`
                    BufferBase::set(impl->buffer().begin(), impl->buffer().size(), impl->offset());
                }
            }

            /// Try to read a next portion of data.
            next_result = impl->next();
            break;
        }
        catch (Exception & e)
        {
            if (!S3::processReadException(e, log, bucket, key, getPosition(), ++attempt)
                || attempt >= max_single_read_retries)
            {
                throw;
            }

            sleepForMilliseconds(sleep_ms);
            sleep_ms *= 2;

            /// Try to reinitialize `impl`.
            resetWorkingBuffer();
            impl.reset();
        }
    }

    if (!next_result) {
        read_all_range_successfully = true;
        return false;
    }

    BufferBase::set(impl->buffer().begin(), impl->buffer().size(), impl->offset());
    ProfileEvents::increment(ProfileEvents::ReadBufferFromS3ReadBytes, working_buffer.size(), Metrics::MetricType::Counter);
    if (read_settings.remote_read_log)
    {
        read_settings.remote_read_log->insert(
            request_time, bucket + ":" + key, offset, working_buffer.size(), timer.elapsedMicroseconds(), read_settings.remote_read_context);
    }
    offset += working_buffer.size();
    return true;
}

off_t ReadBufferFromS3::seek(off_t offset_, int whence)
{
    if (whence == SEEK_CUR)
        offset_ = getPosition() + offset_;
    else if (whence != SEEK_SET)
        throw Exception("Seek expects SEEK_SET or SEEK_CUR as whence", ErrorCodes::BAD_ARGUMENTS);

    if (offset_ < 0)
        throw Exception("Seek position is out of bounds. Offset: " + std::to_string(offset_), ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);

    if (offset_ == getPosition())
        return offset_;

    if (impl && restricted_seek)
        throw Exception("Seek is allowed only before first read attempt from the buffer.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

    read_all_range_successfully = false;

    if (!restricted_seek)
    {
        if (!working_buffer.empty()
            && static_cast<size_t>(offset_) >= offset - working_buffer.size()
            && offset_ < offset)
        {
            pos = working_buffer.end() - (offset - offset_);
            assert(pos >= working_buffer.begin());
            assert(pos < working_buffer.end());

            return getPosition();
        }

        off_t position = getPosition();
        if (impl && offset_ > position && offset_ < stream_end_offset)
        {
            size_t diff = offset_ - position;
            if (diff < read_settings.remote_read_min_bytes_for_seek)
            {
                ignore(diff);
                return offset_;
            }
        }

        resetWorkingBuffer();
        if (impl)
        {
            impl.reset();
        }
        ProfileEvents::increment(ProfileEvents::ReadBufferFromS3Seeks);
    }
    offset = offset_;
    return offset;
}

off_t ReadBufferFromS3::getPosition()
{
    return offset - available();
}

void ReadBufferFromS3::setReadUntilPosition(size_t position)
{
    if (position != static_cast<size_t>(read_until_position))
    {
        read_all_range_successfully = false;

        if (impl)
        {
            offset = getPosition();
            resetWorkingBuffer();
            impl.reset();
        }
        read_until_position = position;
    }
}

void ReadBufferFromS3::setReadUntilEnd()
{
    if (read_until_position)
    {
        read_all_range_successfully = false;

        read_until_position = 0;
        if (impl)
        {
            offset = getPosition();
            resetWorkingBuffer();
            impl.reset();
        }
    }
}

std::unique_ptr<ReadBufferFromIStream> ReadBufferFromS3::initialize()
{
    S3::resetSessionIfNeeded(readAllRangeSuccessfully(), read_result);
    read_all_range_successfully = false;

    /**
     * If remote_filesystem_read_method = 'threadpool', then for MergeTree family tables
     * exact byte ranges to read are always passed here.
     */
    if (read_until_position && offset >= read_until_position)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to read beyond right offset ({} > {})", offset, read_until_position - 1);

    read_result = sendRequest(offset, read_until_position ? std::make_optional(read_until_position) : std::nullopt);

    size_t buffer_size = use_external_buffer ? 0 : read_settings.remote_fs_buffer_size;
    return std::make_unique<ReadBufferFromIStream>(read_result->GetBody(), buffer_size);
}

size_t ReadBufferFromS3::getFileSize()
{
    if (file_size)
        return *file_size;

    S3::S3Util s3_util(client_ptr, bucket, read_settings.for_disk_s3);
    file_size = s3_util.getObjectSize(key);
    return *file_size;
}

Aws::S3::Model::GetObjectResult ReadBufferFromS3::sendRequest(size_t range_begin, std::optional<size_t> range_end_not_incl)
{
    Aws::S3::Model::GetObjectRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);
    // if (!version_id.empty())
    //     req.SetVersionId(version_id);

    if (range_end_not_incl)
    {
        req.SetRange(fmt::format("bytes={}-{}", range_begin, *range_end_not_incl - 1));
        LOG_TRACE(
            log, "Read S3 object. Bucket: {}, Key: {}, Range: {}-{}",
            bucket, key, range_begin, *range_end_not_incl - 1);
    }
    else if (range_begin)
    {
        req.SetRange(fmt::format("bytes={}-", range_begin));
        LOG_TRACE(
            log, "Read S3 object. Bucket: {}, Key: {}, Offset: {}",
            bucket, key, range_begin);
    }

    Stopwatch watch;

    ProfileEvents::increment(ProfileEvents::S3GetObject);
    if (read_settings.for_disk_s3)
        ProfileEvents::increment(ProfileEvents::DiskS3GetObject);
    Aws::S3::Model::GetObjectOutcome outcome = client_ptr->GetObject(req);

    if (outcome.IsSuccess())
    {
        ProfileEvents::increment(ProfileEvents::ReadBufferFromS3StreamInitMicroseconds, watch.elapsedMicroseconds());
        if (range_end_not_incl)
            stream_end_offset = range_end_not_incl;
        return outcome.GetResultWithOwnership();
    }
    else
    {
        throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);
    }
}

size_t ReadBufferFromS3::readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & progress_callback)
{
    if (n == 0)
        return 0;

    size_t attempt = 0;
    size_t sleep_backoff_ms = 100;
    while (true)
    {
        Stopwatch watch;
        std::optional<Aws::S3::Model::GetObjectResult> result;
        bool all_data_read = false;
        SCOPE_EXIT_SAFE({
            ProfileEvents::increment(ProfileEvents::ReadBufferFromS3ReadMicroseconds, watch.elapsedMicroseconds());
            S3::resetSessionIfNeeded(all_data_read, result);
        });

        try
        {
            result = sendRequest(range_begin, range_begin + n);
            std::istream & istr = result->GetBody();

            size_t bytes = copyFromIStreamWithProgressCallback(istr, to, n, progress_callback);

            ProfileEvents::increment(ProfileEvents::ReadBufferFromS3ReadBytes, bytes);
            /// TODO: test for chunked encoding
            /// Read remaining bytes after the end of the payload for chunked encoding
            istr.ignore(INT64_MAX);
            all_data_read = true;
            return bytes;
        }
        catch (Exception & e)
        {
            if (!S3::processReadException(e, log, bucket, key, range_begin, ++attempt)
                || attempt >= max_single_read_retries)
                throw;

            sleepForMilliseconds(sleep_backoff_ms);
            sleep_backoff_ms *= 2;
        }
    }
}

bool ReadBufferFromS3::readAllRangeSuccessfully() const
{
    return stream_end_offset ? offset == stream_end_offset : read_all_range_successfully;
}
}

#endif
