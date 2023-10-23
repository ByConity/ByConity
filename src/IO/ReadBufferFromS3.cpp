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
    extern const Event S3ReadMicroseconds;
    extern const Event S3ReadBytes;

    extern const Event ReadBufferFromS3ReadMicroseconds;
    extern const Event ReadBufferFromS3InitMicroseconds;
    extern const Event ReadBufferFromS3ReadBytes;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}


ReadBufferFromS3::ReadBufferFromS3(
    std::shared_ptr<Aws::S3::S3Client> client_ptr_,
    const String & bucket_,
    const String & key_,
    const ReadSettings & read_settings_,
    UInt64 max_single_read_retries_,
    bool restricted_seek_)
    : ReadBufferFromFileBase()
    , client_ptr(std::move(client_ptr_))
    , bucket(bucket_)
    , key(key_)
    , max_single_read_retries(max_single_read_retries_)
    , read_settings(read_settings_)
    , restricted_seek(restricted_seek_)
{
}

ReadBufferFromS3::~ReadBufferFromS3()
{
    try
    {
        S3::resetSessionIfNeeded(read_all_range_successfully, read_result);
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

bool ReadBufferFromS3::nextImpl()
{
    bool next_result = false;

    if (impl)
    {
        // impl was initialized before, pass position() to it to make
        // sure there is no pending data which was not read.
        impl->position() = pos;
        assert(!impl->hasPendingData());
    }

    size_t attempt = 0;
    size_t sleep_ms = 100;
    while (true)
    {
        Stopwatch watch;
        SCOPE_EXIT({
            ProfileEvents::increment(ProfileEvents::ReadBufferFromS3ReadMicroseconds, watch.elapsedMicroseconds());
        });
        try
        {
            if (!impl)
            {
                impl = initialize();
                /// use the buffer returned by `impl`
                BufferBase::set(impl->buffer().begin(), impl->buffer().size(), impl->offset());
            }
            /// Try to read a next portion of data.
            next_result = impl->next();
            break;
        }
        catch (Exception & e)
        {
            if (!S3::processReadException(e, log, bucket, key, getPosition(), ++attempt)
                || attempt >= max_single_read_retries)
                throw;

            sleepForMilliseconds(sleep_ms);
            sleep_ms *= 2;

            /// Try to reinitialize `impl`.
            resetWorkingBuffer();
            impl.reset();
        }
    }

    if (!next_result)
    {
        read_all_range_successfully = true;
        return false;
    }

    BufferBase::set(impl->buffer().begin(), impl->buffer().size(), impl->offset());
    ProfileEvents::increment(ProfileEvents::S3ReadBytes, working_buffer.size());
    ProfileEvents::increment(ProfileEvents::ReadBufferFromS3ReadBytes, working_buffer.size());
    offset += working_buffer.size();
    return true;
}

off_t ReadBufferFromS3::seek(off_t offset_, int whence)
{
    read_all_range_successfully = false;

    if (impl && restricted_seek)
        throw Exception("Seek is allowed only before first read attempt from the buffer.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

    if (whence != SEEK_SET)
        throw Exception("Only SEEK_SET mode is allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

    if (offset_ < 0)
        throw Exception("Seek position is out of bounds. Offset: " + std::to_string(offset_), ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);

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
        if (impl && offset_ > position)
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
    }
    offset = offset_;
    return offset;
}

off_t ReadBufferFromS3::getPosition()
{
    return offset - available();
}

std::unique_ptr<ReadBuffer> ReadBufferFromS3::initialize()
{
    S3::resetSessionIfNeeded(read_all_range_successfully, read_result);
    read_all_range_successfully = false;

    read_result = sendRequest(offset, std::nullopt);
    return std::make_unique<ReadBufferFromIStream>(read_result->GetBody(), read_settings.buffer_size);
}

size_t ReadBufferFromS3::getFileSize()
{
    if (file_size)
        return *file_size;
    Aws::S3::Model::HeadObjectRequest request;
    request.SetBucket(bucket);
    request.SetKey(key);

    Aws::S3::Model::HeadObjectOutcome outcome = client_ptr->HeadObject(request);
    if (outcome.IsSuccess())
    {
        file_size = outcome.GetResultWithOwnership().GetContentLength();
        return *file_size;
    }
    else
    {
        throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);
    }
}

Aws::S3::Model::GetObjectResult ReadBufferFromS3::sendRequest(size_t range_begin, std::optional<size_t> range_end_incl) const
{
    Aws::S3::Model::GetObjectRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);
    // if (!version_id.empty())
    //     req.SetVersionId(version_id);

    if (range_end_incl)
    {
        req.SetRange(fmt::format("bytes={}-{}", range_begin, *range_end_incl));
        LOG_TRACE(
            log, "Read S3 object. Bucket: {}, Key: {}, Range: {}-{}",
            bucket, key, range_begin, *range_end_incl);
    }
    else if (range_begin)
    {
        req.SetRange(fmt::format("bytes={}-", range_begin));
        LOG_TRACE(
            log, "Read S3 object. Bucket: {}, Key: {}, Offset: {}",
            bucket, key, range_begin);
    }

    // ProfileEvents::increment(ProfileEvents::S3GetObject);
    Stopwatch watch;

    Aws::S3::Model::GetObjectOutcome outcome = client_ptr->GetObject(req);

    if (outcome.IsSuccess())
    {
        ProfileEvents::increment(ProfileEvents::ReadBufferFromS3InitMicroseconds, watch.elapsedMicroseconds());
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
            result = sendRequest(range_begin, range_begin + n - 1);
            std::istream & istr = result->GetBody();

            size_t bytes = copyFromIStreamWithProgressCallback(istr, to, n, progress_callback);

            ProfileEvents::increment(ProfileEvents::S3ReadBytes, bytes);
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
                throw e;

            sleepForMilliseconds(sleep_backoff_ms);
            sleep_backoff_ms *= 2;
        }
    }
}
}

#endif
