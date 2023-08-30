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
#    include <Common/Stopwatch.h>

#    include <aws/s3/S3Client.h>
#    include <aws/s3/model/GetObjectRequest.h>
#    include <aws/s3/model/HeadObjectRequest.h>
#    include <common/logger_useful.h>

#    include <utility>


namespace ProfileEvents
{
    extern const Event S3ReadMicroseconds;
    extern const Event S3ReadBytes;
    extern const Event S3ReadRequestsErrors;

    extern const Event ReadBufferFromS3ReadMicro;
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

bool ReadBufferFromS3::nextImpl()
{
    Stopwatch watch;
    bool next_result = false;
    auto sleep_time_with_backoff_milliseconds = std::chrono::milliseconds(100);

    if (!impl)
        impl = initialize();
    else
        // Sync position in ReadBufferFromS3 to impl, otherwise assertion in impl->next
        // may fail
        impl->position() = pos;

    for (size_t attempt = 0; attempt < max_single_read_retries; ++attempt)
    {
        try
        {
            next_result = impl->next();
            /// FIXME. 1. Poco `istream` cannot read less than buffer_size or this state is being discarded during
            ///           istream <-> iostream conversion. `gcount` always contains 0,
            ///           that's why we always have error "Cannot read from istream at offset 0".

            break;
        }
        catch (const Exception & e)
        {
            ProfileEvents::increment(ProfileEvents::S3ReadRequestsErrors, 1);

            LOG_INFO(log, "Caught exception while reading S3 object. Bucket: {}, Key: {}, Offset: {}, Attempt: {}, Message: {}",
                    bucket, key, getPosition(), attempt, e.message());

            impl.reset();
            impl = initialize();
        }

        std::this_thread::sleep_for(sleep_time_with_backoff_milliseconds);
        sleep_time_with_backoff_milliseconds *= 2;
    }

    watch.stop();
    ProfileEvents::increment(ProfileEvents::S3ReadMicroseconds, watch.elapsedMicroseconds());
    if (!next_result)
        return false;

    working_buffer = internal_buffer = impl->buffer();
    pos = working_buffer.begin();

    ProfileEvents::increment(ProfileEvents::S3ReadBytes, internal_buffer.size());

    offset += working_buffer.size();

    return true;
}

off_t ReadBufferFromS3::seek(off_t offset_, int whence)
{
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
    LOG_TRACE(log, "Read S3 object. Bucket: {}, Key: {}, Offset: {}", bucket, key, offset);

    Aws::S3::Model::GetObjectRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);
    req.SetRange(fmt::format("bytes={}-", offset));

    Aws::S3::Model::GetObjectOutcome outcome = client_ptr->GetObject(req);

    if (outcome.IsSuccess())
    {
        read_result = outcome.GetResultWithOwnership();
        return std::make_unique<ReadBufferFromIStream>(read_result.GetBody(), read_settings.buffer_size);
    }
    else
        throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);
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
        watch.stop();
        ProfileEvents::increment(ProfileEvents::ReadBufferFromS3ReadMicro, watch.elapsedMicroseconds());

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

    size_t sleep_time_with_backoff_milliseconds = 100;
    for (size_t attempt = 0;; ++attempt)
    {
        bool last_attempt = attempt + 1 >= max_single_read_retries;

        Stopwatch watch;

        try
        {
            auto result = sendRequest(range_begin, range_begin + n - 1);
            std::istream & istr = result.GetBody();

            size_t bytes = copyFromIStreamWithProgressCallback(istr, to, n, progress_callback);

            ProfileEvents::increment(ProfileEvents::ReadBufferFromS3ReadBytes, bytes);

            return bytes;
        }
        catch (Poco::Exception & e)
        {
            if (!last_attempt)
                throw e;

            sleepForMilliseconds(sleep_time_with_backoff_milliseconds);
            sleep_time_with_backoff_milliseconds *= 2;
        }

        watch.stop();
        ProfileEvents::increment(ProfileEvents::ReadBufferFromS3ReadMicro, watch.elapsedMicroseconds());
    }
}
}

#endif
