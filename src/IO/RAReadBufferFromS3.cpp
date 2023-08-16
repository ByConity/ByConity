/*
 * Copyright 2023 Bytedance Ltd. and/or its affiliates.
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

#include <cassert>
#include <chrono>
#include <memory>
#include <thread>
#include <aws/s3/model/GetObjectRequest.h>
#include <Core/Types.h>
#include <IO/S3Common.h>
#include <Common/ProfileEvents.h>
#include <common/logger_useful.h>
#include <common/scope_guard.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO//RAReadBufferFromS3.h>

namespace ProfileEvents
{
    extern const Event ReadBufferFromS3Read;
    extern const Event ReadBufferFromS3ReadFailed;
    extern const Event ReadBufferFromS3ReadBytes;
    extern const Event ReadBufferFromS3ReadMicro;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int BAD_ARGUMENTS;
}

RAReadBufferFromS3::RAReadBufferFromS3(const std::shared_ptr<Aws::S3::S3Client>& client,
    const String& bucket, const String& key, size_t read_retry,
    size_t buffer_size, char* existing_memory, size_t alignment,
    const ThrottlerPtr& throttler, size_t max_buffer_expand_times,
    size_t read_expand_pct, size_t seq_read_thres):
        ReadBufferFromFileBase(buffer_size, existing_memory, alignment),
        throttler_(throttler), read_retry_(read_retry),
        reader_(client, bucket, key, buffer_size, max_buffer_expand_times,
            read_expand_pct, seq_read_thres, &Poco::Logger::get("RAReadBufferFromS3")) {}

bool RAReadBufferFromS3::nextImpl()
{
    auto sleep_time_with_backoff_milliseconds = std::chrono::milliseconds(100);
    for (size_t attempt = 0; true; ++attempt)
    {
        try
        {
            uint64_t readed = 0;
            {
                Stopwatch watch;
                ProfileEvents::increment(ProfileEvents::ReadBufferFromS3Read);
                SCOPE_EXIT({
                    auto time = watch.elapsedMicroseconds();
                    ProfileEvents::increment(ProfileEvents::ReadBufferFromS3ReadMicro, time);
                });

                readed = reader_.read(buffer().begin(), internalBuffer().size());

                ProfileEvents::increment(ProfileEvents::ReadBufferFromS3ReadBytes, readed);
            }

            buffer().resize(readed);

            if (throttler_)
            {
                throttler_->add(readed);
            }

            return readed > 0;
        }
        catch (...)
        {
            ProfileEvents::increment(ProfileEvents::ReadBufferFromS3ReadFailed, 1);

            if (attempt >= read_retry_)
                throw;

            std::this_thread::sleep_for(sleep_time_with_backoff_milliseconds);
            sleep_time_with_backoff_milliseconds *= 2;
        }
    }
}

off_t RAReadBufferFromS3::seek(off_t off, int whence)
{
    if (whence != SEEK_SET)
    {
        throw Exception(fmt::format("ReadBufferFromS3::seek expects SEEK_SET as whence while reading {}",
            reader_.key()), ErrorCodes::BAD_ARGUMENTS);
    }

    off_t buffer_start_offset = reader_.offset() - static_cast<off_t>(working_buffer.size());
    if (hasPendingData() && off <= static_cast<off_t>(reader_.offset()) && off >= buffer_start_offset)
    {
        pos = working_buffer.begin() + off - buffer_start_offset;
        return off;
    }
    else
    {
        pos = working_buffer.end();
        return reader_.seek(off);
    }
}

}
