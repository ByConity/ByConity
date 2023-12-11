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

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <IO/HTTPCommon.h>
#include <IO/ReadBuffer.h>
#include <IO/S3Common.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/S3RemoteFSReader.h>
#include <sys/types.h>
#include <Common/Throttler.h>

namespace Aws::S3
{
class S3Client;
}

namespace DB
{

class RAReadBufferFromS3: public ReadBufferFromFileBase
{
public:
    RAReadBufferFromS3(const std::shared_ptr<Aws::S3::S3Client>& client,
        const String& bucket, const String& key, size_t read_retry = 3,
        size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE, char* existing_memory = nullptr,
        size_t alignment = 0, const ThrottlerPtr& throttler = nullptr,
        size_t max_buffer_expand_times = 8, size_t read_expand_pct = 150,
        size_t seq_read_thres = 70);

    virtual bool nextImpl() override;

    virtual off_t getPosition() override
    {
        return reader_.offset() - (working_buffer.end() - pos);
    }

    virtual String getFileName() const override
    {
        return reader_.bucket() + "/" + reader_.key();
    }

    virtual off_t seek(off_t off, int whence) override;

    virtual bool supportsReadAt() override { return true; }

    virtual size_t readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & progress_callback) override;

private:
    ThrottlerPtr throttler_;

    size_t read_retry_;

    S3ReadAheadReader reader_;
};

}
