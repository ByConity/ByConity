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

#pragma once

#include <cstddef>
#include <Core/Defines.h>
#include <Core/SettingsEnums.h>
#include <Common/Throttler.h>

namespace DB
{
class MMappedFileCache;

struct ReadSettings
{
    size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE;
    size_t estimated_size = 0;
    size_t aio_threshold = 0;
    size_t mmap_threshold = 0;
    MMappedFileCache* mmap_cache = nullptr;
    bool byte_hdfs_pread = true;
    size_t filesystem_cache_max_download_size = (128UL * 1024 * 1024 * 1024);
    bool skip_download_if_exceeds_query_cache = true;
    ThrottlerPtr throttler = nullptr;
    size_t remote_read_min_bytes_for_seek = DBMS_DEFAULT_BUFFER_SIZE;
    DiskCacheMode disk_cache_mode {DiskCacheMode::AUTO};

    bool s3_use_read_ahead {true};
};

}
