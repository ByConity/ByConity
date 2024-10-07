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
#include <common/types.h>
#include <Common/Throttler.h>

namespace DB
{
enum class LocalFSReadMethod
{
    /**
     * Simple synchronous reads with 'read'.
     * Can use direct IO after specified size.
     * Can use prefetch by asking OS to perform readahead.
     */
    read,

    /**
     * Simple synchronous reads with 'pread'.
     * In contrast to 'read', shares single file descriptor from multiple threads.
     * Can use direct IO after specified size.
     * Can use prefetch by asking OS to perform readahead.
     */
    pread,

    /**
     * Use mmap after specified size or simple synchronous reads with 'pread'.
     * Can use prefetch by asking OS to perform readahead.
     */
    mmap,

    /**
     * Use the io_uring Linux subsystem for asynchronous reads.
     * Can use direct IO after specified size.
     * Can do prefetch with double buffering.
     */
    io_uring,

    /**
     * Checks if data is in page cache with 'preadv2' on modern Linux kernels.
     * If data is in page cache, read from the same thread.
     * If not, offload IO to separate threadpool.
     * Can do prefetch with double buffering.
     * Can use specified priorities and limit the number of concurrent reads.
     */
    pread_threadpool
};

enum class RemoteFSReadMethod
{
    read,
    threadpool,
};

class MMappedFileCache;
class RemoteReadLog;

struct ReadSettings
{
    /// Method to use reading from local filesystem.
    LocalFSReadMethod local_fs_method = LocalFSReadMethod::pread;
    /// Method to use reading from remote filesystem.
    RemoteFSReadMethod remote_fs_method = RemoteFSReadMethod::threadpool;

    /// https://eklitzke.org/efficient-file-copying-on-linux
    size_t local_fs_buffer_size = 128 * 1024;
    size_t remote_fs_buffer_size = DBMS_DEFAULT_BUFFER_SIZE;

    bool remote_fs_prefetch = false;
    bool local_fs_prefetch = false;

    /// Bandwidth throttler to use during reading
    ThrottlerPtr remote_throttler;
    ThrottlerPtr local_throttler;

    /// For trace all read requests in system table
    RemoteReadLog * remote_read_log = nullptr;
    /// Allow reader to provide additional context (e.g., stream name) for the request,
    /// which will get logged into remote_read_log when enabled
    String remote_read_context;

    /// For 'read', 'pread' and 'pread_threadpool' methods.
    size_t aio_threshold = 0;

    /// For 'mmap' method.
    size_t mmap_threshold = 0;
    MMappedFileCache * mmap_cache = nullptr;

    bool enable_io_scheduler = false;
    bool enable_io_pfra = false;
    bool enable_cloudfs = true;
    bool enable_nexus_fs = true;

    size_t estimated_size = 0;

    bool zero_copy_read_from_cache = false;

    bool byte_hdfs_pread = true;
    size_t filesystem_cache_max_download_size = (128UL * 1024 * 1024 * 1024);
    bool skip_download_if_exceeds_query_cache = true;
    size_t remote_read_min_bytes_for_seek = 3 * DBMS_DEFAULT_BUFFER_SIZE;
    DiskCacheMode disk_cache_mode {DiskCacheMode::AUTO};

    size_t parquet_decode_threads = 48;

    size_t filtered_ratio_to_use_skip_read = 0;
    /// Monitoring
    bool for_disk_s3 = false; // to choose which profile events should be incremented

    Int64 remote_fs_read_failed_injection = 0;

    void adjustBufferSize(size_t size)
    {
        local_fs_buffer_size = std::min(size, local_fs_buffer_size);
        remote_fs_buffer_size = std::min(size, remote_fs_buffer_size);
    }

    ReadSettings initializeReadSettings(size_t size)
    {
        local_fs_buffer_size = std::min(size, local_fs_buffer_size);
        remote_fs_buffer_size = std::min(size, remote_fs_buffer_size);
        return *this;
    }
};

}
