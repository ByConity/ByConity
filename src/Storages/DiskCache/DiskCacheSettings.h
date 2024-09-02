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
#include <limits>
#include <Poco/Util/AbstractConfiguration.h>

#include <common/types.h>

namespace DB
{
struct DiskCacheSettings
{
    static constexpr auto root = "disk_cache_strategies";
    void loadFromConfig(const Poco::Util::AbstractConfiguration & conf, const std::string & disk_cache_name);

    String disk_policy {"default"};
    size_t lru_max_size {std::numeric_limits<size_t>::max()};
    size_t lru_max_nums {std::numeric_limits<size_t>::max()};
    // max percent of disk total capacity
    size_t lru_max_percent {80};
    // When queue size exceed random drop ratio, start drop disk cache task, range from 0 - 100
    size_t random_drop_threshold {50};
    // Cache mapping bucket size
    size_t mapping_bucket_size {5000};
    // LRU queue update interval in seconds
    size_t lru_update_interval {60};

    size_t cache_shard_num {12};
    // If true every disk will have it's own dispatcher, otherwise only one dispatcher
    // is used
    bool cache_dispatcher_per_disk {true};

    // Number of cache loader per disk
    size_t cache_loader_per_disk {2};
    int cache_load_dispatcher_drill_down_level {1};
    size_t cache_set_rate_limit {0};
    size_t cache_set_throughput_limit {0};

    size_t segment_size {8192};
    size_t hits_to_cache {2};
    // Size of disk cache statistics bucket size
    size_t stats_bucket_size {10000};

    // load previous folder cached data if it's not empty for compatible when data dir config is changed
    std::string previous_disk_cache_dir{};
    std::string latest_disk_cache_dir{"disk_cache_v1"};
    UInt64 meta_cache_size_ratio{0};
    UInt64 meta_cache_nums_ratio{50};

    // config for cache stealing
    UInt64 stealing_max_request_rate{0};
    UInt64 stealing_connection_timeout_ms{10000};
    UInt64 stealing_read_timeout_ms{20000};
    UInt64 stealing_max_retry_times{3};
    UInt64 stealing_retry_sleep_ms{100};
    UInt64 stealing_max_queue_count{10000};

    std::string toString() const;
};

}
