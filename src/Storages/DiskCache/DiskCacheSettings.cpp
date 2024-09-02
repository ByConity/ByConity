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

#include "DiskCacheSettings.h"

#include <fmt/format.h>
#include "Storages/DiskCache/DiskCacheLRU.h"

namespace DB
{
void DiskCacheSettings::loadFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & disk_cache_name)
{
    std::string config_prefix = fmt::format("{}.{}", root, disk_cache_name); // {root}.MergeTree
    disk_policy = config.getString(config_prefix + ".disk_policy", "default");
    lru_max_nums = config.getUInt64(config_prefix + ".lru_max_object_num", std::numeric_limits<size_t>::max());
    // Todo: process the case which disk not have 2 TB free space
    lru_max_size = config.getUInt64(config_prefix + ".lru_max_size", static_cast<uint64_t>(2) * 1024 * 1024 * 1024 * 1024);
    lru_max_percent = config.getUInt64(config_prefix + ".lru_max_percent", 80);
    random_drop_threshold = config.getUInt64(config_prefix + ".random_drop_threshold", 50);
    mapping_bucket_size = config.getUInt64(config_prefix + ".mapping_bucket_size", 5000);
    lru_update_interval = config.getUInt64(config_prefix + ".lru_update_interval", 60);
    cache_shard_num = config.getUInt(config_prefix + ".cache_shard_num", 12);
    cache_dispatcher_per_disk = config.getBool(config_prefix + ".cache_dispatcher_per_disk", true);
    cache_loader_per_disk = config.getUInt(config_prefix + ".cache_loader_per_disk", 2);
    cache_load_dispatcher_drill_down_level = config.getInt(config_prefix + ".cache_load_dispatcher_drill_down_level", 1);
    cache_set_rate_limit = config.getUInt64(config_prefix + ".cache_set_rate_limit", 0);
    cache_set_throughput_limit = config.getUInt64(config_prefix + ".cache_set_throughput_limit", 0);
    segment_size = config.getUInt64(config_prefix + ".segment_size", 8192);
    hits_to_cache = config.getUInt64(config_prefix + ".hits_to_cache", 2);
    stats_bucket_size = config.getUInt64(config_prefix + ".stats_bucket_size", 10000);
    latest_disk_cache_dir = config.getString(config_prefix + ".disk_cache_dir", "part_disk_cache");
    previous_disk_cache_dir = config.getString(config_prefix + ".previous_disk_cache_dir", (disk_cache_name == "simple" || disk_cache_name == "MergeTree" ? "disk_cache,disk_cache_v1,mergetree_disk_cache" : ""));
    meta_cache_size_ratio = config.getUInt(config_prefix + ".meta_cache_size_ratio", 0);
    meta_cache_nums_ratio = config.getUInt(config_prefix + ".meta_cache_nums_ratio", 50);
    stealing_max_request_rate = config.getUInt(config_prefix + ".stealing_max_request_rate", 0);
    stealing_connection_timeout_ms = config.getUInt(config_prefix + ".stealing_connection_timeout_ms", 5000);
    stealing_read_timeout_ms = config.getUInt(config_prefix + ".stealing_read_timeout_ms", 10000);
    stealing_max_retry_times = config.getUInt(config_prefix + ".stealing_max_retry_times", 3);
    stealing_retry_sleep_ms = config.getUInt(config_prefix + ".stealing_retry_sleep_ms", 100);
    stealing_max_queue_count = config.getUInt(config_prefix + ".stealing_max_queue_count", 10000);
}

std::string DiskCacheSettings::toString() const
    {
    return fmt::format(
        R"({{
            "disk_policy": {},
            "lru_max_percent": {},
            "lru_max_size": {},
            "lru_max_nums": {},
            "random_drop_threshold": {},
            "mapping_bucket_size": {},
            "lru_update_interval": {},
            "cache_shard_num": {},
            "cache_dispatcher_per_disk": {},
            "cache_loader_per_disk": {},
            "cache_load_dispatcher_drill_down_level": {},
            "cache_set_rate_limit": {},
            "cache_set_throughput_limit": {},
            "segment_size": {},
            "hits_to_cache": {},
            "stats_bucket_size": {},
            "previous_disk_cache_dir": "{}",
            "latest_disk_cache_dir": "{}",
            "meta_cache_size_ratio": "{}",
            "meta_cache_nums_ratio": "{}"
        }})",
        disk_policy,
        lru_max_percent,
        lru_max_size,
        lru_max_nums,
        random_drop_threshold,
        mapping_bucket_size,
        lru_update_interval,
        cache_shard_num,
        cache_dispatcher_per_disk,
        cache_loader_per_disk,
        cache_load_dispatcher_drill_down_level,
        cache_set_rate_limit,
        cache_set_throughput_limit,
        segment_size,
        hits_to_cache,
        stats_bucket_size,
        previous_disk_cache_dir,
        latest_disk_cache_dir,
        meta_cache_size_ratio,
        meta_cache_nums_ratio);
    }
}
