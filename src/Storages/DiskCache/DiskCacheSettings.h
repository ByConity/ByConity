#pragma once

#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{
struct DiskCacheSettings
{
    static constexpr auto prefix = "disk_cache";
    void loadFromConfig(const Poco::Util::AbstractConfiguration & conf, const std::string & disk_cache_name);

    size_t lru_max_size;
    // When queue size exceed random drop ratio, start drop disk cache task, range from 0 - 100
    size_t random_drop_threshold;
    // Cache mapping bucket size
    size_t mapping_bucket_size;
    // LRU queue update interval in seconds
    size_t lru_update_interval;
    std::string cache_base_path;
};

struct DiskCacheStrategySettings
{
    static constexpr auto prefix = "disk_cache_strategy";
    void loadFromConfig(const Poco::Util::AbstractConfiguration & conf, const std::string & disk_cache_strategy_name);

    size_t segment_size;
    size_t hits_to_cache;
    // Size of disk cache statistics bucket size
    size_t stats_bucket_size;
};

}
