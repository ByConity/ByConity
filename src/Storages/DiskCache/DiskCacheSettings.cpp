#include "DiskCacheSettings.h"

#include <fmt/format.h>

namespace DB
{
void DiskCacheSettings::loadFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & disk_cache_name)
{
    std::string config_prefix = fmt::format("{}.{}", prefix, disk_cache_name);
    lru_max_size = config.getUInt64(config_prefix + ".lru_max_size", static_cast<uint64_t>(2) * 1024 * 1024 * 1024 * 1024);
    random_drop_threshold = config.getUInt64(config_prefix + ".random_drop_threshold", 50);
    mapping_bucket_size = config.getUInt64(config_prefix + ".mapping_bucket_size", 5000);
    lru_update_interval = config.getUInt64("disk_cache_strategies.simple.lru_update_interval", 24 * 60 * 60);
}

void DiskCacheStrategySettings::loadFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & disk_cache_strategy_name)
{
    std::string config_prefix = fmt::format("{}.{}", prefix, disk_cache_strategy_name);
    segment_size = config.getUInt64(config_prefix + ".segment_size", 8192);
    hits_to_cache = config.getUInt64(config_prefix + ".hits_to_cache", 2);
    stats_bucket_size = config.getUInt64(config_prefix + ".stats_bucket_size", 10000);
}

}
