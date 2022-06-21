#include "DiskCacheFactory.h"

#include <Disks/IStoragePolicy.h>
#include <Interpreters/Context.h>
#include <Storages/DiskCache/DiskCacheLRU.h>
#include <Storages/DiskCache/DiskCacheSettings.h>
#include <Storages/DiskCache/DiskCacheSimpleStrategy.h>

namespace DB
{
void DiskCacheFactory::init(Context & context)
{
    const auto & config = context.getConfigRef();
    DiskCacheSettings cache_settings;
    DiskCacheStrategySettings strategy_settings;
    cache_settings.loadFromConfig(config, "lru");
    strategy_settings.loadFromConfig(config, "simple");

    // TODO: volume
    VolumePtr disk_cache_volume = context.getStoragePolicy("default")->getVolume(0);
    auto disk_cache = std::make_shared<DiskCacheLRU>(context, disk_cache_volume, cache_settings);
    auto cache_strategy = std::make_shared<DiskCacheSimpleStrategy>(strategy_settings);

    default_cache = std::make_pair(std::move(disk_cache), std::move(cache_strategy));
}

}
