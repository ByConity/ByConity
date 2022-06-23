#pragma once

#include <Storages/DiskCache/DiskCache_fwd.h>
#include <common/singleton.h>

namespace DB
{
class Context;

class DiskCacheFactory : public ext::singleton<DiskCacheFactory>
{
public:
    using CacheEntry = std::pair<IDiskCachePtr, IDiskCacheStrategyPtr>;

    void init(Context & global_context);
    CacheEntry getDefault() const { return default_cache; }

private:
    // std::unordered_map<String, CacheEntry> caches;
    CacheEntry default_cache;
};
}
