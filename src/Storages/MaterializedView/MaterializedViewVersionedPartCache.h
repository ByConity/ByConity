#pragma once

#include <mutex>
#include <unordered_map>
#include <Common/CacheBase.h>

namespace DB
{

class MaterializedViewVersionedPartCache
{
    const size_t default_max_size_in_bytes = 200000000;
public:
    using Key = std::string;
    struct Entry {
        String version;
        std::vector<std::shared_ptr<Protos::VersionedPartitions>> kv_cache;
        Entry(String version_, std::vector<std::shared_ptr<Protos::VersionedPartitions>> && kv_cache_)
            :version(version_), kv_cache(kv_cache_) {}
    };
    using EntryPtr = std::shared_ptr<Entry>;

    struct VersionedPartCacheEntryWeight {
        size_t operator()(const Entry & entry) {
            size_t total = entry.version.size();
            for (const auto & it : entry.kv_cache)
            {
                total += it->SpaceUsedLong();
            }
            return total;
        }
    };

    static MaterializedViewVersionedPartCache & getInstance()
    {
        static MaterializedViewVersionedPartCache cache_instance;
        return cache_instance;
    }

    size_t weight() const
    {
        return cache.weight();
    }

    void setMaxSize(size_t max_size_in_bytes)
    {
        cache.setMaxSize(max_size_in_bytes);
    }

    EntryPtr get(String mv_uuid)
    {
        return cache.get(mv_uuid);
    }

    void set(String mv_uuid, EntryPtr entry_ptr)
    {
        cache.set(mv_uuid, entry_ptr);
    }

    template <typename LoadFunc>
    std::pair<EntryPtr, bool> getOrSet(const Key & key, const String & version, LoadFunc && load_func)
    {
        return cache.getOrSet(key, [&version](EntryPtr entry) {
                return (entry->version == version);
            }, load_func);
    }

private:

    using Cache = CacheBase<Key, Entry, std::hash<Key>, VersionedPartCacheEntryWeight>;

    MaterializedViewVersionedPartCache(): cache("LRU", default_max_size_in_bytes)
    {

    }
    virtual ~MaterializedViewVersionedPartCache() = default;


private:
    Cache cache;
    mutable std::mutex mutex;
};

}
