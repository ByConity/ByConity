#pragma once

#include <Common/HashTable/Hash.h>
#include <Common/LRUCache.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>

namespace ProfileEvents
{
extern const Event CloudTableDefinitionCacheHits;
extern const Event CloudTableDefinitionCacheMisses;
}

namespace DB
{
class StorageCloudMergeTree;

/// cache value can be nullptr, client should decide how to handle it
class CloudTableDefinitionCache : public LRUCache<UInt128, StorageCloudMergeTree, UInt128TrivialHash>
{
    using Base = LRUCache<UInt128, StorageCloudMergeTree, UInt128TrivialHash>;

public:
    using Base::Base;

    explicit CloudTableDefinitionCache(size_t max_size_in_bytes) : Base(max_size_in_bytes) { }

    static UInt128 hash(const String & create_query) { return sipHash128(create_query.data(), create_query.length()); }

    template <typename LoadFunc>
    std::pair<MappedPtr, bool> getOrSet(const Key & key, LoadFunc && load)
    {
        auto result = Base::getOrSet(key, load);
        if (result.second)
            ProfileEvents::increment(ProfileEvents::CloudTableDefinitionCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::CloudTableDefinitionCacheHits);
        return result;
    }
};

using CloudTableDefinitionCachePtr = std::shared_ptr<CloudTableDefinitionCache>;
}
