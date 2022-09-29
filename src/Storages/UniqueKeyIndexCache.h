#pragma once

#include <Storages/UniqueKeyIndex.h>
#include <Storages/UUIDAndPartName.h>
#include <Common/LRUCache.h>

namespace ProfileEvents
{
    extern const Event UniqueKeyIndexMetaCacheHit;
    extern const Event UniqueKeyIndexMetaCacheMiss;
}

namespace DB
{
struct UniqueKeyIndexWeightFunction
{
    size_t operator()(const UniqueKeyIndex & index) const
    {
        return index.residentMemoryUsage();
    }
};

class UniqueKeyIndexCache
    : public LRUCache<UUIDAndPartName, UniqueKeyIndex, UUIDAndPartNameHash, UniqueKeyIndexWeightFunction>
{
    using Base = LRUCache<UUIDAndPartName, UniqueKeyIndex, UUIDAndPartNameHash, UniqueKeyIndexWeightFunction>;
public:
    using Base::Base;

    template <typename LoadFunc>
    std::pair<MappedPtr, bool> getOrSet(const Key & key, LoadFunc && load_func)
    {
        auto result = Base::getOrSet(key, load_func);
        if (!result.second)
        {
            // ProfileEvents::increment(ProfileEvents::UniqueKeyIndexMetaCacheHit);
        }
        else
        {
            // ProfileEvents::increment(ProfileEvents::UniqueKeyIndexMetaCacheMiss);
        }

        return result;
    }
};

}
