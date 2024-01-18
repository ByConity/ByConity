#pragma once

#include <Columns/IColumn.h>
#include <Common/LRUCache.h>
#include <Common/ProfileEvents.h>
#include <Storages/UUIDAndPartName.h>

namespace ProfileEvents
{
    extern const Event PrimaryIndexCacheHits;
    extern const Event PrimaryIndexCacheMisses;
}

namespace DB
{

using PrimaryIndex = Columns;

struct PrimaryIndexWeightFunction
{
    size_t operator()(const PrimaryIndex & index) const
    {
        size_t sum_bytes = 0;
        for (const auto & column : index)
            sum_bytes += column->allocatedBytes();
        return sum_bytes;
    }
};

class PrimaryIndexCache : public LRUCache<UUIDAndPartName, PrimaryIndex, UUIDAndPartNameHash, PrimaryIndexWeightFunction>
{
    using Base = LRUCache<UUIDAndPartName, PrimaryIndex, UUIDAndPartNameHash, PrimaryIndexWeightFunction>;
public:
    using Base::Base;

    template <typename LoadFunc>
    std::pair<MappedPtr, bool> getOrSet(const Key & key, LoadFunc && load)
    {
        auto result = Base::getOrSet(key, load);
        if (result.second)
            ProfileEvents::increment(ProfileEvents::PrimaryIndexCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::PrimaryIndexCacheHits);
        return result;
    }
};

using PrimaryIndexCachePtr = std::shared_ptr<PrimaryIndexCache>;

}
