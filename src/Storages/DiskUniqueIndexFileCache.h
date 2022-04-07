#pragma once

#include <Storages/UniqueKeyIndex.h>
#include <Storages/UniqueRowStore.h>
#include <Common/LRUCache.h>

namespace DB
{
struct DiskUniqueKeyIndexWeightFunction
{
    size_t operator()(const DiskUniqueKeyIndex & index) const { return index.residentMemoryUsage(); }
};

struct DiskUniqueRowStoreWeightFunction
{
    size_t operator()(const UniqueRowStore & index) const { return index.residentMemoryUsage(); }
};

struct DataPartMemoryAddressHash
{
    size_t operator()(const String & key) const { return CityHash_v1_0_2::CityHash64(key.data(), key.length()); }
};

class DiskUniqueKeyIndexCache : public LRUCache<String, DiskUniqueKeyIndex, DataPartMemoryAddressHash, DiskUniqueKeyIndexWeightFunction>
{
    using Base = LRUCache<String, DiskUniqueKeyIndex, DataPartMemoryAddressHash, DiskUniqueKeyIndexWeightFunction>;

public:
    DiskUniqueKeyIndexCache(size_t max_value_size_, size_t max_key_size_) : Base(max_value_size_), max_key_size(max_key_size_) {}

    bool shouldRemoveEldestEntry() const override
    {
        return Base::shouldRemoveEldestEntry() || (max_key_size > 0 && cells.size() > max_key_size);
    }

private:
    size_t max_key_size;
};

class DiskUniqueRowStoreCache : public LRUCache<String, UniqueRowStore, DataPartMemoryAddressHash, DiskUniqueRowStoreWeightFunction>
{
    using Base = LRUCache<String, UniqueRowStore, DataPartMemoryAddressHash, DiskUniqueRowStoreWeightFunction>;

public:
    DiskUniqueRowStoreCache(size_t max_value_size_, size_t max_key_size_) : Base(max_value_size_), max_key_size(max_key_size_) {}

    bool shouldRemoveEldestEntry() const override
    {
        return Base::shouldRemoveEldestEntry() || (max_key_size > 0 && cells.size() > max_key_size);
    }

private:
    size_t max_key_size;
};

using DiskUniqueKeyIndexCachePtr = std::shared_ptr<DiskUniqueKeyIndexCache>;
using DiskUniqueRowStoreCachePtr = std::shared_ptr<DiskUniqueRowStoreCache>;

}
