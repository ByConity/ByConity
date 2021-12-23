#pragma once

#include <Core/Types.h>
#include <Storages/IndexFile/Cache.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{
class DeleteBitmapCache
{
public:
    explicit DeleteBitmapCache(size_t max_size_in_bytes);
    ~DeleteBitmapCache();

    /// insert bitmap into the cache using "key".
    void insert(const String & key, DeleteBitmapPtr bitmap);

    /// if found key, return true and set "out_bitmap".
    /// otherwise, return false.
    bool lookup(const String & key, DeleteBitmapPtr & out_bitmap);

    /// if found key, erase it from cache
    void erase(const String & key);

private:
    std::shared_ptr<IndexFile::Cache> cache;
};

using DeleteBitmapCachePtr = std::shared_ptr<DeleteBitmapCache>;

}
