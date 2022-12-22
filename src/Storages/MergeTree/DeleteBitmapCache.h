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

    /// insert ("version", "bitmap") into the cache using "key".
    void insert(const String & key, UInt64 version, ImmutableDeleteBitmapPtr bitmap);

    /// if found key, return true and set "out_version" and "out_bitmap".
    /// otherwise, return false.
    bool lookup(const String & key, UInt64 & out_version, ImmutableDeleteBitmapPtr & out_bitmap);

    /// if found key, erase it from cache
    void erase(const String & key);

    static String buildKey(UUID storage_uuid, const String & partition_id, Int64 min_block, Int64 max_block);

private:
    std::shared_ptr<IndexFile::Cache> cache;
};

using DeleteBitmapCachePtr = std::shared_ptr<DeleteBitmapCache>;

}
