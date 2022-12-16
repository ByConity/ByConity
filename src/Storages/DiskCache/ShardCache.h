#pragma once

#include <vector>
#include <Storages/DiskCache/BucketLRUCache.h>

namespace DB
{

template <typename Key, typename ShardHash, typename CacheType>
class ShardCache
{
public:
    using ContainerType = CacheType;
    using CacheOptions = typename CacheType::Options;

    ShardCache(UInt32 shard_num_, const CacheOptions& opts):
        shard_num(shard_num_)
    {
        for (UInt32 i = 0; i < shard_num_; ++i)
        {
            containers.push_back(std::make_unique<CacheType>(opts));
        }
    }

    inline CacheType& shard(const Key& key)
    {
        size_t shard_id = hasher(key);
        shard_id ^= shard_id + 0x9e3779b9 + (shard_id << 6) + (shard_id >> 2);
        return *(containers[shard_id % shard_num]);
    }

    size_t count() const
    {
        size_t total_count = 0;
        for (const std::unique_ptr<CacheType>& container : containers)
        {
            total_count += container->count();
        }
        return total_count;
    }

    size_t weight() const
    {
        size_t total_weight = 0;
        for (const std::unique_ptr<CacheType>& container : containers)
        {
            total_weight += container->weight();
        }
        return total_weight;
    }

private:
    size_t shard_num;
    ShardHash hasher;
    std::vector<std::unique_ptr<CacheType>> containers;
};

}
