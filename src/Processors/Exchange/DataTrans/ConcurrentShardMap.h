#pragma once

#include "ConcurrentShardElement.h"

#include <common/logger_useful.h>

namespace DB
{
template <typename KeyType, typename ElementType, typename Hash = std::hash<KeyType>>
class ConcurrentShardMap
{
  using ElementPtr = std::shared_ptr<ElementType>;

public:
    explicit ConcurrentShardMap(size_t num_shard = 128, Hash const & hash_function_ = Hash())
        : hash_function(hash_function_), shards(num_shard)
    {
        for (unsigned i = 0; i < num_shard; ++i)
        {
            shards[i].reset(new ConcurrentShardElement<KeyType, ElementPtr>());
        }
    }

    bool exist(const KeyType & key) { return getShard(key).exist(key); }

    void put(const KeyType & key, ElementPtr value) { getShard(key).put(key, value); }

    bool putIfNotExists(const KeyType & key, ElementPtr value) { return getShard(key).putIfNotExists(key, value); }

    ElementPtr get(const KeyType & key) { return getShard(key).get(key); }

    ElementPtr get(const KeyType & key, size_t timeout) { return getShard(key).get(key, timeout); }

    bool remove(const KeyType & key) { return getShard(key).remove(key); }

    size_t size()
    {
        size_t total_size = 0;
        std::for_each(shards.begin(), shards.end(), [&](std::unique_ptr<ConcurrentShardElement<KeyType, ElementPtr>> & element) {
            total_size += element->size();
        });
        return total_size;
    }

    String keys()
    {
        String ret;
        std::for_each(shards.begin(), shards.end(), [&](std::unique_ptr<ConcurrentShardElement<KeyType, ElementPtr>> & element) {
            ret += element->keys();
        });
        return ret;
    }

    void forEach(std::function<void(std::pair<KeyType, ElementPtr>)> func)
    {
        std::for_each(
            shards.begin(), shards.end(), [&](std::unique_ptr<ConcurrentShardElement<KeyType, ElementPtr>> & element) { func(element); });
    }

private:
    Poco::Logger * log = &Poco::Logger::get("ConcurrentShardMap");
    ConcurrentShardElement<KeyType, ElementPtr> & getShard(const KeyType & key)
    {
        std::size_t const shard_index = hash_function(key) % shards.size();
        return *shards[shard_index];
    }

    Hash hash_function;
    std::vector<std::unique_ptr<ConcurrentShardElement<KeyType, ElementPtr>>> shards;
};
}
