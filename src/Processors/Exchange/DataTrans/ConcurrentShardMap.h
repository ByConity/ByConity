#pragma once

#include "ConcurrentShardElement.h"

#include <common/logger_useful.h>

namespace DB
{
template <typename KeyType, typename ElementType, typename Hash = std::hash<KeyType>>
class ConcurrentShardMap
{
public:
    explicit ConcurrentShardMap(size_t num_shard = 128, Hash const & hash_function_ = Hash())
        : hash_function(hash_function_), shards(num_shard)
    {
        for (unsigned i = 0; i < num_shard; ++i)
        {
            shards[i].reset(new ConcurrentShardElement<KeyType, ElementType>());
        }
    }

    bool exist(const KeyType & key) { return getShard(key).exist(key); }

    void put(const KeyType & key, ElementType value) { getShard(key).put(key, value); }

    ElementType & get(const KeyType & key) { return getShard(key).get(key); }

    bool remove(const KeyType & key) { return getShard(key).remove(key); }

    void removeIf(std::function<bool(std::pair<KeyType, ElementType>)> predicate)
    {
        std::for_each(shards.begin(), shards.end(), [&](std::unique_ptr<ConcurrentShardElement<KeyType, ElementType>> & element) {
            if (!element->empty())
                element->removeIf(predicate);
        });
    }

    size_t size()
    {
        size_t total_size = 0;
        std::for_each(shards.begin(), shards.end(), [&](std::unique_ptr<ConcurrentShardElement<KeyType, ElementType>> & element) {
            total_size += element->size();
        });
        return total_size;
    }

    String keys()
    {
        std::ostringstream oss;
        std::for_each(shards.begin(), shards.end(), [&](std::unique_ptr<ConcurrentShardElement<KeyType, ElementType>> & element) {
            oss << element->keys();
        });
        return oss.str();
    }

    void forEach(std::function<void(std::pair<KeyType, ElementType>)> func)
    {
        std::for_each(
            shards.begin(), shards.end(), [&](std::unique_ptr<ConcurrentShardElement<KeyType, ElementType>> & element) { func(element); });
    }

private:
    Poco::Logger * log = &Poco::Logger::get("ConcurrentShardMap");
    ConcurrentShardElement<KeyType, ElementType> & getShard(const KeyType & key)
    {
        std::size_t const shard_index = hash_function(key) % shards.size();
        return *shards[shard_index];
    }

    Hash hash_function;
    std::vector<std::unique_ptr<ConcurrentShardElement<KeyType, ElementType>>> shards;
};
}
