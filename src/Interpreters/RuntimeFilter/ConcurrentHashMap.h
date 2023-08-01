#pragma once

#include <unordered_map>
#include <unordered_set>
#include <Interpreters/Context.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

namespace DB
{

template <typename Key, typename Value, typename Hash = std::hash<Key>>
class ConcurrentMapShard
{
public:
    void put(const Key & key, Value value)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        map_data.emplace(key, value);
    }

    size_t size()
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        return map_data.size();
    }

    Value get(const Key & key)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        auto it = map_data.find(key);
        if (it == map_data.end())
            throw Exception("Key not found", ErrorCodes::LOGICAL_ERROR);
        return map_data[key];
    }

    Value compute(const Key & key, std::function<Value(const Key &, Value)> func)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        auto it = map_data.find(key);
        if (it == map_data.end())
        {
            auto res = func(key, Value{});
            map_data.emplace(key, res);
            return res;
        }
        auto res = func(key, it->second);
        it->second = res;
        return res;
    }

    bool remove(const Key & key)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        return map_data.erase(key);
    }

private:
    bthread::Mutex mutex;
    std::unordered_map<Key, Value, Hash> map_data;
};

template <typename Key, typename Value, typename Hash = std::hash<Key>>
class ConcurrentHashMap
{
public:
    explicit ConcurrentHashMap(size_t num_shard = 128, Hash const & hash_function_ = Hash())
        : hash_function(hash_function_), shards(num_shard), log(&Poco::Logger::get("ConcurrentShardMap"))
    {
        for (unsigned i = 0; i < num_shard; ++i)
            shards[i].reset(new ConcurrentMapShard<Key, Value, Hash>());
    }

    void put(const Key & key, Value value) { getShard(key).put(key, value); }

    Value get(const Key & key) { return getShard(key).get(key); }

    Value compute(const Key & key, std::function<Value(const Key &, Value)> func) { return getShard(key).compute(key, func); }

    bool remove(const Key & key) { return getShard(key).remove(key); }

    size_t size()
    {
        size_t total_size = 0;
        std::for_each(shards.begin(), shards.end(), [&](std::unique_ptr<ConcurrentMapShard<Key, Value>> & element) {
            total_size += element->size();
        });
        return total_size;
    }

private:
    ConcurrentMapShard<Key, Value> & getShard(const Key & key)
    {
        std::size_t const shard_index = hash_function(key) % shards.size();
        return *shards[shard_index];
    }

    Hash hash_function;
    std::vector<std::unique_ptr<ConcurrentMapShard<Key, Value, Hash>>> shards;
    Poco::Logger * log;
};

}
