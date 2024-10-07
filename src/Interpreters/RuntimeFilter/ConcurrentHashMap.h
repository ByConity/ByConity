#pragma once

#include <Common/Logger.h>
#include <unordered_map>
#include <unordered_set>
#include <Interpreters/Context.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <bthread/shared_mutex.h>

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

    std::pair<Value, bool> tryGet(const Key & key)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        auto it = map_data.find(key);
        if (it == map_data.end())
        {
            Value default_value;
            return {default_value, false};
        }
        else
        {
            return {it->second, true};
        }
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

    void clear()
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        return map_data.clear();
    }

    size_t removeKeys(const std::unordered_set<Key> & key_set)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        size_t res = 0;
        for (const auto & key: key_set)
        {
            res += map_data.erase(key);
        }
        return res;
    }

    std::pair<Value, bool> tryEmplace(const Key & key, const Value & value)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        auto res = map_data.emplace(key, value);
        return {res.first->second, res.second};
    }

    bool iterateWithHandler(std::function<bool(const Key &, Value)> handler)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        for(const auto & ele : map_data)
        {
            auto should_continue = handler(ele.first, ele.second);
            if (!should_continue)
            {
                return should_continue;
            }
        }
        return true;
    }
    
    template<typename F>
    void computeExpireKeys(F && f)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        if (map_data.empty())
            return;
        for (const auto & [k, v] : map_data)
        {
            f(k, v);
        }
    }

private:
    bthread::Mutex mutex;
    std::unordered_map<Key, Value, Hash> map_data;
};

template <typename Key, typename Value, typename Hash = std::hash<Key>>
class ConcurrentReadMapShard
{
public:
    void put(const Key & key, Value value)
    {
        std::unique_lock<bthread::SharedMutex> lock(mutex);
        map_data.emplace(key, value);
    }

    size_t size()
    {
        std::shared_lock<bthread::SharedMutex> lock(mutex);
        return map_data.size();
    }

    Value get(const Key & key)
    {
        std::shared_lock<bthread::SharedMutex> lock(mutex);
        auto it = map_data.find(key);
        if (it == map_data.end())
            throw Exception("Key not found", ErrorCodes::LOGICAL_ERROR);
        return map_data[key];
    }

    std::pair<Value, bool> tryGet(const Key & key)
    {
        std::shared_lock<bthread::SharedMutex> lock(mutex);
        auto it = map_data.find(key);
        if (it == map_data.end())
        {
            Value default_value;
            return {default_value, false};
        }
        else
        {
            return {it->second, true};
        }
    }

    Value compute(const Key & key, std::function<Value(const Key &, Value)> func)
    {
        std::unique_lock<bthread::SharedMutex> lock(mutex);
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
        std::unique_lock<bthread::SharedMutex> lock(mutex);
        return map_data.erase(key);
    }

    void clear()
    {
        std::unique_lock<bthread::SharedMutex> lock(mutex);
        return map_data.clear();
    }

    size_t removeKeys(const std::unordered_set<Key> & key_set)
    {
        std::unique_lock<bthread::SharedMutex> lock(mutex);
        size_t res = 0;
        for (const auto & key: key_set)
        {
            res += map_data.erase(key);
        }
        return res;
    }

    std::pair<Value, bool> tryEmplace(const Key & key, const Value & value)
    {
        std::unique_lock<bthread::SharedMutex> lock(mutex);
        auto res = map_data.emplace(key, value);
        return {res.first->second, res.second};
    }

    bool iterateWithHandler(std::function<bool(const Key &, Value)> handler)
    {
        std::shared_lock<bthread::SharedMutex> lock(mutex);
        for(const auto & ele : map_data)
        {
            auto should_continue = handler(ele.first, ele.second);
            if (!should_continue)
            {
                return should_continue;
            }
        }
        return true;
    }

private:
    bthread::SharedMutex mutex;
    std::unordered_map<Key, Value, Hash> map_data;
};

template <typename Key, typename Value, typename Hash = std::hash<Key>, typename Shard = ConcurrentMapShard<Key, Value, Hash>>
class ConcurrentHashMap
{
public:
    explicit ConcurrentHashMap(size_t num_shard = 128, Hash const & hash_function_ = Hash())
        : hash_function(hash_function_), shards(num_shard), log(getLogger("ConcurrentShardMap"))
    {
        for (unsigned i = 0; i < num_shard; ++i)
            shards[i].reset(new Shard());
    }

    void put(const Key & key, Value value) { getShard(key).put(key, value); }

    Value get(const Key & key) { return getShard(key).get(key); }

    std::pair<Value, bool> tryGet(const Key & key)
    {
        return getShard(key).tryGet(key);
    }

    Value compute(const Key & key, std::function<Value(const Key &, Value)> func) { return getShard(key).compute(key, func); }

    bool remove(const Key & key) { return getShard(key).remove(key); }

    template<typename F>
    void computeExpireKeys(F && f)
    {
        std::for_each(shards.begin(), shards.end(), [&](std::unique_ptr<ConcurrentMapShard<Key, Value>> & element) {
            element->computeExpireKeys(std::forward<F>(f));
        });
    }

    size_t size()
    {
        size_t total_size = 0;
        std::for_each(shards.begin(), shards.end(), [&](std::unique_ptr<Shard> & element) {
            total_size += element->size();
        });
        return total_size;
    }

    void clear()
    {
        std::for_each(shards.begin(), shards.end(), [&](std::unique_ptr<Shard> & element) {
            element->clear();
        });
    }

    size_t removeKeys(const std::unordered_set<Key> & key_set)
    {
        size_t total_size = 0;
        std::for_each(shards.begin(), shards.end(), [&](std::unique_ptr<Shard> & element) {
            total_size += element->removeKeys(key_set);
        });
        return total_size;
    }

    std::pair<Value, bool> tryEmplace(const Key & key, const Value & value)
    {
        return getShard(key).tryEmplace(key, value);
    }

    void iterateWithHandler(std::function<bool(const Key &, Value)> handler)
    {
        for(const auto & element : shards)
        {
            auto should_continue = element->iterateWithHandler(handler);
            if (!should_continue)
            {
                return;
            }
        }
    }

private:
    Shard & getShard(const Key & key)
    {
        std::size_t const shard_index = hash_function(key) % shards.size();
        return *shards[shard_index];
    }

    Hash hash_function;
    std::vector<std::unique_ptr<Shard>> shards;
    LoggerPtr log;
};

}
