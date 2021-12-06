#pragma once

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int DISTRIBUTE_STAGE_QUERY_EXCEPTION;
    extern const int MEMORY_LIMIT_EXCEEDED;
}

template <typename KeyType, typename ElementType>
class ConcurrentShardElement
{
private:
    bthread::Mutex mutex;
    std::unordered_map<KeyType, ElementType> map_data;
    std::unordered_map<KeyType, bthread::ConditionVariable> events;
    Poco::Logger * log;

public:
    ConcurrentShardElement() { log = &Poco::Logger::get("ConcurrentShardElement"); }

    bool empty()
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        return map_data.empty();
    }

    size_t size()
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        return map_data.size();
    }

    bool exist(const KeyType & key)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        return map_data.find(key) != map_data.end();
    }

    void put(const KeyType & key, ElementType value)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        map_data.emplace(key, value);
    }

    ElementType & get(const KeyType & key)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        return map_data[key];
    }

    bool remove(const KeyType & key)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        auto n = map_data.erase(key);
        return n;
    }

    String keys()
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        std::ostringstream oss;
        for (const auto & element : map_data)
            oss << element.first << std::endl;
        return oss.str();
    }

    void removeIf(std::function<bool(std::pair<KeyType, ElementType>)> predicate)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        for (auto data_iter = map_data.begin(); data_iter != map_data.end();)
        {
            if (predicate(*data_iter))
            {
                LOG_DEBUG(log, "remove this key-", data_iter->first);
                map_data.erase(data_iter++);
            }
            else
                data_iter++;
        }
    }
};
}
