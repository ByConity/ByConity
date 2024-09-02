#include <chrono>
#include <shared_mutex>
#include <Statistics/StatisticsCache.h>
#include "Statistics/StatisticsBase.h"

namespace DB::Statistics
{
void StatisticsCache::invalidate(const UUID & table)
{
    std::unique_lock lock(mutex);
    if (auto iter = impl.find(table); iter != impl.end())
    {
        // move is faster
        auto tmp = std::move(iter->second);
        impl.erase(iter);
        lock.unlock();
    }
}

void StatisticsCache::clear()
{
    std::unique_lock lock(mutex);
    decltype(impl) tmp;
    tmp.swap(impl);
    lock.unlock();

    tmp.clear();
}

std::shared_ptr<StatsData> StatisticsCache::get(const UUID & table)
{
    auto now = chrono::steady_clock::now();
    std::shared_lock lck(mutex);
    if (auto iter = impl.find(table); iter != impl.end())
    {
        auto & entry = iter->second;
        if (now > entry.expire_time_point)
        {
            // we don't delete expired cache entry here since we are holding a shared mutex
            // expired entry will be cleaned only when invalidate and update
            // since normally a missed cache will usually be followed with a update request
            // this design won't have any performance issues comparing to Poco::ExpiredCache
            return nullptr;
        }
        return entry.data;
    }
    else
    {
        return nullptr;
    }
}

void StatisticsCache::update(const UUID & table, std::shared_ptr<StatsData> data)
{
    auto expire_time_point = chrono::steady_clock::now() + expire_time;
    std::unique_lock lck(mutex);
    impl[table] = CacheEntry{std::move(data), expire_time_point};
}

}
