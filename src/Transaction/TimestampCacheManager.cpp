#include "TimestampCacheManager.h"
#include <common/logger_useful.h>

namespace DB
{
TimestampCacheTableGuard::TimestampCacheTableGuard(TableMutexMap & map_, std::unique_lock<std::mutex> guard_lock_, const UUID & uuid)
    : map(map_), guard_lock(std::move(guard_lock_))
{
    it = map.emplace(uuid, Entry{std::make_unique<std::mutex>(), 0}).first;
    ++it->second.counter;
    guard_lock.unlock();
    table_lock = std::unique_lock<std::mutex>(*it->second.mutex);
}

TimestampCacheTableGuard::~TimestampCacheTableGuard()
{
    guard_lock.lock();
    --it->second.counter;
    if (!it->second.counter)
    {
        table_lock.unlock();
        map.erase(it);
    }
}

std::unique_ptr<TimestampCacheTableGuard> TimestampCacheManager::getTimestampCacheTableGuard(const UUID & uuid)
{
    std::unique_lock<std::mutex> lock(mutex);
    return std::make_unique<TimestampCacheTableGuard>(table_guard, std::move(lock), uuid);
}

TimestampCachePtr & TimestampCacheManager::getTimestampCacheUnlocked(const UUID & uuid)
{
    if (tsCaches.find(uuid) == tsCaches.end())
        tsCaches.emplace(uuid, std::make_unique<TimestampCache>(max_size));

    return tsCaches[uuid];
}

}
