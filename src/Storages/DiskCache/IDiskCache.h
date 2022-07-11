#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Storages/DiskCache/DiskCacheSettings.h>
#include <Storages/DiskCache/IDiskCacheSegment.h>

#include <common/logger_useful.h>
#include <optional>

namespace DB
{
class Context;
class ReadBuffer;
class WriteBuffer;
class Throttler;
class IVolume;
class IDisk;

using ThrottlerPtr = std::shared_ptr<Throttler>;
using VolumePtr = std::shared_ptr<IVolume>;
using DiskPtr = std::shared_ptr<IDisk>;

class IDiskCache
{
public:
    explicit IDiskCache(Context & context_, VolumePtr volume_, const DiskCacheSettings & settings);
    virtual ~IDiskCache();

    IDiskCache(const IDiskCache &) = delete;
    IDiskCache & operator=(const IDiskCache &) = delete;

    void asyncLoad();

    /// set segment name in cache and write value to disk cache
    virtual void set(const String & key, ReadBuffer & value, size_t weight_hint) = 0;

    /// get segment from cache and return local path if exists.
    virtual std::pair<DiskPtr, String> get(const String & key) = 0;

    /// initialize disk cache from local disk
    virtual void load() = 0;

    /// get number of keys
    virtual size_t getKeyCount() const = 0;

    /// get cached files size
    virtual size_t getCachedSize() const = 0;

    void cacheSegmentsToLocalDisk(IDiskCacheSegmentsVector hit_segments);

    VolumePtr getStorageVolume() const { return storage_volume; }
    ThrottlerPtr getDiskCacheThrottler() const { return disk_cache_throttler; }
    Poco::Logger * getLogger() const { return log; }

protected:
    Context & context;
    VolumePtr storage_volume;
    ThrottlerPtr disk_cache_throttler;
    size_t random_drop_threshold;
    BackgroundSchedulePool::TaskHolder sync_task;

private:
    bool scheduleCacheTask(const std::function<void()> & task);

    Poco::Logger * log = &Poco::Logger::get("DiskCache");
};

using IDiskCachePtr = std::shared_ptr<IDiskCache>;

}
