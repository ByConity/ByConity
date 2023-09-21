/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Disks/IDisk.h>
#include <Disks/IVolume.h>
#include <Storages/DiskCache/DiskCacheSettings.h>
#include <Storages/DiskCache/IDiskCacheSegment.h>
#include <Storages/DiskCache/IDiskCacheStrategy.h>
#include <Common/Exception.h>
#include <Common/Throttler.h>
#include <common/logger_useful.h>

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
    static void init(const Context & global_context);
    static void close();
    static ThreadPool & getThreadPool();
    static ThreadPool & getEvictPool();

    explicit IDiskCache(const VolumePtr & volume_, const ThrottlerPtr & throttler, const DiskCacheSettings & settings);

    virtual ~IDiskCache()
    {
        try
        {
            shutdown();
        }
        catch (...) {}
    }

    IDiskCache(const IDiskCache &) = delete;
    IDiskCache & operator=(const IDiskCache &) = delete;

    virtual void shutdown();

    /// set segment name in cache and write value to disk cache
    virtual void set(const String & key, ReadBuffer & value, size_t weight_hint) = 0;

    /// get segment from cache and return local path if exists.
    virtual std::pair<DiskPtr, String> get(const String & key) = 0;

    /// initialize disk cache from local disk
    virtual void load() = 0;

    virtual size_t drop(const String & part_name) = 0;

    /// get number of keys
    virtual size_t getKeyCount() const = 0;

    /// get cached files size
    virtual size_t getCachedSize() const = 0;

    virtual IDiskCacheStrategyPtr getStrategy() const = 0;

    using CacheSegmentsCallback = std::function<void(const String &, const int &)>;
    void cacheSegmentsToLocalDisk(IDiskCacheSegmentsVector hit_segments, CacheSegmentsCallback callback = {});

    VolumePtr getStorageVolume() const { return volume; }
    ThrottlerPtr getDiskCacheThrottler() const { return disk_cache_throttler; }
    Poco::Logger * getLogger() const { return log; }
    String getDataDir() const {return latest_disk_cache_dir;}

protected:
    VolumePtr volume;
    ThrottlerPtr disk_cache_throttler;
    DiskCacheSettings settings;

    std::atomic<bool> shutdown_called {false};
    BackgroundSchedulePool::TaskHolder sync_task;
    String previous_disk_cache_dir;// load previous folder cached data for compatible if data dir config is changed
    String latest_disk_cache_dir;

private:
    bool scheduleCacheTask(const std::function<void()> & task);

    static std::unique_ptr<ThreadPool> local_disk_cache_thread_pool;
    static std::unique_ptr<ThreadPool> local_disk_cache_evict_thread_pool;

    Poco::Logger * log = &Poco::Logger::get("DiskCache");
};

using IDiskCachePtr = std::shared_ptr<IDiskCache>;

}
