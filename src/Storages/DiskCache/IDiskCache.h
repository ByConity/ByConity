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

#include <Common/Logger.h>
#include <exception>
#include <vector>
#include <Core/BackgroundSchedulePool.h>
#include <Disks/IDisk.h>
#include <Disks/IVolume.h>
#include <Storages/DiskCache/DiskCacheSettings.h>
#include <Storages/DiskCache/IDiskCacheSegment.h>
#include <Storages/DiskCache/IDiskCacheStrategy.h>
#include <fmt/core.h>
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

class IDiskCache : public std::enable_shared_from_this<IDiskCache>
{
public:
    static void init(const Context & global_context);
    static void close();
    static ThreadPool & getThreadPool();
    static ThreadPool & getEvictPool();
    static ThreadPool & getPreloadPool();

    enum class DataType
    {
        ALL,
        MULTI,
        META,
        DATA,
    };

    explicit IDiskCache(
        const String & name_,
        const VolumePtr & volume_,
        const ThrottlerPtr & throttler,
        const DiskCacheSettings & settings,
        const IDiskCacheStrategyPtr & strategy_,
        bool support_multi_cache_ = false,
        IDiskCache::DataType type_ = DataType::ALL);

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
    virtual void set(const String & key, ReadBuffer & value, size_t weight_hint, bool is_preload) = 0;

    /// get segment from cache and return local path if exists.
    virtual std::pair<DiskPtr, String> get(const String & key) = 0;

    /// initialize disk cache from local disk
    virtual void load() = 0;

    virtual size_t drop(const String & part_name) = 0;

    /// get number of keys
    virtual size_t getKeyCount() const = 0;

    /// get cached files size
    virtual size_t getCachedSize() const = 0;

    virtual IDiskCacheStrategyPtr getStrategy() { return strategy; }

    using CacheSegmentsCallback = std::function<void(std::exception_ptr, const int &)>;
    void cacheSegmentsToLocalDisk(IDiskCacheSegmentsVector hit_segments, CacheSegmentsCallback callback = {});
    void cacheBitmapIndexToLocalDisk(const IDiskCacheSegmentPtr & bitmap_segment);

    VolumePtr getStorageVolume() const { return volume; }
    ThrottlerPtr getDiskCacheThrottler() const { return disk_cache_throttler; }
    LoggerPtr getLogger() const { return log; }
    String getDataDir() const {return latest_disk_cache_dir;}

    virtual std::shared_ptr<IDiskCache> getMetaCache() { return shared_from_this(); }
    virtual std::shared_ptr<IDiskCache> getDataCache() { return shared_from_this(); }

    bool supportMultiDiskCache() const {return support_multi_cache;}
    String getName() const
    {
        switch (type)
        {
            case DataType::ALL:
                return fmt::format("ALL({})", name);
            case DataType::MULTI:
                return fmt::format("MULTI({})", name);
            case DataType::META:
                return fmt::format("META({})", name);
            case DataType::DATA:
                return fmt::format("DATA({})", name);
            default:
                return fmt::format("ALL({})", name);
        }
    }

    DiskCacheSettings getSettings() const { return settings;}

protected:
    VolumePtr volume;
    ThrottlerPtr disk_cache_throttler;
    DiskCacheSettings settings;
    IDiskCacheStrategyPtr strategy;

    std::atomic<bool> shutdown_called {false};
    BackgroundSchedulePool::TaskHolder sync_task;
    std::vector<String> previous_disk_cache_dirs;// load previous folder cached data for compatible if data dir config is changed
    String latest_disk_cache_dir;

    bool support_multi_cache;
    IDiskCache::DataType type;
    String name;

    LoggerPtr log;

private:
    bool scheduleCacheTask(const std::function<void()> & task);

    static std::unique_ptr<ThreadPool> local_disk_cache_preload_thread_pool;
    static std::unique_ptr<ThreadPool> local_disk_cache_thread_pool;
    static std::unique_ptr<ThreadPool> local_disk_cache_evict_thread_pool;
};

using IDiskCachePtr = std::shared_ptr<IDiskCache>;

class MultiDiskCache : public IDiskCache
{
public:
    MultiDiskCache(
        const String & name_,
        const VolumePtr & volume_,
        const ThrottlerPtr & throttler_,
        const DiskCacheSettings & settings_,
        const IDiskCacheStrategyPtr & strategy_,
        const IDiskCachePtr & meta_,
        const IDiskCachePtr & data_)
        : IDiskCache(name_, volume_, throttler_, settings_, strategy_, true, IDiskCache::DataType::MULTI), meta(meta_), data(data_)
    {
    }

private:
    IDiskCachePtr meta;
    IDiskCachePtr data;

public:
    std::shared_ptr<IDiskCache> getMetaCache() override {
      if (!meta)
        throw Exception("MultiDiskCache `meta cache` is nullptr", ErrorCodes::LOGICAL_ERROR);
      return meta;
    }

    std::shared_ptr<IDiskCache> getDataCache() override {
        if (!data)
            throw Exception("MultiDiskCache `data cache` is nullptr", ErrorCodes::LOGICAL_ERROR);
        return data;
    }

    virtual size_t drop(const String & part_base_path) override {
        if (!meta || !data)
            throw Exception("MultiDiskCache `meta or data` cache is nullptr", ErrorCodes::LOGICAL_ERROR);

        size_t dropped_size = meta->drop(part_base_path);
        dropped_size += data->drop(part_base_path);// meta&data cache path is same, enable force=true to delete data cache map kv stat
        return dropped_size;
    }

    virtual void set(const String &, ReadBuffer &, size_t, bool ) override { throw Exception("MultiDiskCache `set` is not supported now", ErrorCodes::LOGICAL_ERROR);}
    virtual std::pair<DiskPtr, String> get(const String &) override { throw Exception("MultiDiskCache `get` is not supported now", ErrorCodes::LOGICAL_ERROR);}
    virtual void load() override { throw Exception("MultiDiskCache `load` is not supported now", ErrorCodes::LOGICAL_ERROR);}
    virtual size_t getKeyCount() const override {throw Exception("MultiDiskCache `getKeyCount` is not supported now", ErrorCodes::LOGICAL_ERROR); }
    virtual size_t getCachedSize() const override {throw Exception("MultiDiskCache `getCachedSize` is not supported now", ErrorCodes::LOGICAL_ERROR);}
};

using MultiDiskCachePtr = std::shared_ptr<MultiDiskCache>;

}
