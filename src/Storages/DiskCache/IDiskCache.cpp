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

#include "IDiskCache.h"

#include <random>
#include <Interpreters/Context.h>
#include <Common/Stopwatch.h>
#include <common/logger_useful.h>
#include "Storages/DiskCache/DiskCache_fwd.h"
#include "Storages/DiskCache/IDiskCacheSegment.h"

namespace ProfileEvents
{
extern const Event DiskCacheScheduleCacheTaskMicroseconds;
extern const Event DiskCacheTaskDropCount;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_SCHEDULE_TASK;
}

std::unique_ptr<ThreadPool> IDiskCache::local_disk_cache_thread_pool;
std::unique_ptr<ThreadPool> IDiskCache::local_disk_cache_evict_thread_pool;
std::unique_ptr<ThreadPool> IDiskCache::local_disk_cache_preload_thread_pool;

void IDiskCache::init(const Context & global_context)
{
    if (local_disk_cache_thread_pool)
        throw Exception("disk cache thread pool is inited twice", ErrorCodes::LOGICAL_ERROR);

    if (local_disk_cache_evict_thread_pool)
        throw Exception("disk cache evict thread pool is initialized twice", ErrorCodes::LOGICAL_ERROR);

    auto settings = global_context.getSettingsRef();

    /// copy the old init logic.
    local_disk_cache_thread_pool = std::make_unique<ThreadPool>(
            settings.local_disk_cache_thread_pool_size,
            settings.local_disk_cache_thread_pool_size,
            settings.local_disk_cache_thread_pool_size * 100);

    local_disk_cache_evict_thread_pool = std::make_unique<ThreadPool>(
            settings.local_disk_cache_evict_thread_pool_size,
            settings.local_disk_cache_evict_thread_pool_size,
            settings.local_disk_cache_evict_thread_pool_size * 100);
    
    local_disk_cache_preload_thread_pool = std::make_unique<ThreadPool>(
            settings.cnch_parallel_preloading,
            settings.cnch_parallel_preloading,
            settings.cnch_parallel_preloading * 100);
}

void IDiskCache::close()
{
    if (local_disk_cache_thread_pool)
        local_disk_cache_thread_pool.reset();
    if (local_disk_cache_evict_thread_pool)
        local_disk_cache_evict_thread_pool.reset();
    if (local_disk_cache_preload_thread_pool)
        local_disk_cache_preload_thread_pool.reset();
}

ThreadPool & IDiskCache::getThreadPool()
{
    if (!local_disk_cache_thread_pool)
        throw Exception("Uninitialized disk cache thread pool", ErrorCodes::CANNOT_SCHEDULE_TASK);
    return *local_disk_cache_thread_pool;
}

ThreadPool & IDiskCache::getEvictPool()
{
    if (!local_disk_cache_evict_thread_pool)
        throw Exception("Uninitialized disk cache thread pool", ErrorCodes::CANNOT_SCHEDULE_TASK);
    return *local_disk_cache_evict_thread_pool;
}

ThreadPool & IDiskCache::getPreloadPool()
{
    if (!local_disk_cache_preload_thread_pool)
        throw Exception("Uninitialized disk cache thread pool", ErrorCodes::CANNOT_SCHEDULE_TASK);
    return *local_disk_cache_preload_thread_pool;
}


IDiskCache::IDiskCache(
    const String & name_,
    const VolumePtr & volume_,
    const ThrottlerPtr & throttler_,
    const DiskCacheSettings & settings_,
    const IDiskCacheStrategyPtr & strategy_,
    bool support_multi_cache_,
    IDiskCache::DataType type_)
    : volume(volume_)
    , disk_cache_throttler(throttler_)
    , settings(settings_)
    , strategy(strategy_)
    , latest_disk_cache_dir(settings_.latest_disk_cache_dir)
    , support_multi_cache(support_multi_cache_)
    , type(type_)
    , name(name_)
    , log(&Poco::Logger::get(fmt::format("DiskCache(name={})", getName())))
{
    if (!settings.previous_disk_cache_dir.empty())
    {
        boost::replace_all(settings.previous_disk_cache_dir, " ", "");
        boost::split(previous_disk_cache_dirs, settings.previous_disk_cache_dir, boost::is_any_of(","));
    }
}

void IDiskCache::shutdown()
{
    shutdown_called = true;
    if (sync_task)
        sync_task->deactivate();
}

void IDiskCache::cacheSegmentsToLocalDisk(IDiskCacheSegmentsVector hit_segments, CacheSegmentsCallback callback)
{
    if (hit_segments.empty())
        return;

    Stopwatch watch;
    SCOPE_EXIT({ ProfileEvents::increment(ProfileEvents::DiskCacheScheduleCacheTaskMicroseconds, watch.elapsedMicroseconds()); });

    // Notes: split to more tasks?
    bool success = scheduleCacheTask([this, segments = std::move(hit_segments), cb = callback] {
        try
        {
            std::exception_ptr first_exception;
            for (const auto & hit_segment : segments)
            {
                try
                {
                    String mark_name = hit_segment->getMarkName();
                    String segment_name = hit_segment->getSegmentName();
                    if ((!mark_name.empty() && !getMetaCache()->get(mark_name).second.empty())
                        && !getDataCache()->get(segment_name).second.empty())
                        continue;

                    hit_segment->cacheToDisk(*this);
                }
                catch (...)
                {
                    if (!first_exception)
                        first_exception = std::current_exception();
                    tryLogCurrentException(log, "Failed to cache segment " + hit_segment->getSegmentName());
                }
            }

            if (cb)
                cb(first_exception, segments.size());
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }
    });

    if (!success)
        ProfileEvents::increment(ProfileEvents::DiskCacheTaskDropCount);
}

void IDiskCache::cacheBitmapIndexToLocalDisk(const IDiskCacheSegmentPtr & bitmap_segment)
{
    Stopwatch watch;
    SCOPE_EXIT({ ProfileEvents::increment(ProfileEvents::DiskCacheScheduleCacheTaskMicroseconds, watch.elapsedMicroseconds()); });

    bool success = scheduleCacheTask([this, bitmap_segment] {
        try
        {
            bitmap_segment->cacheToDisk(*this);
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
    });
    if (!success)
        ProfileEvents::increment(ProfileEvents::DiskCacheTaskDropCount);
}

// Schedule cache task, when threadpool's current running task exceed certain ratio, start random
// drop disk cache task
bool IDiskCache::scheduleCacheTask(const std::function<void()> & task)
{
    if (shutdown_called)
        return false;

    auto & thread_pool = IDiskCache::getThreadPool();
    size_t active_task_size = thread_pool.active();
    size_t max_queue_size = thread_pool.getMaxQueueSize();
    // (Running + Pending tasks) / (Max Running + Max Pending tasks)
    size_t current_ratio = max_queue_size == 0 ? 0 : ((active_task_size * 100) / max_queue_size);

    if (current_ratio <= settings.random_drop_threshold || settings.random_drop_threshold >= 100)
    {
        return thread_pool.trySchedule(task);
    }
    else
    {
        // Drop disk cache task base on queue's full ratio
        // (current task queue full ratio/ (100 - random_drop_threshold)) * 100
        // The drop possibility when current_ratio == random_drop_threshold is 0%
        // The drop possibility when current_ratio == 100 is 100%
        size_t drop_possibility = (100 * (current_ratio - settings.random_drop_threshold)) / (100 - settings.random_drop_threshold);
        std::random_device rd;
        std::mt19937 random_generator(rd());
        std::uniform_int_distribution<size_t> dist(1, 100);
        if (dist(random_generator) <= drop_possibility)
        {
            LOG_DEBUG(log, "Drop disk cache since queue is almost full, Queue length: {}, Max: {}, curren_ratio: {} ", active_task_size, max_queue_size, current_ratio);
            ProfileEvents::increment(ProfileEvents::DiskCacheTaskDropCount, 1, Metrics::MetricType::Meter);
            return false;
        }
        else
        {
            return thread_pool.trySchedule(task);
        }
    }
}

}
