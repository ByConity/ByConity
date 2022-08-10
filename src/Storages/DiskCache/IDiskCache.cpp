#include "IDiskCache.h"

#include <random>
#include <Interpreters/Context.h>
#include <Common/Stopwatch.h>
#include <common/logger_useful.h>

namespace ProfileEvents
{
extern const Event DiskCacheScheduleCacheTaskMicroSeconds;
}

namespace DB
{
IDiskCache::IDiskCache(Context & context_, VolumePtr volume_, const DiskCacheSettings & settings_)
    : context(context_)
    , storage_volume(std::move(volume_))
    , disk_cache_throttler(context.getDiskCacheThrottler())
    , random_drop_threshold(settings_.random_drop_threshold)
{
}

IDiskCache::~IDiskCache()
{
    if (sync_task)
        sync_task->deactivate();
}

void IDiskCache::asyncLoad()
{
    sync_task = context.getSchedulePool().createTask("DiskCacheMetaSync", [this] { load(); });
    sync_task->activateAndSchedule();
}

void IDiskCache::cacheSegmentsToLocalDisk(IDiskCacheSegmentsVector hit_segments)
{
    if (hit_segments.empty())
        return;

    Stopwatch watch;
    SCOPE_EXIT({ ProfileEvents::increment(ProfileEvents::DiskCacheScheduleCacheTaskMicroSeconds, watch.elapsedMicroseconds()); });

    // Notes: split to more tasks?
    scheduleCacheTask([this, segments = std::move(hit_segments)] {
        for (const auto & hit_segment : segments)
        {
            try
            {
                auto [disk, path] = get(hit_segment->getSegmentName());
                if (disk == nullptr)
                {
                    hit_segment->cacheToDisk(*this);
                }
            }
            catch (...)
            {
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
            }
        }
    });
}

// Schedule cache task, when threadpool's current running task exceed certain ratio, start random
// drop disk cache task
bool IDiskCache::scheduleCacheTask(const std::function<void()> & task)
{
    auto & thread_pool = context.getLocalDiskCacheThreadPool();
    size_t active_task_size = thread_pool.active();
    size_t max_queue_size = thread_pool.getMaxQueueSize();
    // (Running + Pending tasks) / (Max Running + Max Pending tasks)
    size_t current_ratio = max_queue_size == 0 ? 0 : ((active_task_size * 100) / max_queue_size);

    if (current_ratio <= random_drop_threshold || random_drop_threshold >= 100)
    {
        return thread_pool.trySchedule(task);
    }
    else
    {
        // Drop disk cache task base on queue's full ratio
        // (current task queue full ratio/ (100 - random_drop_threshold)) * 100
        // The drop possibility when current_ratio == random_drop_threshold is 0%
        // The drop possibility when current_ratio == 100 is 100%
        size_t drop_possibility = (100 * (current_ratio - random_drop_threshold)) / (100 - random_drop_threshold);
        std::random_device rd;
        std::mt19937 random_generator(rd());
        std::uniform_int_distribution<size_t> dist(1, 100);
        if (dist(random_generator) <= drop_possibility)
        {
            LOG_DEBUG(log, "Drop disk cache since queue is almost full, Queue length: {}, Max: {}", active_task_size, max_queue_size);
            return false;
        }
        else
        {
            return thread_pool.trySchedule(task);
        }
    }
}

}
