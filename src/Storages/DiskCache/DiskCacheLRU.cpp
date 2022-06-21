#include "DiskCacheLRU.h"

#include <optional>
#include <Disks/IVolume.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>
#include <common/scope_guard.h>

namespace CurrentMetrics
{
extern const Metric DiskCacheEvictQueueLength;
}

namespace ProfileEvents
{
extern const Event DiskCacheGetMicroSeconds;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int DISK_CACHE_SEGMENT_SIZE_CHANGED;
    extern const int CORRUPTED_DATA;
}

DiskCacheLRU::DiskCacheLRU(
    Context & context_,
    VolumePtr storage_volume_,
    const DiskCacheSettings & settings_)
    : IDiskCache(context_, storage_volume_, settings_)
    , Base(settings_.lru_max_size, settings_.lru_update_interval, settings_.mapping_bucket_size)
    , logger(&Poco::Logger::get("DiskCache"))
{
    base_path = settings_.cache_base_path.empty() ? "disk_cache/" : settings_.cache_base_path;
    if (!base_path.ends_with('/'))
        base_path += '/';
}

void DiskCacheLRU::set(const String & key, ReadBuffer & value, size_t weight_hint)
{
    /// TODO @(cl) use getOrSet Here
    if (!isExist(key))
    {
        try
        {
            auto meta = std::make_shared<DiskCacheMeta>(DiskCacheState::Caching, weight_hint, "");
            Base::set(key, meta);
            // NOTE(wsy) potenial race condition, lru cache may evict caching entry
            // Maybe let lrucache skip evict entry with 0 weight
            auto [disk_name, file_size] = writeSegment(key, value, weight_hint);
            {
                std::lock_guard lock(meta->mutex);
                meta->state = DiskCacheState::Cached;
            }
        }
        catch (...)
        {
            Base::remove(key);
        }
    }
}

std::pair<String, size_t> DiskCacheLRU::writeSegment(const String & key, ReadBuffer & value, size_t weight_hint)
{
    ReservationPtr reserved_space = nullptr;
    /*
    if (weight_hint == 0)
        reserved_space = storage_volume->makeEmptyReservationOnLargestDisk();
    else
        reserved_space = storage_volume->reserve(weight_hint);
    */
    // TODO: select disk
    reserved_space = storage_volume->reserve(weight_hint);

    if (reserved_space == nullptr)
        throw Exception("Can't reserve enough space on disk, weight_hint: " + std::to_string(weight_hint), ErrorCodes::LOGICAL_ERROR);

    DiskPtr disk = reserved_space->getDisk();
    String cache_rel_path = "disk_cache/" + key;
    String temp_cache_rel_path = cache_rel_path + ".temp";
    try
    {
        // Create parent directories
        disk->createDirectories(Poco::Path(temp_cache_rel_path).parent().toString());

        // write into temp file
        WriteBufferFromFile to(disk->getPath() + temp_cache_rel_path);
        copyData(value, to);
        // flush the last buffer to disk
        to.next();

        std::unique_lock<std::shared_mutex> lock(evict_mutex);
        disk->removeFileIfExists(cache_rel_path);
        disk->moveFile(temp_cache_rel_path, cache_rel_path);
        return std::pair<String, size_t>(disk->getName(), disk->getFileSize(cache_rel_path));
    }
    catch (...)
    {
        disk->removeFileIfExists(temp_cache_rel_path);
        disk->removeFileIfExists(cache_rel_path);
        LOG_ERROR(logger, "{} write disk cache file failed", key);
        tryLogCurrentException("DiskCache", __PRETTY_FUNCTION__);
        throw;
    }
}

std::optional<String> DiskCacheLRU::get(const String & key)
{
    Stopwatch watch;
    SCOPE_EXIT(ProfileEvents::increment(ProfileEvents::DiskCacheGetMicroSeconds, watch.elapsedMicroseconds()));

    auto meta = Base::get(key);
    if (!meta || meta->state != DiskCacheState::Cached)
        return {};

    try
    {
        // DiskPtr disk = storage_volume->getDiskByName(ret->disk_name);
        DiskPtr disk = context.getDisk(meta->disk_name);
        return disk->getPath() + base_path + key;
    }
    catch (...)
    {
        Base::remove(key);
        tryLogCurrentException(logger);
    }

    return {};
}

void DiskCacheLRU::load()
{
    size_t cached_segments = loadSegmentsFromVolume(*storage_volume);
    LOG_DEBUG(logger, "Loaded {} segments from meta file", cached_segments);
}

size_t DiskCacheLRU::loadSegmentsFromVolume(const IVolume & volume)
{
    size_t segment_from_disk = 0;
    Disks disks = volume.getDisks();
    for (const DiskPtr & disk : disks)
    {
        String path;
        segment_from_disk += loadSegmentsFromDisk(*disk, base_path, path);
    }
    return segment_from_disk;
}

size_t DiskCacheLRU::loadSegmentsFromDisk(IDisk & disk, const String & current_path, String & partial_cache_name)
{
    size_t segment_from_disk = 0;
    for (auto iter = disk.iterateDirectory(current_path); iter->isValid(); iter->next())
    {
        if (disk.isFile(iter->path()))
        {
            if (loadSegmentFromFile(disk, iter->path(), partial_cache_name + iter->name()))
            {
                ++segment_from_disk;
            }
        }
        else if (disk.isDirectory(iter->path()))
        {
            // load segment on subdirectory, this won't be many recursive call
            // since disk cache is stored as table_uuid/part_name/cache_file
            String dir_name = iter->name() + '/';
            partial_cache_name += dir_name;
            segment_from_disk += loadSegmentsFromDisk(disk, iter->path(), partial_cache_name);
            partial_cache_name.resize(partial_cache_name.size() - dir_name.size());
        }
    }
    return segment_from_disk;
}

bool DiskCacheLRU::loadSegmentFromFile(IDisk & disk, const String & segment_rel_path, const String & segment_name)
{
    if (endsWith(segment_name, ".temp"))
    {
        // Only remove temp file if it didn't appear in cache, otherwise we may delete
        // caching entry
        if (!isExist(segment_name))
        {
            disk.removeFile(segment_rel_path);
        }
        return false;
    }
    else
    {
        return loadSegment(disk, segment_rel_path, segment_name, false);
    }
}

bool DiskCacheLRU::loadSegment(IDisk & disk, const String & segment_rel_path, const String & segment_name, bool need_check_existence)
{
    if (need_check_existence && !disk.exists(segment_rel_path))
    {
        return false;
    }

    if (isExist(segment_name))
    {
        return false;
    }

    // Currently there won't be any data with old name format(which all cache stored in same directory)
    // So the code about old key format compatability is removed

    size_t file_size = disk.getFileSize(segment_rel_path);
    Base::set(segment_name, std::make_shared<DiskCacheMeta>(DiskCacheState::Cached, file_size, disk.getName()));

    return true;
}

bool DiskCacheLRU::isExist(const String & key)
{
    return Base::get(key) != nullptr;
}

/// TODO remove cache segment async
void DiskCacheLRU::removeExternal(const Key & key, const std::shared_ptr<DiskCacheMeta> & value, size_t /*weight*/)
{
    auto & thread_pool = context.getLocalDiskCacheEvictThreadPool();

    auto remove_impl = [this, key, disk_name = value->disk_name, &thread_pool]() {
        std::shared_lock<std::shared_mutex> lock(evict_mutex);
        /// isExist need to lock lru mutex, there is a dead lock case:
        /// DiskCacheMetaSyncThread holds mutex lock to wait the task is scheduled by DiskCacheEvictThreadPool,
        /// but DiskCacheEvictThreadPool may wait the lock held by DiskCacheMetaSyncThread that will lead to dead lock,
        /// here we remove isExist judgment first but this will cause a potential problem:
        /// the key is stored in lru cache memory but does not exist in disk cache,
        /// we need to fix it later although this problem does not cause the query to fail.
        /// if (!isExist(key))
        /// {
        // DiskPtr disk = storage_volume->getDiskByName(disk_name);
        DiskPtr disk = context.getDisk(disk_name);

        String cache_path = "disk_cache/" + key;
        if (disk != nullptr)
        {
            disk->removeFileIfExists(cache_path);
        }
        else
        {
            LOG_ERROR(logger, "Disk {} not found when trying to remove {} from disk cache", disk_name, key);
        }
        /// }

        CurrentMetrics::set(CurrentMetrics::DiskCacheEvictQueueLength, thread_pool.active() - 1);
    };

    CurrentMetrics::set(CurrentMetrics::DiskCacheEvictQueueLength, thread_pool.active());

    // NOTE(wsy) Potential race condition, remove cache file after this key is
    // cache again
    thread_pool.scheduleOrThrow(remove_impl);
}

}
