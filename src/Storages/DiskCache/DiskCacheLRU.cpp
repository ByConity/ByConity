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
    extern const int NOT_ENOUGH_SPACE;
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
    , logger(&Poco::Logger::get("DiskCacheLRU"))
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
            auto cache_meta = std::make_shared<DiskCacheMeta>(DiskCacheState::Caching, weight_hint);
            Base::set(key, cache_meta);
            // NOTE(wsy) potenial race condition, lru cache may evict caching entry
            // Maybe let lrucache skip evict entry with 0 weight
            auto disk = writeSegment(key, value, weight_hint);
            {
                std::lock_guard lock(cache_meta->mutex);
                cache_meta->state = DiskCacheState::Cached;
                cache_meta->setDisk(disk);
            }
        }
        catch (...)
        {
            tryLogCurrentException(logger);
            Base::remove(key);
        }
    }
}

DiskPtr DiskCacheLRU::writeSegment(const String & key, ReadBuffer & value, size_t weight_hint)
{
    ReservationPtr reserved_space = storage_volume->reserve(weight_hint);
    if (!reserved_space)
        throw Exception("Can't reserve enough space on disk, weight_hint: ", ErrorCodes::LOGICAL_ERROR);

    DiskPtr disk = reserved_space->getDisk();
    String path = fs::path(base_path) / key;
    String tmp_path = path + ".temp";
    String dir_path = directoryPath(path);

    try
    {
        if (!disk->exists(dir_path))
            disk->createDirectories(dir_path);

        auto tmp_buffer = disk->writeFile(tmp_path, {});
        copyData(value, *tmp_buffer);
        tmp_buffer->finalize();

        disk->moveFile(tmp_path, path);
    }
    catch (...)
    {
        disk->removeFileIfExists(tmp_path);
        disk->removeFileIfExists(path);
        throw;
    }

    return disk;
}

std::pair<DiskPtr, String> DiskCacheLRU::get(const String & key)
{
    Stopwatch watch;
    SCOPE_EXIT(ProfileEvents::increment(ProfileEvents::DiskCacheGetMicroSeconds, watch.elapsedMicroseconds()));

    auto meta = Base::get(key);
    if (!meta || meta->state != DiskCacheState::Cached)
        return {};

    try
    {
        auto disk = meta->getDisk();
        if (!disk)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No disk for {}", key);
        String path = fs::path(base_path) / key;
        if (disk->exists(path))
            return {std::move(disk), std::move(path)};
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No disk cache path found {} in disk {}", path, disk->getName());
    }
    catch (...)
    {
        Base::remove(key);
        tryLogCurrentException(logger);
        return {};
    }
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
        segment_from_disk += loadSegmentsFromDisk(disk, base_path, path);
    }
    return segment_from_disk;
}

size_t DiskCacheLRU::loadSegmentsFromDisk(const DiskPtr & disk, const String & current_path, String & partial_cache_name)
{
    size_t segment_from_disk = 0;
    if (!disk->exists(current_path))
        return 0;

    for (auto iter = disk->iterateDirectory(current_path); iter->isValid(); iter->next())
    {
        if (disk->isFile(iter->path()))
        {
            if (loadSegmentFromFile(disk, iter->path(), partial_cache_name + iter->name()))
            {
                ++segment_from_disk;
            }
        }
        else if (disk->isDirectory(iter->path()))
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

bool DiskCacheLRU::loadSegmentFromFile(const DiskPtr & disk, const String & segment_rel_path, const String & segment_name)
{
    if (endsWith(segment_name, ".temp"))
    {
        // Only remove temp file if it didn't appear in cache, otherwise we may delete
        // caching entry
        if (!isExist(segment_name))
        {
            disk->removeFile(segment_rel_path);
        }
        return false;
    }
    else
    {
        return loadSegment(disk, segment_rel_path, segment_name, false);
    }
}

bool DiskCacheLRU::loadSegment(const DiskPtr & disk, const String & segment_rel_path, const String & segment_name, bool need_check_existence)
{
    if (need_check_existence && !disk->exists(segment_rel_path))
    {
        return false;
    }

    if (isExist(segment_name))
    {
        return false;
    }

    // Currently there won't be any data with old name format(which all cache stored in same directory)
    // So the code about old key format compatability is removed

    size_t file_size = disk->getFileSize(segment_rel_path);
    auto meta = std::make_shared<DiskCacheMeta>(DiskCacheState::Cached, file_size);
    meta->setDisk(disk);
    Base::set(segment_name, meta);

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

    auto remove_impl = [this, key, disk = value->getDisk(), &thread_pool]() {
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
        String cache_path = fs::path(base_path) / key;
        if (disk != nullptr)
        {
            disk->removeFileIfExists(cache_path);
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
