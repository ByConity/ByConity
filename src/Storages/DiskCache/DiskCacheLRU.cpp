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

#include <Storages/DiskCache/DiskCacheLRU.h>
#include <Storages/DiskCache/DiskCacheSimpleStrategy.h>
#include <fmt/core.h>
#include <sys/stat.h>
#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include "Common/Exception.h"
#include "Common/hex.h"
#include "common/logger_useful.h"
#include <Common/Throttler.h>
#include <Common/setThreadName.h>
#include "DataTypes/IDataType.h"
#include <IO/OpenedFileCache.h>
#include "Interpreters/Context.h"
#include "Storages/DiskCache/DiskCache_fwd.h"
#include "Storages/DiskCache/IDiskCache.h"
#include <common/errnoToString.h>
#include <Disks/IVolume.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>

namespace fs = std::filesystem;

namespace CurrentMetrics
{
    extern const Metric DiskCacheEvictQueueLength;
}

namespace ProfileEvents
{
    extern const Event DiskCacheGetMetaMicroSeconds;
    extern const Event DiskCacheGetTotalOps;
    extern const Event DiskCacheSetTotalOps;
    extern const Event DiskCacheSetTotalBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int SYSTEM_ERROR;
}

static constexpr auto DISK_CACHE_TEMP_FILE_SUFFIX = ".temp";
static constexpr auto TMP_SUFFIX_LEN = std::char_traits<char>::length(DISK_CACHE_TEMP_FILE_SUFFIX);
static constexpr auto META_DISK_CACHE_DIR_PREFIX = "meta";
static constexpr auto DATA_DISK_CACHE_DIR_PREFIX = "data";

namespace
{
    constexpr size_t HEX_KEY_LEN = sizeof(DiskCacheLRU::KeyType) * 2;

    UInt64 unhex16(const char * data)
    {
        UInt64 res = 0;
        for (size_t i = 0; i < sizeof(UInt64) * 2; ++i, ++data)
        {
            res <<= 4;
            res += static_cast<UInt64>(unhex(*data));
        }
        return res;
    }

    bool isHexKey(const String & hex_key)
    {
        if (hex_key.size() != HEX_KEY_LEN)
            return false;

        for (char c : hex_key)
        {
            if (!(isNumericASCII(c) || (c >= 'a' && c <= 'f')))
                return false;
        }

        return true;
    }
}

DiskCacheLRU::DiskCacheLRU(
    const String & name_,
    const VolumePtr & volume_,
    const ThrottlerPtr & throttler_,
    const DiskCacheSettings & settings_,
    const IDiskCacheStrategyPtr & strategy_,
    IDiskCache::DataType type_)
    : IDiskCache(name_, volume_, throttler_, settings_, strategy_, false, type_)
    , set_rate_throttler(settings_.cache_set_rate_limit == 0 ? nullptr : std::make_shared<Throttler>(settings_.cache_set_rate_limit))
    , set_throughput_throttler(settings_.cache_set_throughput_limit == 0 ? nullptr : std::make_shared<Throttler>(settings_.cache_set_throughput_limit))
    , containers(
        settings.cache_shard_num,
        BucketLRUCache<KeyType, DiskCacheMeta, DiskCacheWeight, DiskCacheWeightFunction>::Options {
            .lru_update_interval = static_cast<UInt32>(settings.lru_update_interval),
            .mapping_bucket_size = static_cast<UInt32>(std::max(1UL, settings.mapping_bucket_size / settings.cache_shard_num)),
            .max_weight = DiskCacheWeight({
                std::max(static_cast<size_t>(1),
                    type == IDiskCache::DataType::META ?
                        static_cast<size_t>(settings.lru_max_size * (settings.meta_cache_size_ratio * 1.0 / 100) / settings.cache_shard_num) :
                        static_cast<size_t>(settings.lru_max_size * ((100 - settings.meta_cache_size_ratio) * 1.0 / 100) / settings.cache_shard_num)),
                std::max(static_cast<size_t>(1),
                    type == IDiskCache::DataType::META ?
                        static_cast<size_t>(settings.lru_max_nums * (settings.meta_cache_nums_ratio * 1.0 / 100) / settings.cache_shard_num) :
                        static_cast<size_t>(settings.lru_max_nums * ((100 - settings.meta_cache_nums_ratio) * 1.0 / 100) / settings.cache_shard_num))
            }),
            .evict_handler = [](const KeyType& key, const DiskCacheMeta& meta, const DiskCacheWeight& weight) { return onEvictSegment(key, meta, weight); },
            .post_evict_callback = [this](
                const std::vector<std::pair<KeyType, std::shared_ptr<DiskCacheMeta>>> & removed_elements,
                const std::vector<std::pair<KeyType, std::shared_ptr<DiskCacheMeta>>> & updated_elements) {
                    afterEvictSegment(removed_elements, updated_elements);
                },
        })
{
    if (settings.cache_load_dispatcher_drill_down_level < -1)
    {
        throw Exception(fmt::format("Load dispatcher's drill down level {} invalid, "
            "must be positive or -1", settings.cache_load_dispatcher_drill_down_level),
            ErrorCodes::BAD_ARGUMENTS);
    }

    auto & thread_pool = IDiskCache::getThreadPool();
    thread_pool.scheduleOrThrowOnError([this] { load(); });
}

DiskCacheLRU::KeyType DiskCacheLRU::hash(const String & seg_key)
{
    size_t stream_name_pos = seg_key.find_last_of('/');
    if (stream_name_pos == std::string::npos)
        throw Exception("Invalid seg key: " + seg_key, ErrorCodes::LOGICAL_ERROR);

    stream_name_pos += 1;

    /// hash of stream name
    auto low = sipHash64(seg_key.data() + stream_name_pos, seg_key.size() - stream_name_pos);

    /// hash of (table_uuid + part_name)
    auto high = sipHash64(seg_key.data(), stream_name_pos - 1);

    return {high, low};
}

String DiskCacheLRU::hexKey(const KeyType & key)
{
    std::string res(HEX_KEY_LEN, '\0');
    /// Use little endian
    writeHexUIntLowercase(key, res.data());
    return res;
}

std::optional<DiskCacheLRU::KeyType> DiskCacheLRU::unhexKey(const String & hex_key)
{
    if (!isHexKey(hex_key))
        return {};

    auto low = unhex16(hex_key.data());
    auto high = unhex16(hex_key.data() + HEX_KEY_LEN / 2);

    return UInt128{high, low};
}

// disk_cache_v1/uuid_part_name[:3]/uuid_part_name/stream_name
// e.g. disk_cache_v1/752/752573bf0b591cd/de3e88c72ced6c3d
// files belongs to the same part will be placed under the same path
fs::path DiskCacheLRU::getPath(const DiskCacheLRU::KeyType & hash_key, const String & path, const String & seg_name, const String & prefix)
{
    String hex_key = hexKey(hash_key);
    std::string_view view(hex_key);
    std::string_view hex_key_low = view.substr(0, HEX_KEY_LEN / 2);
    std::string_view hex_key_high = view.substr(HEX_KEY_LEN / 2, HEX_KEY_LEN);
    if (!prefix.empty())
        return fs::path(path) / prefix / hex_key_high.substr(0, 3) / hex_key_high / hex_key_low;

    return fs::path(path) / (endsWith(seg_name, DATA_FILE_EXTENSION) ? DATA_DISK_CACHE_DIR_PREFIX : META_DISK_CACHE_DIR_PREFIX)
        / hex_key_high.substr(0, 3) / hex_key_high / hex_key_low;
}

static fs::path getRelativePathForPart(const String & part_name, const String & prefix)
{
    /// relative path for part
    auto hash = sipHash64(part_name.data(), part_name.size());
    String hex_key(HEX_KEY_LEN / 2, '\0');
    writeHexUIntLowercase(hash, hex_key.data());

    return fs::path(prefix) / hex_key.substr(0, 3) / hex_key / "";
}

void DiskCacheLRU::set(const String& seg_name, ReadBuffer& value, size_t weight_hint, bool is_preload)
{
    if (is_droping)
    {
        LOG_WARNING(log, fmt::format("skip write disk cache for droping disk cache is running"));
        return;
    }

    // Limit set rate to avoid too high write iops or lock contention
    if (set_rate_throttler)
    {
        set_rate_throttler->add(1);
    }

    ProfileEvents::increment(ProfileEvents::DiskCacheSetTotalOps, 1, Metrics::MetricType::Rate, {{"type", (is_preload ? "preload": "query")}});

    auto key = hash(seg_name);
    auto& shard = containers.shard(key);
    // Insert cache meta first, if there is a entry already there, skip this insert
    bool inserted = shard.emplace(key, std::make_shared<DiskCacheMeta>(
        DiskCacheMeta::State::Caching, nullptr, 0
    ));
    if (!inserted)
    {
        return;
    }

    ReservationPtr reserved_space = nullptr;
    try
    {
        // Avoid reserve as much as possible, since it will acquire a disk level
        // exclusive lock
        reserved_space = volume->reserve(weight_hint);
        if (reserved_space == nullptr) {
            throw Exception("Failed to reserve space", ErrorCodes::BAD_ARGUMENTS);
        }

        // Write data to local
        size_t weight = writeSegment(seg_name, value, reserved_space);
        ProfileEvents::increment(ProfileEvents::DiskCacheSetTotalBytes, weight, Metrics::MetricType::Rate, {{"type", (is_preload ? "preload": "query")}});

        // Update meta in lru cache, it must still there, since it should get evicted
        // since it have 0 weight
        shard.update(key, std::make_shared<DiskCacheMeta>(
            DiskCacheMeta::State::Cached, reserved_space->getDisk(), weight
        ));
    }
    catch(const Exception & e)
    {
        String local_disk_path = reserved_space == nullptr ? "" : reserved_space->getDisk()->getPath();
        tryLogCurrentException(log, fmt::format("Failed to key {} "
            "to local, disk path: {}, weight: {}, fail: {}", seg_name, local_disk_path, weight_hint, e.message()));
        shard.erase(key);
    }
}

std::pair<DiskPtr, String> DiskCacheLRU::get(const String & seg_name)
{
    ProfileEvents::increment(ProfileEvents::DiskCacheGetTotalOps);
    Stopwatch watch;
    SCOPE_EXIT({ProfileEvents::increment(ProfileEvents::DiskCacheGetMetaMicroSeconds,
        watch.elapsedMicroseconds());});

    auto key = hash(seg_name);
    auto& shard = containers.shard(key);
    std::shared_ptr<DiskCacheMeta> cache_meta = shard.get(key);
    if (cache_meta == nullptr || cache_meta->state != DiskCacheMeta::State::Cached)
    {
        return {};
    }

    if (unlikely(cache_meta->disk == nullptr))
    {
        // Disk is removed, erase it, should be rare
        shard.erase(key);
        return {};
    }

    return {cache_meta->disk, getRelativePath(key, seg_name)};
}

size_t DiskCacheLRU::writeSegment(const String& seg_key, ReadBuffer& buffer, ReservationPtr& reservation)
{
    DiskPtr disk = reservation->getDisk();
    String cache_rel_path = getRelativePath(hash(seg_key), seg_key);
    String temp_cache_rel_path = cache_rel_path + ".temp";

    try
    {
        // Create parent directory
        disk->createDirectories(fs::path(cache_rel_path).parent_path());

        // Write into temporary file, by default it will truncate this file
        size_t written_size = 0;
        {
            WriteBufferFromFile to(
                fs::path(disk->getPath()) / temp_cache_rel_path, DBMS_DEFAULT_BUFFER_SIZE, -1, 0666, nullptr, 0, set_throughput_throttler);
            copyData(buffer, to, reservation.get());
            to.finalize();
            written_size = to.count();
        }

        // Finish file writing, rename it, there shouldn't be any threads trying
        // to modify these files now
        disk->replaceFile(temp_cache_rel_path, cache_rel_path);

        if (disk->getFileSize(cache_rel_path) != written_size)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "cached {} file size {} doesn't match written size {}",
                cache_rel_path,
                disk->getFileSize(cache_rel_path),
                written_size);

        return written_size;
    }
    catch (...)
    {
        disk->removeFileIfExists(temp_cache_rel_path);
        disk->removeFileIfExists(cache_rel_path);
        throw;
    }
}

std::pair<bool, std::shared_ptr<DiskCacheMeta>> DiskCacheLRU::onEvictSegment(
    const KeyType&, const DiskCacheMeta& meta, const DiskCacheWeight&)
{
    if (meta.state != DiskCacheMeta::State::Cached)
    {
        return {false, nullptr};
    }

    return {false, std::make_shared<DiskCacheMeta>(DiskCacheMeta::State::Deleting,
        meta.disk, 0)};
}

// NOTE(wsy) This is called outside lru's lock, maybe we can remove evict thread pool
void DiskCacheLRU::afterEvictSegment(const std::vector<std::pair<KeyType, std::shared_ptr<DiskCacheMeta>>>&,
    const std::vector<std::pair<KeyType, std::shared_ptr<DiskCacheMeta>>>& updated_elements)
{
    if (shutdown_called)
        return;

    auto& thread_pool = IDiskCache::getEvictPool();
    for (const auto & updated_element : updated_elements)
    {
        DiskPtr disk = updated_element.second->disk;
        if (unlikely(disk == nullptr))
        {
            continue;
        }

        thread_pool.scheduleOrThrowOnError([this, key = updated_element.first, dsk = std::move(disk), &thread_pool]() {
            String prefix = META_DISK_CACHE_DIR_PREFIX;
            try
            {
                auto rel_meta_path = getRelativePath(key, "", META_DISK_CACHE_DIR_PREFIX);
                auto rel_data_path = getRelativePath(key, "", DATA_DISK_CACHE_DIR_PREFIX);
                if (dsk->exists(rel_meta_path))
                {
                    dsk->removeRecursive(rel_meta_path);
                    OpenedFileCache::instance().remove(rel_meta_path, -1);
                }
                else
                {
                    prefix = DATA_DISK_CACHE_DIR_PREFIX;
                    dsk->removeRecursive(rel_data_path);
                    OpenedFileCache::instance().remove(rel_data_path, -1);
                }

                // Since we are holding locks of lru when calling onEvictSegment,
                // this erase must happen after state is update to Deleting
                auto& shard = containers.shard(key);
                shard.erase(key);
            }
            catch (...)
            {
                tryLogCurrentException(log, fmt::format("Failed to remove cache {}, "
                    "disk path {}", String(getRelativePath(key, "", prefix)), dsk->getPath()));
            }

            /// remove parent path if it's empty
            try
            {
                auto rel_path = getRelativePath(key, "", prefix);
                if (dsk->isDirectoryEmpty(rel_path.parent_path()))
                {
                    dsk->removeDirectory(rel_path.parent_path());
                }
            }
            catch (...)
            {
            }

            CurrentMetrics::set(CurrentMetrics::DiskCacheEvictQueueLength, thread_pool.active() - 1);
        });
    }
}

void DiskCacheLRU::load()
{
    Stopwatch watch;
    SCOPE_EXIT({ LOG_INFO(log, fmt::format("load thread takes {} ms", watch.elapsedMilliseconds())); });
    Disks disks = volume->getDisks();
    if (settings.cache_dispatcher_per_disk)
    {
        ThreadPool dispatcher_pool(disks.size());
        ExceptionHandler except_handler;
        for (const DiskPtr & disk : disks)
        {
            dispatcher_pool.scheduleOrThrowOnError(createExceptionHandledJob(
                [this, disk] {
                    auto loader = std::make_unique<DiskCacheLoader>(
                        *this, disk, settings.cache_loader_per_disk, 1, 1);
                    if (type == DataType::ALL)
                        loader->exec(fs::path(latest_disk_cache_dir) / "");
                    else if (type == DataType::META)
                        loader->exec(fs::path(latest_disk_cache_dir) / META_DISK_CACHE_DIR_PREFIX);
                    else if (type == DataType::DATA)
                        loader->exec(fs::path(latest_disk_cache_dir) / DATA_DISK_CACHE_DIR_PREFIX);
                },
                except_handler));

            if (!previous_disk_cache_dirs.empty())
            {
                dispatcher_pool.scheduleOrThrowOnError(createExceptionHandledJob(
                    [this, disk] {
                        auto migrator = std::make_unique<DiskCacheMigrator>(*this, disk, settings.cache_loader_per_disk, 1, 1);
                        for (const auto & path : previous_disk_cache_dirs)
                        {
                            LOG_TRACE(log, "Start migrate cache from {} to {}", disk->getPath() + path, disk->getPath() + latest_disk_cache_dir);
                            migrator->exec(fs::path(path) / "");
                        }

                    },
                    except_handler));
            }
        }
        dispatcher_pool.wait();
        except_handler.throwIfException();
    }
    else
    {
        setThreadName("DCDispatcher");

        for (const DiskPtr & disk : disks)
        {
            auto loader = std::make_unique<DiskCacheLoader>(*this, disk, settings.cache_loader_per_disk, 1, 1);
            if (type == DataType::ALL)
                loader->exec(fs::path(latest_disk_cache_dir) / "");
            else if (type == DataType::META)
                loader->exec(fs::path(latest_disk_cache_dir) / META_DISK_CACHE_DIR_PREFIX);
            else if (type == DataType::DATA)
                loader->exec(fs::path(latest_disk_cache_dir) / DATA_DISK_CACHE_DIR_PREFIX);
        }

        if (!previous_disk_cache_dirs.empty())
        {
            for (const DiskPtr & disk : disks)
            {
                auto migrator = std::make_unique<DiskCacheMigrator>(*this, disk, settings.cache_loader_per_disk, 1, 1);
                for (const auto & path : previous_disk_cache_dirs)
                {
                    LOG_TRACE(log, "Start migrate cache from {} to {}", disk->getPath() + path, disk->getPath() + latest_disk_cache_dir);
                     migrator->exec(fs::path(path) / "");
                }
            }
        }
    }
}

size_t DiskCacheLRU::drop(const String & part_name)
{
    is_droping = true;
    SCOPE_EXIT({is_droping = false; });

    fs::path meta_path, data_path;

    if (name == "Manifest")
    {
        meta_path = getRelativePath(hash(part_name), part_name).parent_path();
        data_path = meta_path;

        LOG_TRACE(log, fmt::format("start delete manifest {} cache {} {}", part_name, meta_path.relative_path().c_str(), data_path.relative_path().c_str()));
    }
    else
    {
        if (type == DataType::ALL || type == DataType::META)
            meta_path = part_name.empty() ? fs::path(latest_disk_cache_dir) / META_DISK_CACHE_DIR_PREFIX : fs::path(latest_disk_cache_dir) /  getRelativePathForPart(part_name, META_DISK_CACHE_DIR_PREFIX);

        if (type == DataType::ALL || type == DataType::DATA)
            data_path = part_name.empty() ? fs::path(latest_disk_cache_dir) / DATA_DISK_CACHE_DIR_PREFIX : fs::path(latest_disk_cache_dir) /  getRelativePathForPart(part_name, DATA_DISK_CACHE_DIR_PREFIX);

        LOG_TRACE(log, fmt::format("start delete part {} cache {} {}", part_name, meta_path.relative_path().c_str(), data_path.relative_path().c_str()));
    }

    const Disks & disks = volume->getDisks();
    size_t delete_file_size = 0;
    for (const auto & disk : disks)
    {
        if (!meta_path.relative_path().empty() && disk->exists(meta_path))
        {
            DiskCacheDeleter deleter(*this, disk, 1, -1, -1);
            deleter.exec(meta_path);
            delete_file_size += deleter.delete_file_size;
        }

        if (!data_path.relative_path().empty() && disk->exists(data_path))
        {
            DiskCacheDeleter deleter(*this, disk, 1, -1, -1);
            deleter.exec(data_path);
            delete_file_size += deleter.delete_file_size;
        }
    }

    LOG_DEBUG(log, fmt::format("deleted {} cache size {} {} {}", part_name, delete_file_size,meta_path.relative_path().c_str(), data_path.relative_path().c_str()));
    return delete_file_size;
}

DiskCacheLRU::DiskIterator::DiskIterator(
    const String & name_, DiskCacheLRU & cache_, DiskPtr disk_, size_t worker_per_disk_, int min_depth_parallel_, int max_depth_parallel_)
    : name(name_)
    , disk_cache(cache_)
    , disk(std::move(disk_))
    , worker_per_disk(worker_per_disk_)
    , min_depth_parallel(min_depth_parallel_)
    , max_depth_parallel(max_depth_parallel_)
    , log(::getLogger(fmt::format("DiskIterator{}({})", name, disk_cache.getName())))
{
}

void DiskCacheLRU::DiskIterator::exec(fs::path entry_path)
{
    String full_path = fs::path(disk->getPath()) / entry_path;
    struct stat obj_stat;
    if (stat(full_path.c_str(), &obj_stat) != 0)
        return;  // not exists

    if (!S_ISDIR(obj_stat.st_mode))
        throw Exception(fmt::format("disk path {} {} is not a directory", disk->getPath(), entry_path.string()), ErrorCodes::LOGICAL_ERROR);

    pool = worker_per_disk <= 1 ? nullptr : std::make_unique<ThreadPool>(worker_per_disk);
    iterateDirectory(entry_path / "", 0);
    if (pool)
        pool->wait();
    handler.throwIfException();
}

void DiskCacheLRU::DiskIterator::iterateDirectory(fs::path rel_path, size_t depth)
{
    auto iterate_dir_impl = [this, current_depth = depth] (fs::path current_rel_path) {
        for (auto iter = disk->iterateDirectory(current_rel_path); iter->isValid(); iter->next())
        {
            try
            {
                String abs_obj_path = fs::path(disk->getPath()) / current_rel_path / iter->name();
                struct stat obj_stat;
                if (stat(abs_obj_path.c_str(), &obj_stat) != 0)
                {
                    LOG_WARNING(log, fmt::format("Failed to stat file {}, error {}", abs_obj_path, errnoToString(errno)));
                    continue;
                }

                if (S_ISREG(obj_stat.st_mode))
                {
                    iterateFile(current_rel_path / iter->name(), obj_stat.st_size);
                }
                else if (S_ISDIR(obj_stat.st_mode))
                {
                    iterateDirectory(current_rel_path / iter->name() / "", current_depth + 1);
                }
            }
            catch (...)
            {
                tryLogCurrentException(log);
            }
        }

        // if (disk->isDirectoryEmpty(current_rel_path))
        //     disk->remove(current_rel_path); // rmdir
    };

    if (pool && static_cast<int>(depth) >= min_depth_parallel && static_cast<int>(depth) <= max_depth_parallel)
    {
        pool->scheduleOrThrowOnError(createExceptionHandledJob(
            [iterate_dir_impl, rel_path] {
                setThreadName("DCWorker");
                iterate_dir_impl(std::move(rel_path));
            },
            handler));
    }
    else
    {
        try
        {
            iterate_dir_impl(std::move(rel_path));
        }
        catch (...)
        {
            handler.setException(std::current_exception());
        }
    }
}

DiskCacheLRU::DiskCacheLoader::DiskCacheLoader(
    DiskCacheLRU & cache_, DiskPtr disk_, size_t worker_per_disk_, int min_depth_parallel_, int max_depth_parallel_)
    : DiskIterator("Loader", cache_, std::move(disk_), worker_per_disk_, min_depth_parallel_, max_depth_parallel_)
{
}

DiskCacheLRU::DiskCacheLoader::~DiskCacheLoader()
{
    try
    {
        LOG_INFO(log, fmt::format("Loaded {} segs from disk {}", total_loaded, disk->getPath()));
    }
    catch (...) {}
}

void DiskCacheLRU::DiskCacheLoader::iterateFile(fs::path file_path, size_t file_size)
{
    bool is_temp_segment = endsWith(file_path, DISK_CACHE_TEMP_FILE_SUFFIX);
    String segment_hex = file_path.filename().string();

    /// remove temp prefix
    if (is_temp_segment)
    {
        segment_hex.resize(segment_hex.size() - TMP_SUFFIX_LEN);
    }

    /// Use little endian
    String hex_key = segment_hex + file_path.parent_path().filename().string();
    auto unhexed = unhexKey(hex_key);
    if (!unhexed)
    {
        LOG_ERROR(log, "Invalid disk cache file path {} with hex key {}", (fs::path(disk->getPath()) / file_path).string(), hex_key);
        disk->removeFileIfExists(file_path);
        return;
    }

    const KeyType & key = unhexed.value();

    if (is_temp_segment)
    {
        std::shared_ptr<DiskCacheMeta> meta = nullptr;
        {
            ProfileEvents::increment(ProfileEvents::DiskCacheGetTotalOps);
            Stopwatch watch;
            SCOPE_EXIT({ProfileEvents::increment(ProfileEvents::DiskCacheGetMetaMicroSeconds,
                watch.elapsedMicroseconds());});

            meta = disk_cache.containers.shard(key).get(key);
        }

        if (meta == nullptr)
        {
            // There is a temporary cache file and no corresponding entry in disk cache
            try
            {
                disk->removeFileIfExists(file_path);
            }
            catch(...)
            {
                tryLogCurrentException(log, fmt::format("Failed to remove temporary "
                    "cache file {}, disk path: {}, should be rare", file_path.string(),
                    disk->getPath()));
            }
        }
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::DiskCacheSetTotalOps);
        if (disk_cache.containers.shard(key).emplace(key, std::make_shared<DiskCacheMeta>(DiskCacheMeta::State::Cached, disk, file_size)))
            ++total_loaded;
    }
}

DiskCacheLRU::DiskCacheMigrator::DiskCacheMigrator(
    DiskCacheLRU & cache_, DiskPtr disk_, size_t worker_per_disk_, int min_depth_parallel_, int max_depth_parallel_)
    : DiskIterator("Migrator", cache_, std::move(disk_), worker_per_disk_, min_depth_parallel_, max_depth_parallel_)
{
}

DiskCacheLRU::DiskCacheMigrator::~DiskCacheMigrator()
{
    try
    {
        LOG_INFO(log, fmt::format("Migrated {} segs from disk {}", total_migrated, disk->getPath()));
    }
    catch (...) {}
}

void DiskCacheLRU::DiskCacheMigrator::iterateFile(fs::path rel_path, size_t file_size)
{
    if (endsWith(rel_path, DISK_CACHE_TEMP_FILE_SUFFIX))
    {
        disk->removeFileIfExists(rel_path);
        return;
    }

    String full_path(rel_path.relative_path().c_str());
    String entry_path = full_path.substr(full_path.find_first_of('/'), full_path.size() - 1);

    String segment_hex = rel_path.filename().string();
    /// Use little endian
    String hex_key = segment_hex + rel_path.parent_path().filename().string();
    auto unhexed = unhexKey(hex_key);
    if (!unhexed)
    {
        LOG_ERROR(log, "Invalid disk cache file path: {}, hex_key: {}", (fs::path(disk->getPath()) / rel_path).c_str(), hex_key);
        disk->removeFileIfExists(rel_path);
        return;
    }

    const KeyType & key = unhexed.value();
    auto& shard = disk_cache.containers.shard(key);
    // Insert cache meta first, if there is a entry already there, skip this insert

    auto target_path = fs::path(disk_cache.getDataDir()) / fmt::format("{}{}", DATA_DISK_CACHE_DIR_PREFIX, entry_path);
    try
    {
        bool inserted = shard.emplace(key, std::make_shared<DiskCacheMeta>(DiskCacheMeta::State::Caching, nullptr, 0));
        if (inserted)
        {
            disk->createDirectories(target_path.parent_path());
            disk->moveFile(rel_path, target_path);
            shard.update(key, std::make_shared<DiskCacheMeta>(DiskCacheMeta::State::Cached, disk, file_size));
            ++total_migrated;
        }
        else
        {
            disk->removeFileIfExists(rel_path);
        }
    }
    catch (...)
    {
        disk->removeFileIfExists(rel_path);
        // disk->removeIfExists(target_path);
        shard.erase(key);
        throw;
    }
}

DiskCacheLRU::DiskCacheDeleter::DiskCacheDeleter(
    DiskCacheLRU & cache_, DiskPtr disk_, size_t worker_per_disk_, int min_depth_parallel_, int max_depth_parallel_)
    : DiskIterator("Deleter", cache_, std::move(disk_), worker_per_disk_, min_depth_parallel_, max_depth_parallel_)
{
}

DiskCacheLRU::DiskCacheDeleter::~DiskCacheDeleter() = default;

void DiskCacheLRU::DiskCacheDeleter::exec(std::filesystem::path entry_path)
{
    String full_path = fs::path(disk->getPath()) / entry_path;
    struct stat obj_stat;
    if (stat(full_path.c_str(), &obj_stat) != 0)
        return; // not exists

    if (!S_ISDIR(obj_stat.st_mode))
        throw Exception(fmt::format("disk path {} {} is not a directory", disk->getPath(), entry_path.string()), ErrorCodes::LOGICAL_ERROR);

    iterateDirectory(entry_path / "", 0);

    LOG_DEBUG(log, "remove part disk cache path {} {}", disk->getPath().c_str(), entry_path.c_str());
    disk->removeRecursive(entry_path);
}

void DiskCacheLRU::DiskCacheDeleter::iterateFile(std::filesystem::path file_path, size_t file_size)
{
    /// logic is similar to DiskCacheLoader
    bool is_temp_segment = endsWith(file_path, DISK_CACHE_TEMP_FILE_SUFFIX);
    String segment_hex = file_path.filename().string();

    /// remove temp prefix
    if (is_temp_segment)
    {
        segment_hex.resize(segment_hex.size() - TMP_SUFFIX_LEN);
    }

    String hex_key = segment_hex + file_path.parent_path().filename().string();
    auto unhexed = unhexKey(hex_key);
    if (!unhexed)
    {
        LOG_ERROR(log, "Invalid disk cache file path: {}, hex_key : {}", (fs::path(disk->getPath()) / file_path).c_str(), hex_key);
        disk->removeFileIfExists(file_path);
        return;
    }

    const KeyType & key = unhexed.value();

    /// directly erase the key, to prevent setImpl setting the key again
    disk_cache.containers.shard(key).erase(key);
    delete_file_size += file_size;
}

}
