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
#include <atomic>
#include <filesystem>
#include <Common/HashTable/Hash.h>
#include <Storages/DiskCache/IDiskCache.h>
#include <Common/BucketLRUCache.h>
#include <Common/ShardCache.h>
#include <sys/types.h>
#include <Poco/Logger.h>

namespace DB
{

class DiskCacheMeta
{
public:
    enum class State
    {
        Caching,
        Cached,
        Deleting,
    };

    DiskCacheMeta(State state_, const DiskPtr & disk_, size_t size_):
        state(state_), disk(disk_), size(size_) {}

    State state;
    DiskPtr disk;
    size_t size;
};

/// First value is cache size, second value is cache num
using DiskCacheWeight = DimensionBucketLRUWeight<2>;

struct DiskCacheWeightFunction
{
    DiskCacheWeight operator()(const DiskCacheMeta& meta) const
    {
        if (meta.state == DiskCacheMeta::State::Cached)
        {
            return DiskCacheWeight({meta.size, 1});
        }
        return DiskCacheWeight({0, 1});
    }
};

//            ┌───────┐            ┌──────┐        ┌────────┐               ┌────┐
// CacheState │Caching├───────────►│Cached├───────►│Deleting├──────────────►│Gone│
//            └──┬────┘            └──────┘        └───┬────┘               └────┘
//               │                     ▲               │                       ▲
// FileState     └───►Temp────►Final───┘               └───►Final────►Deleted──┘
class DiskCacheLRU: public IDiskCache
{
public:
    using KeyType = UInt128;

    DiskCacheLRU(
        const String & name_,
        const VolumePtr & volume,
        const ThrottlerPtr & throttler,
        const DiskCacheSettings & settings,
        const IDiskCacheStrategyPtr & strategy_,
        IDiskCache::DataType type_ = IDiskCache::DataType::ALL);

    void set(const String& seg_name, ReadBuffer& value, size_t weight_hint, bool is_preload) override;
    std::pair<DiskPtr, String> get(const String& seg_name) override;
    void load() override;
    size_t drop(const String & part_name) override;

    size_t getKeyCount() const override { return containers.weight()[1]; }
    size_t getCachedSize() const override { return containers.weight()[0]; }
    std::filesystem::path getRelativePath(const KeyType & key, const String & seg_name, const String & prefix = {}) { return getPath(key, latest_disk_cache_dir, seg_name, prefix);}

    static std::filesystem::path getPath(const KeyType & key, const String & path, const String & seg_name, const String & prefix);

    /// for test
    static KeyType hash(const String & seg_key);
    static String hexKey(const KeyType & key);
    static std::optional<KeyType> unhexKey(const String & hex);

private:
    static std::pair<bool, std::shared_ptr<DiskCacheMeta>> onEvictSegment(const KeyType&,
        const DiskCacheMeta& meta, const DiskCacheWeight&);

    size_t writeSegment(const String& seg_key, ReadBuffer& buffer, ReservationPtr& reservation);
    void afterEvictSegment(const std::vector<std::pair<KeyType, std::shared_ptr<DiskCacheMeta>>>&,
        const std::vector<std::pair<KeyType, std::shared_ptr<DiskCacheMeta>>>& updated_elements);

    struct DiskIterator : private boost::noncopyable
    {
        explicit DiskIterator(
            const String & name_, DiskCacheLRU & cache_, DiskPtr disk_, size_t worker_per_disk_, int min_depth_parallel_, int max_depth_parallel_);
        virtual ~DiskIterator() = default;

        virtual void exec(std::filesystem::path entry_path);
        virtual void iterateDirectory(std::filesystem::path rel_path, size_t depth);
        virtual void iterateFile(std::filesystem::path rel_path, size_t file_size) = 0;

        String name;
        // lifecycle shorter than disk cache
        DiskCacheLRU & disk_cache;
        DiskPtr disk;

        /// worker_per_disk > 1 to enable parallel iterate
        size_t worker_per_disk{1};

        /// min iterate depth to use thread pool
        int min_depth_parallel{-1};

        /// max iterate depth to use thread pool
        int max_depth_parallel{-1};

        std::unique_ptr<ThreadPool> pool;
        ExceptionHandler handler;
        LoggerPtr log;
    };

    /// Load from disk when Disk cache starts up
    struct DiskCacheLoader : DiskIterator
    {
        explicit DiskCacheLoader(
            DiskCacheLRU & cache_, DiskPtr disk_, size_t worker_per_disk, int min_depth_parallel, int max_depth_parallel);
        ~DiskCacheLoader() override;
        void iterateFile(std::filesystem::path file_path, size_t file_size) override;

        std::atomic_size_t total_loaded = 0;
    };

    /// Migrate from old format to new format
    struct DiskCacheMigrator : DiskIterator
    {
        explicit DiskCacheMigrator(
            DiskCacheLRU & cache_, DiskPtr disk_, size_t worker_per_disk, int min_depth_parallel, int max_depth_parallel);
        ~DiskCacheMigrator() override;
        void iterateFile(std::filesystem::path file_path, size_t file_size) override;

        std::atomic_size_t total_migrated = 0;
    };

    struct DiskCacheDeleter : DiskIterator
    {
        explicit DiskCacheDeleter(
            DiskCacheLRU & cache_, DiskPtr disk_, size_t worker_per_disk, int min_depth_parallel, int max_depth_parallel);
        ~DiskCacheDeleter() override;
        void exec(std::filesystem::path entry_path) override;
        void iterateFile(std::filesystem::path file_path, size_t file_size) override;

        size_t delete_file_size {0};
    };

    ThrottlerPtr set_rate_throttler;
    ThrottlerPtr set_throughput_throttler;
    std::atomic<bool> is_droping{false};
    ShardCache<KeyType, UInt128Hash, BucketLRUCache<KeyType, DiskCacheMeta, DiskCacheWeight, DiskCacheWeightFunction>> containers;
};

}
