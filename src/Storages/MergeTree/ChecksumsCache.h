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

#include <Common/BucketLRUCache.h>
#include <Common/ShardCache.h>
#include <Storages/DiskCache/NvmCache.h>
#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/HashKey.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/ProfileEvents.h>
#include <common/find_symbols.h>
#include <cstddef>
#include <limits>
#include <sstream>
#include <unordered_map>

namespace ProfileEvents
{
    extern const Event ChecksumsCacheHits;
    extern const Event ChecksumsCacheMisses;
}

namespace DB
{

using ChecksumsName = std::string;

inline String getChecksumsCacheKey(const String & storage_unique_id, const IMergeTreeDataPart & part)
{
    /// for those table without uuid, directly use storage object address as unique key for checksum cache.
    return storage_unique_id + "_" + part.getUniquePartName();
}

inline String getStorageUniqueId(const String & checksums_cache_key)
{
    Strings items;
    splitInto<'_'>(items, checksums_cache_key);
    return items.empty() ? "" : items.front();
}

struct ChecksumsWeightFunction
{
    /// We spent additional bytes on key in hashmap, linked lists, shared pointers, etc ...
    static constexpr size_t CHECKSUM_CACHE_OVERHEAD = 128;

    DimensionBucketLRUWeight<2> operator()(const MergeTreeDataPartChecksums& cache_item) const
    {
        // * 1.5 means MergeTreeDataPartChecksums class overhead
        constexpr size_t k_approximately_bytes_per_element = 128 * 1.5;
        return {(cache_item.files.size() * k_approximately_bytes_per_element) + CHECKSUM_CACHE_OVERHEAD, 1};
    }
};

struct ChecksumsCacheSettings
{
    size_t lru_max_size {5368709120};

    // Cache mapping bucket size
    size_t mapping_bucket_size {5000};

    // LRU queue update interval in seconds
    size_t lru_update_interval {60};

    size_t cache_shard_num {8};
};

class ChecksumsCacheShard: private BucketLRUCache<ChecksumsName, MergeTreeDataPartChecksums, DimensionBucketLRUWeight<2>, ChecksumsWeightFunction>
{
public:
    using Base = BucketLRUCache<ChecksumsName, MergeTreeDataPartChecksums, DimensionBucketLRUWeight<2>, ChecksumsWeightFunction>;

    explicit ChecksumsCacheShard(const ChecksumsCacheSettings & settings):
        Base(Base::Options {
                .lru_update_interval = static_cast<UInt32>(settings.lru_update_interval),
                .mapping_bucket_size = static_cast<UInt32>(settings.mapping_bucket_size),
                .max_weight = DimensionBucketLRUWeight<2>({
                    std::max(static_cast<size_t>(1), settings.lru_max_size),
                    std::numeric_limits<size_t>::max()
                }),
                .evict_handler = [this](const ChecksumsName& checksum_name, const MergeTreeDataPartChecksums&, const DimensionBucketLRUWeight<2>&) {
                    removeMapping(checksum_name);
                    return std::make_pair(true, std::shared_ptr<MergeTreeDataPartChecksums>(nullptr));
                },
                .insert_callback = [this](const ChecksumsName& checksum_name) {
                    addMapping(checksum_name);
                }
            })
    {
    }

    template <typename LoadFunc>
    std::pair<std::shared_ptr<MergeTreeDataPartChecksums>, bool> getOrSet(
        const String&, const ChecksumsName& key, LoadFunc&& load)
    {
        bool loaded = false;
        std::shared_ptr<MergeTreeDataPartChecksums> part_checksums =
            Base::getOrSet(key, [&load, &loaded]() {
                loaded = true;
                return load();
            });
        ProfileEvents::increment(loaded ? ProfileEvents::ChecksumsCacheMisses : ProfileEvents::ChecksumsCacheHits);
        return std::make_pair(part_checksums, loaded);
    }

    void dropChecksumCache(const String& name)
    {
        std::lock_guard lock(Base::mutex);

        if (auto iter = table_to_checksums_name.find(name);
            iter != table_to_checksums_name.end())
        {
            for (const auto& checksum_name : *(iter->second))
            {
                eraseWithoutLock(checksum_name);
            }

            table_to_checksums_name.erase(iter);
        }
    }

    size_t weight() const
    {
        return Base::weight()[0];
    }

    size_t count() const
    {
        return Base::weight()[1];
    }

    void reset()
    {
        std::lock_guard lock(Base::mutex);

        for (const auto& entry : table_to_checksums_name)
        {
            for (const auto& checksum_name : *(entry.second))
            {
                eraseWithoutLock(checksum_name);
            }
        }
        table_to_checksums_name.clear();
    }

private:
    void addMapping(const ChecksumsName& key)
    {
        String storage_id = getStorageUniqueId(key);
        std::unique_ptr<std::unordered_set<ChecksumsName>>& checksums_set =
            table_to_checksums_name[storage_id];
        if (checksums_set == nullptr)
        {
            checksums_set = std::make_unique<std::unordered_set<ChecksumsName>>();
        }
        checksums_set->insert(key);
    }

    void removeMapping(const ChecksumsName& key)
    {
        String storage_id = getStorageUniqueId(key);
        if (auto iter = table_to_checksums_name.find(storage_id);
            iter != table_to_checksums_name.end())
        {
            iter->second->erase(key);

            if (iter->second->empty())
            {
                table_to_checksums_name.erase(iter);
            }
        }
    }

    std::unordered_map<String, std::unique_ptr<std::unordered_set<ChecksumsName>>> table_to_checksums_name;
};

class ChecksumsCache
{
public:
    explicit ChecksumsCache(const ChecksumsCacheSettings& settings_)
    {
        for (size_t i = 0; i < settings_.cache_shard_num; ++i)
        {
            shards.emplace_back(std::make_unique<ChecksumsCacheShard>(settings_));
        }
    }

    template <typename LoadFunc>
    std::pair<std::shared_ptr<MergeTreeDataPartChecksums>, bool> getOrSet(
        const String& name, const ChecksumsName& key, LoadFunc&& load)
    {
        return shard(name).getOrSet(name, key, load);
    }

    void dropChecksumCache(const String& name)
    {
        shard(name).dropChecksumCache(name);
    }

    size_t weight() const
    {
        size_t sum = 0;
        for (const auto& shard : shards)
        {
            sum += shard->weight();
        }
        return sum;
    }

    size_t count() const
    {
        size_t sum = 0;
        for (const auto& shard : shards)
        {
            sum += shard->count();
        }
        return sum;
    }

    void reset()
    {
        for (auto& shard : shards)
        {
            shard->reset();
        }
    }

private:
    ChecksumsCacheShard& shard(const String& name)
    {
        return *shards[hasher(name) % shards.size()];
    }

    std::hash<String> hasher;
    std::vector<std::unique_ptr<ChecksumsCacheShard>> shards;
};

using ChecksumsCachePtr = std::shared_ptr<ChecksumsCache>;

}
