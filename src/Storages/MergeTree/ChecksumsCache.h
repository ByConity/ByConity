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
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/ProfileEvents.h>
#include <sstream>

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

struct ChecksumsWeightFunction
{
    size_t operator()(const MergeTreeDataPartChecksums & checksums) const
    {
        constexpr size_t kApproximatelyBytesPerElement = 128;
        return checksums.files.size() * kApproximatelyBytesPerElement;
    }
};

struct ChecksumsCacheSettings
{
    size_t lru_max_size {std::numeric_limits<size_t>::max()};

    size_t lru_max_nums {std::numeric_limits<size_t>::max()};

    // Cache mapping bucket size
    size_t mapping_bucket_size {5000};

    // LRU queue update interval in seconds
    size_t lru_update_interval {60};

    size_t cache_shard_num {8};
};


class ChecksumsCache
{
public:
    explicit ChecksumsCache(const ChecksumsCacheSettings & settings)
    : cache_shard_num(settings.cache_shard_num),
      first_level_containers(new std::unordered_map<String, std::set<ChecksumsName>>[settings.cache_shard_num]), 
      first_level_containers_locks(new std::mutex[settings.cache_shard_num]),
      second_level_cache(
          settings.cache_shard_num,
          BucketLRUCache<ChecksumsName, MergeTreeDataPartChecksums, std::hash<ChecksumsName>, ChecksumsWeightFunction>::Options{
              .lru_update_interval = static_cast<UInt32>(settings.lru_update_interval),
              .mapping_bucket_size = static_cast<UInt32>(std::max(1UL, settings.mapping_bucket_size / settings.cache_shard_num)),
              .max_size = std::max(static_cast<size_t>(1), static_cast<size_t>(settings.lru_max_size / settings.cache_shard_num)),
              .max_nums = std::max( static_cast<size_t>(1), static_cast<size_t>(settings.lru_max_nums / settings.cache_shard_num)),
              .enable_customize_evict_handler = false,
            })
    {
    }

    template <typename LoadFunc>
    std::pair<std::shared_ptr<MergeTreeDataPartChecksums>, bool> getOrSet(const String & name, const ChecksumsName& key, LoadFunc && load)
    {
        bool flag = false;

        auto& shard = second_level_cache.shard(key);
        std::shared_ptr<MergeTreeDataPartChecksums> result = shard.get(key);
        if (!result)
        {
            ProfileEvents::increment(ProfileEvents::ChecksumsCacheMisses);
            result = load();
            shard.upsert(key, result);
            flag = true;
            
            size_t first_level_bucket = std::hash<String>()(name) % cache_shard_num;
            std::unique_lock<std::mutex> guard(first_level_containers_locks[first_level_bucket]);
            first_level_containers[first_level_bucket][name].insert(key);
        }
        else
        {
            ProfileEvents::increment(ProfileEvents::ChecksumsCacheHits);
            flag = false;
        }
        
        return std::make_pair(result, flag);
    }

    size_t count() const
    {
        return second_level_cache.count();
    }

    size_t weight() const
    {
        return second_level_cache.weight();
    }

    void dropChecksumCache(const String & name, bool with_lock = false)
    {
        size_t first_level_bucket = std::hash<String>()(name) % cache_shard_num;

        std::unique_lock<std::mutex> guard; 
        if (!with_lock)
        {
            guard = std::unique_lock(first_level_containers_locks[first_level_bucket]);
        }

        auto & keys = first_level_containers[first_level_bucket][name];
        for (const auto & key: keys) 
        {
            auto & shard = second_level_cache.shard(key);
            shard.erase(key);
        }

        first_level_containers[first_level_bucket][name].erase(name);
    }

    void reset()
    {
        for (size_t i = 0; i < cache_shard_num; i++) 
        {
            std::unique_lock<std::mutex> guard(first_level_containers_locks[i]);
            auto iter = first_level_containers[i].begin();
            while (iter != first_level_containers[i].end())
            {
                auto name = iter->first;
                iter++;
                dropChecksumCache(name, true);
            }
        }
    }

private:
    size_t cache_shard_num;
    std::unique_ptr<std::unordered_map<String, std::set<ChecksumsName>>[]> first_level_containers;
    std::unique_ptr<std::mutex[]> first_level_containers_locks;

    ShardCache<ChecksumsName, std::hash<ChecksumsName>, 
        BucketLRUCache<ChecksumsName, MergeTreeDataPartChecksums, std::hash<ChecksumsName>, ChecksumsWeightFunction>> second_level_cache;
};

using ChecksumsCachePtr = std::shared_ptr<ChecksumsCache>;

}
