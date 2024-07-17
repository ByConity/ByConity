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
#include <sstream>

namespace ProfileEvents
{
    extern const Event ChecksumsCacheHits;
    extern const Event ChecksumsCacheMisses;
}

namespace DB
{


using ChecksumsName = std::string;

enum class ChecksumsCacheState
{
    Caching,
    Cached,
    Deleting,
};
using ChecksumsCacheItem = std::pair<ChecksumsCacheState, std::shared_ptr<MergeTreeDataPartChecksums>>;

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

    size_t operator()(const ChecksumsCacheItem & cache_item) const
    {
        const auto & checksums = cache_item.second;
        if (!checksums)
            return 0;
        
        // * 1.5 means MergeTreeDataPartChecksums class overhead
        constexpr size_t kApproximatelyBytesPerElement = 128 * 1.5;
        return (checksums->files.size() * kApproximatelyBytesPerElement) + CHECKSUM_CACHE_OVERHEAD;
    }
};

struct ChecksumsCacheSettings
{
    size_t lru_max_size {std::numeric_limits<size_t>::max()};

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
    : cache_shard_num(settings.cache_shard_num)
    , table_to_parts_cache(new std::unordered_map<String, std::set<ChecksumsName>>[settings.cache_shard_num])
    , table_to_parts_cache_locks(new std::mutex[settings.cache_shard_num])
    , parts_to_checksums_cache(
          settings.cache_shard_num,
          BucketLRUCache<ChecksumsName, ChecksumsCacheItem, std::hash<ChecksumsName>, ChecksumsWeightFunction>::Options{
              .lru_update_interval = static_cast<UInt32>(settings.lru_update_interval),
              .mapping_bucket_size = static_cast<UInt32>(std::max(1UL, settings.mapping_bucket_size / settings.cache_shard_num)),
              .max_size = std::max(static_cast<size_t>(1), static_cast<size_t>(settings.lru_max_size / settings.cache_shard_num)),
              .max_nums = std::numeric_limits<size_t>::max(),
              .enable_customize_evict_handler = true,
              .customize_evict_handler
              = [this](
                    const ChecksumsName& key, const std::shared_ptr<ChecksumsCacheItem>& item, size_t sz) {
                    return onEvictChecksums(key, item, sz);
                },
              .customize_post_evict_handler =
                  [this](
                      const std::vector<std::pair<ChecksumsName, std::shared_ptr<ChecksumsCacheItem>>> & removed_elements,
                      const std::vector<std::pair<ChecksumsName, std::shared_ptr<ChecksumsCacheItem>>> & updated_elements) {
                    afterEvictChecksums(removed_elements, updated_elements);
                  },
            })
    , logger(&Poco::Logger::get("ChecksumsCache"))
    {
    }

    template <typename LoadFunc>
    std::pair<std::shared_ptr<MergeTreeDataPartChecksums>, bool> getOrSet(const String & name, const ChecksumsName& key, LoadFunc && load)
    {
        bool flag = false;

        std::shared_ptr<MergeTreeDataPartChecksums> result;

        auto& shard = parts_to_checksums_cache.shard(key);
        std::shared_ptr<ChecksumsCacheItem> cache_result = shard.get(key);
        if (!cache_result || cache_result->first != ChecksumsCacheState::Cached)
        {
            if (nvm_cache && nvm_cache->isEnabled())
            {
                auto handle = nvm_cache->find<MergeTreeDataPartChecksums>(HybridCache::makeHashKey(key.c_str()), [&key, &shard, &name, &flag, this](std::shared_ptr<void> ptr, HybridCache::Buffer buffer)
                {
                    auto checksums = std::static_pointer_cast<MergeTreeDataPartChecksums>(ptr);
                    auto read_buffer = buffer.asReadBuffer();
                    checksums->read(read_buffer);
                    if (internalInsert(shard, key, name, checksums))
                        flag = true;

                }, HybridCache::EngineTag::ChecksumCache);
                if (auto ptr = handle.get())
                {
                    auto mapped = std::static_pointer_cast<MergeTreeDataPartChecksums>(ptr);
                    if (!mapped->empty())
                        return std::make_pair(mapped, flag);
                }
            }

            ProfileEvents::increment(ProfileEvents::ChecksumsCacheMisses);

            result = load();

            if(internalInsert(shard, key, name, result))
                flag = true;
        }
        else
        {
            ProfileEvents::increment(ProfileEvents::ChecksumsCacheHits);
            result = cache_result->second;
        }
        
        return std::make_pair(result, flag);
    }

    size_t count() const
    {
        return parts_to_checksums_cache.count();
    }

    size_t weight() const
    {
        return parts_to_checksums_cache.weight();
    }

    void dropChecksumCache(const String & name, bool with_lock = false)
    {
        size_t first_level_bucket = std::hash<String>()(name) % cache_shard_num;

        std::unique_lock<std::mutex> guard; 
        if (!with_lock)
        {
            guard = std::unique_lock(table_to_parts_cache_locks[first_level_bucket]);
        }

        auto & keys = table_to_parts_cache[first_level_bucket][name];
        for (const auto & key: keys) 
        {
            auto & shard = parts_to_checksums_cache.shard(key);
            shard.erase(key);
        }

        table_to_parts_cache[first_level_bucket][name].erase(name);
    }

    void reset()
    {
        for (size_t i = 0; i < cache_shard_num; i++) 
        {
            std::unique_lock<std::mutex> guard(table_to_parts_cache_locks[i]);
            auto iter = table_to_parts_cache[i].begin();
            while (iter != table_to_parts_cache[i].end())
            {
                auto name = iter->first;
                iter++;
                dropChecksumCache(name, true);
            }
        }
    }

    void setNvmCache(std::shared_ptr<NvmCache> nvm_cache_) { nvm_cache = nvm_cache_; }

private:
    using ChecksumsCacheBucket = BucketLRUCache<ChecksumsName, ChecksumsCacheItem, std::hash<ChecksumsName>, ChecksumsWeightFunction>;

    bool internalInsert(ChecksumsCacheBucket & shard, const ChecksumsName& key, const String & name, std::shared_ptr<MergeTreeDataPartChecksums> result)
    {
        bool inserted = shard.emplace(key, std::make_shared<ChecksumsCacheItem>(std::make_pair(ChecksumsCacheState::Caching, nullptr)));
        if (inserted)
        {
            {
                size_t first_level_bucket = std::hash<String>()(name) % cache_shard_num;
                std::unique_lock<std::mutex> guard(table_to_parts_cache_locks[first_level_bucket]);
                table_to_parts_cache[first_level_bucket][name].insert(key);
                LOG_TRACE(logger, "checksums insert table {} part {} bucket {}", name, key, first_level_bucket);
            }
            shard.update(key, std::make_shared<ChecksumsCacheItem>(std::make_pair(ChecksumsCacheState::Cached, result)));
        }
        return inserted;
    }

    std::pair<bool, std::shared_ptr<ChecksumsCacheItem>> onEvictChecksums(
            [[maybe_unused]]const ChecksumsName& key, const std::shared_ptr<ChecksumsCacheItem>& item, size_t)
    {
        if (item->first != ChecksumsCacheState::Cached)
        {
            return {false, nullptr};
        }

        return {false, std::make_shared<ChecksumsCacheItem>(std::make_pair(ChecksumsCacheState::Deleting, nullptr))};
    }

    void afterEvictChecksums([[maybe_unused]] const std::vector<std::pair<ChecksumsName, std::shared_ptr<ChecksumsCacheItem>>>& removed_elements,
         const std::vector<std::pair<ChecksumsName, std::shared_ptr<ChecksumsCacheItem>>>& updated_elements) 
    {
        for (const auto & pair : updated_elements)
        {
            auto key = pair.first;
            auto name = getStorageUniqueId(key);

            size_t first_level_bucket = std::hash<String>()(name) % cache_shard_num;

            auto & cache_item = pair.second;
            if (cache_item->first != ChecksumsCacheState::Cached) 
            {
                LOG_TRACE(logger, "afterEvictChecksums table {} part {} bucket {} state {}", 
                    name, key, first_level_bucket, cache_item->first);
                continue;
            }

            {
                std::unique_lock<std::mutex> guard(table_to_parts_cache_locks[first_level_bucket]);
                table_to_parts_cache[first_level_bucket][name].erase(key);
            }

            auto& shard = parts_to_checksums_cache.shard(key);
            shard.erase(key);
        }

        if (nvm_cache && nvm_cache->isEnabled())
        {
            for (const auto & pair : removed_elements)
            {
                auto hash_key = HybridCache::makeHashKey(pair.first.c_str());
                auto token = nvm_cache->createPutToken(hash_key.key());

                auto & checksums = pair.second->second;
                std::shared_ptr<Memory<>> mem = std::make_shared<Memory<>>(DBMS_DEFAULT_BUFFER_SIZE);
                BufferWithOutsideMemory<WriteBuffer> write_buffer(*mem);
                checksums->write(write_buffer);

                nvm_cache->put(hash_key, std::move(mem), std::move(token), [](void * obj) {
                    auto * ptr = reinterpret_cast<Memory<> *>(obj);
                    return HybridCache::BufferView{ptr->size(), reinterpret_cast<UInt8 *>(ptr->data())};
                }, HybridCache::EngineTag::ChecksumCache); 
            }
        }
    }

    size_t cache_shard_num;
    std::unique_ptr<std::unordered_map<String, std::set<ChecksumsName>>[]> table_to_parts_cache;
    std::unique_ptr<std::mutex[]> table_to_parts_cache_locks;

    ShardCache<ChecksumsName, std::hash<ChecksumsName>, 
        BucketLRUCache<ChecksumsName, ChecksumsCacheItem, std::hash<ChecksumsName>, ChecksumsWeightFunction>> parts_to_checksums_cache;

    std::shared_ptr<NvmCache> nvm_cache{};

    Poco::Logger * logger = nullptr;
};

using ChecksumsCachePtr = std::shared_ptr<ChecksumsCache>;

}
