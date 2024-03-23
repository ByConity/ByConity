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

#include <Catalog/DataModelPartWrapper.h>
#include <Core/UUID.h>
#include <Protos/DataModelHelpers.h>
#include <Protos/data_models.pb.h>
#include <Storages/UUIDAndPartName.h>
#include <Common/LRUCache.h>
#include <Common/ScanWaitFreeMap.h>

namespace ProfileEvents
{
extern const Event CnchDataPartCacheHits;
extern const Event CnchDataPartCacheMisses;
extern const Event CnchDataDeleteBitmapCacheHits;
extern const Event CnchDataDeleteBitmapCacheMisses;
}

namespace DB
{

using TableWithPartition = std::pair<UUID, String>;
using DataPartModelsMap = ScanWaitFreeMap<String, DataModelPartWrapperPtr>;
using DeleteBitmapModelsMap = ScanWaitFreeMap<String, DataModelDeleteBitmapPtr>;

const std::function<String(const DataModelPartWrapperPtr &)> dataPartGetKeyFunc =
    [](const DataModelPartWrapperPtr & part_wrapper_ptr) {
        return part_wrapper_ptr->name;
    };
const std::function<String(const DataModelDeleteBitmapPtr &)> dataDeleteBitmapGetKeyFunc
    = [](const DataModelDeleteBitmapPtr & ptr) { return dataModelName(*ptr); };

template <typename Type>
struct DataWeightFunction
{
    size_t operator()(const Type & parts_map) const { return parts_map.size(); }
};

template <typename TKey, typename TMapped, typename HashFunction, typename WeightFunction>
class CnchDataCache : public LRUCache<TKey, TMapped, HashFunction, WeightFunction>
{
private:
    virtual void hits() { }
    virtual void misses() { }

public:
    virtual ~CnchDataCache() override = default;
    using Key = TKey;
    using Mapped = TMapped;
    using MappedPtr = std::shared_ptr<Mapped>;
    using Delay = std::chrono::seconds;
    using Base = LRUCache<TKey, TMapped, HashFunction, WeightFunction>;

    explicit CnchDataCache(size_t max_parts_to_cache, const Delay & expiration_delay = Delay::zero()) 
        : Base(max_parts_to_cache, expiration_delay)
    {
        this->inner_container = std::make_unique<CacheContainer<Key>>();
    }

    template <typename LoadFunc>
    MappedPtr getOrSet(const Key & key, LoadFunc && load)
    {
        auto result = Base::getOrSet(key, load);

        if (result.second)
        {
            this->hits();
            if (this->inner_container)
            {
                String uuid_str = UUIDHelpers::UUIDToString(key.first);
                this->inner_container->insert(uuid_str, key);
            }
        }
        else
            this->misses();

        return result.first;
    }

    void insert(const Key & key, const MappedPtr & value)
    {
        Base::set(key, value);
        if (this->inner_container)
        {
            String uuid_str = UUIDHelpers::UUIDToString(key.first);
            this->inner_container->insert(uuid_str, key);
        }
    }

    void dropCache(const UUID & uuid)
    {
        if (!this->inner_container)
            return;

        String uuid_str = UUIDHelpers::UUIDToString(uuid);
        const auto keys = this->inner_container->getKeys(uuid_str);
        for (const auto & key : keys)
            remove(key);
    }

    std::unordered_map<String, std::pair<size_t, size_t>> getTableCacheInfo()
    {
        auto keys = this->inner_container->getAllKeys();

        std::unordered_map<String, std::pair<size_t, size_t>> res;

        for (const auto & key : keys)
        {
            auto cached = this->get(key);
            if (cached)
            {
                String uuid_str = UUIDHelpers::UUIDToString(key.first);
                auto it = res.find(uuid_str);
                if (it == res.end())
                {
                    res.emplace(uuid_str, std::make_pair(1, cached->size()));
                }
                else
                {
                    it->second.first++;
                    it->second.second += cached->size();
                }
            }
        }

        return res;
    }

    void remove(const Key & key) { Base::remove(key); }
};

class CnchDataPartCache
    : public CnchDataCache<TableWithPartition, DataPartModelsMap, TableWithPartitionHash, DataWeightFunction<DataPartModelsMap>>
{
private:
    using Base = CnchDataCache<TableWithPartition, DataPartModelsMap, TableWithPartitionHash, DataWeightFunction<DataPartModelsMap>>;

    void hits() override { ProfileEvents::increment(ProfileEvents::CnchDataPartCacheHits); }

    void misses() override { ProfileEvents::increment(ProfileEvents::CnchDataPartCacheMisses); }

public:
    ~CnchDataPartCache() override = default;
    explicit CnchDataPartCache(size_t max_parts_to_cache, const Delay & expiration_delay) : Base(max_parts_to_cache, expiration_delay) { }
};

class CnchDeleteBitmapCache
    : public CnchDataCache<TableWithPartition, DeleteBitmapModelsMap, TableWithPartitionHash, DataWeightFunction<DeleteBitmapModelsMap>>
{
private:
    using Base
        = CnchDataCache<TableWithPartition, DeleteBitmapModelsMap, TableWithPartitionHash, DataWeightFunction<DeleteBitmapModelsMap>>;
    void hits() override { ProfileEvents::increment(ProfileEvents::CnchDataDeleteBitmapCacheHits); }

    void misses() override { ProfileEvents::increment(ProfileEvents::CnchDataDeleteBitmapCacheMisses); }

public:
    ~CnchDeleteBitmapCache() override = default;
    explicit CnchDeleteBitmapCache(size_t max_parts_to_cache, const Delay & expiration_delay) : Base(max_parts_to_cache, expiration_delay) { }
};

using CnchDataPartCachePtr = std::shared_ptr<CnchDataPartCache>;
using CnchDeleteBitmapCachePtr = std::shared_ptr<CnchDeleteBitmapCache>;

}
