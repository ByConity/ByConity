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

#include "DiskCacheFactory.h"
#include <cstddef>
#include <memory>

#include <Disks/IStoragePolicy.h>
#include <Interpreters/Context.h>
#include <Storages/DiskCache/DiskCacheLRU.h>
#include <Storages/DiskCache/DiskCacheSettings.h>
#include <Storages/DiskCache/DiskCacheSimpleStrategy.h>
#include <common/logger_useful.h>
#include <Disks/SingleDiskVolume.h>
#include <Storages/DiskCache/IDiskCache.h>

namespace DB
{

void DiskCacheFactory::init(Context & context)
{
    if (!caches.empty())
        throw Exception("Can't repeat register DiskCache!", DB::ErrorCodes::LOGICAL_ERROR);
    const auto & config = context.getConfigRef();

    /// init pool
    IDiskCache::init(context);
    LoggerPtr log{getLogger("DiskCacheFactory")};

    // build disk cache for each type
    if (config.has(DiskCacheSettings::root))
    {
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(DiskCacheSettings::root, keys);
        for (const auto & key : keys)
            addNewCache(context, key, false);
    }

    // create dafault cache for MergeTree Diskcache
    if (caches.find(DiskCacheType::MergeTree) == caches.end())
    {
        LOG_TRACE(log, "Creating default DiskCache of {}", diskCacheTypeToString(DiskCacheType::MergeTree));
        addNewCache(context, diskCacheTypeToString(DiskCacheType::MergeTree), true);
    }

    // create default manifest file cache if it does not exists
    if (caches.find(DiskCacheType::Manifest) == caches.end())
    {
        LOG_TRACE(log, "Creating default DiskCache of {}", diskCacheTypeToString(DiskCacheType::Manifest));
        addNewCache(context, diskCacheTypeToString(DiskCacheType::Manifest), true);
    }
}

std::string diskCacheTypeToString(const DiskCacheType type)
{
    switch (type)
    {
        case DiskCacheType::File:
            return "File";
        case DiskCacheType::MergeTree:
            return "MergeTree";
        case DiskCacheType::Hive:
            return "Hive";
        case DiskCacheType::Manifest:
            return "Manifest";
    }

    return "InvalidDiskCacheType";
}

DiskCacheType stringToDiskCacheType(const std::string & type)
{
    if (type == "File")
        return DiskCacheType::File;

    if (type == "simple" || type == "MergeTree") // `simple` for compatible with old config
        return DiskCacheType::MergeTree;

    if (type == "parquet" || type == "Hive") // `parquet` for compatible with old config
        return DiskCacheType::Hive;

    if (type == "Manifest")
        return DiskCacheType::Manifest;

    throw Poco::Exception("Invalid strategy name: " + type + " should be `simple`, `parquet`, `File`, `MergeTree`, `Hive`", ErrorCodes::BAD_ARGUMENTS);
}


void DiskCacheFactory::shutdown()
{
    for (const auto & disk_cache : caches)
    {
        if (disk_cache.second)
            disk_cache.second->shutdown();
    }
    IDiskCache::close();
}

void DiskCacheFactory::addNewCache(Context & context, const std::string & cache_name, bool create_default)
{
    LoggerPtr log{getLogger("DiskCacheFactory")};

    DiskCacheSettings cache_settings;
    auto throttler = context.getDiskCacheThrottler();

    const auto & config = context.getConfigRef();
    cache_settings.loadFromConfig(config, cache_name);

    VolumePtr disk_cache_volume = context.getStoragePolicy(cache_settings.disk_policy)->getVolumeByName("local", true);

    auto total_space_unlimited = disk_cache_volume->getTotalSpace(true);
    auto total_space_limited = disk_cache_volume->getTotalSpace(false);

    if (create_default)
    {
        cache_settings.lru_max_size = std::min(
            static_cast<size_t>(total_space_unlimited.bytes * (cache_settings.lru_max_percent * 1.0 / 100)), cache_settings.lru_max_size);
        cache_settings.lru_max_nums = std::min(
            static_cast<size_t>(total_space_unlimited.inodes * (cache_settings.lru_max_percent * 1.0 / 100)), cache_settings.lru_max_nums);
    }
    else
    {
        cache_settings.lru_max_size = std::min(
            total_space_limited.bytes,
            std::min(
                static_cast<size_t>(total_space_unlimited.bytes * (cache_settings.lru_max_percent * 1.0 / 100)),
                cache_settings.lru_max_size));
        cache_settings.lru_max_nums = std::min(
            total_space_limited.inodes,
            std::min(
                static_cast<size_t>(total_space_unlimited.inodes * (cache_settings.lru_max_percent * 1.0 / 100)),
                cache_settings.lru_max_nums));
    }

    if (!cache_settings.meta_cache_size_ratio)
    {
        auto disk_cache = std::make_shared<DiskCacheLRU>(
            cache_name, disk_cache_volume, throttler, cache_settings, std::make_shared<DiskCacheSimpleStrategy>(cache_settings));
        caches.emplace(stringToDiskCacheType(cache_name), disk_cache);
        LOG_DEBUG(log, fmt::format("Registered `{}` single disk cache", cache_name));
    }
    else
    {
        auto strategy = std::make_shared<DiskCacheSimpleStrategy>(cache_settings);

        auto meta_disk_cache = std::make_shared<DiskCacheLRU>(
            cache_name, disk_cache_volume, throttler, cache_settings, strategy, IDiskCache::DataType::META);
        auto data_disk_cache = std::make_shared<DiskCacheLRU>(
            cache_name, disk_cache_volume, throttler, cache_settings, strategy, IDiskCache::DataType::DATA);
        caches.emplace(
            stringToDiskCacheType(cache_name),
            std::make_shared<MultiDiskCache>(
                cache_name, disk_cache_volume, throttler, cache_settings, strategy, meta_disk_cache, data_disk_cache));
        LOG_DEBUG(log, fmt::format("Registered `{}` multi disk cache", cache_name));
    }
}

}
