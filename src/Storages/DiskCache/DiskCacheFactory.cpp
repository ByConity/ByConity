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

#include <Disks/IStoragePolicy.h>
#include <Interpreters/Context.h>
#include <Storages/DiskCache/DiskCacheLRU.h>
#include <Storages/DiskCache/DiskCacheSettings.h>
#include <Storages/DiskCache/DiskCacheSimpleStrategy.h>
#include "common/logger_useful.h"

namespace DB
{

void DiskCacheFactory::init(Context & context)
{
    if (!caches.empty())
        throw Exception("Can't repeat register DiskCache!", DB::ErrorCodes::LOGICAL_ERROR);
    const auto & config = context.getConfigRef();

    // TODO: volume
    VolumePtr disk_cache_volume = context.getStoragePolicy("default")->getVolume(0);
    auto throttler = context.getDiskCacheThrottler();

    /// init pool
    IDiskCache::init(context);
    Poco::Logger * log {&Poco::Logger::get("DiskCacheFactory")};

    // build disk cache for each type
    if (config.has(DiskCacheSettings::root))
    {
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(DiskCacheSettings::root, keys);
        for (const auto & key : keys)
        {
            DiskCacheSettings cache_settings;
            cache_settings.loadFromConfig(config, key);
            LOG_TRACE(log,fmt::format("Creating DiskCache of {} kind by setting: {}",key,cache_settings.toString()));
            auto disk_cache = std::make_shared<DiskCacheLRU>(disk_cache_volume, throttler, cache_settings);
            caches.emplace(stringToDiskCacheType(key), disk_cache);
        }
    }

    // create dafault cache for MergeTree Diskcache
    DiskCacheSettings cache_settings;
    if (caches.find(DiskCacheType::MergeTree) == caches.end())
    {
        LOG_TRACE(log,fmt::format("Creating DiskCache of {} kind by setting: {}",diskCacheTypeToString(DiskCacheType::MergeTree),cache_settings.toString()));
        auto disk_cache = std::make_shared<DiskCacheLRU>(disk_cache_volume, throttler, cache_settings);
        caches.emplace(DiskCacheType::MergeTree, disk_cache);
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


}
