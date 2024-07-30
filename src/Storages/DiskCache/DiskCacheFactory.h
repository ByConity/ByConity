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

#include <Storages/DiskCache/DiskCache_fwd.h>
#include <common/singleton.h>
#include <unordered_map>
#include <Poco/Exception.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}

namespace DB
{
class Context;

enum class DiskCacheType {
    File, // for generic file disk cache
    MergeTree,
    Hive,
    Manifest
};

std::string diskCacheTypeToString(DiskCacheType type);
DiskCacheType stringToDiskCacheType(const std::string & type);

class DiskCacheFactory : public ext::singleton<DiskCacheFactory>
{
public:
    void init(Context & context);

    /// not thread-safe, you must call it when server startup
    void registerDiskCaches(Context & global_context);

    void shutdown();

    IDiskCachePtr get(DiskCacheType type)
    {
        auto it = caches.find(type);
        if (it == caches.end())
            throw Poco::Exception("Unknown disk cache " + diskCacheTypeToString(type), ErrorCodes::BAD_ARGUMENTS);
        return it->second;
    }

    IDiskCachePtr tryGet(DiskCacheType type)
    {
        auto it = caches.find(type);
        if (it == caches.end())
            return nullptr;
        return it->second;
    }

private:
    void addNewCache(Context & context, const std::string & cache_name, bool create_default);
    std::unordered_map<DiskCacheType, IDiskCachePtr> caches;
};
}
