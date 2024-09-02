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

#include <Storages/CnchStorageCache.h>
#include <Storages/IStorage.h>

namespace DB
{

void CnchStorageCache::insert(const StorageID & storage_id, const UInt64 ts, const StoragePtr & storage_ptr)
{
    std::unique_lock<std::shared_mutex> lock(cache_mutex);

    TableName full_name = std::make_pair(storage_id.database_name, storage_id.table_name);
    auto cached = Base::get(full_name);
    if (cached && cached->first > ts)
        return;
    auto new_data = std::make_shared<TableData>(ts, storage_ptr);
    Base::set(full_name, new_data);
    if (inner_container)
        inner_container->insert(storage_id.database_name, full_name);
    if (storage_id.hasUUID())
    {
        // erase both by name and uuid to make sure update to bimap succeed.
        uuid_to_table_names.left.erase(storage_id.uuid);
        uuid_to_table_names.right.erase(full_name);
        uuid_to_table_names.insert({storage_id.uuid, full_name});
    }
}

StoragePtr CnchStorageCache::getImpl(const TableName & table_name)
{
    auto table_data_ptr = Base::get(table_name);
    if (table_data_ptr)
        return table_data_ptr->second;
    else
        return {};
}

StoragePtr CnchStorageCache::get(const String & db, const String & table)
{
    std::shared_lock<std::shared_mutex> lock(cache_mutex);
    return getImpl(std::make_pair(db, table));
}

StoragePtr CnchStorageCache::get(const UUID & uuid)
{
    std::shared_lock<std::shared_mutex> lock(cache_mutex);
    auto it = uuid_to_table_names.left.find(uuid);
    if (it == uuid_to_table_names.left.end())
    {
        return {};
    }
    return getImpl(it->second);
}

void CnchStorageCache::removeUnlock(const String & db, const String & table)
{
    TableName table_name = std::make_pair(db, table);
    uuid_to_table_names.right.erase(table_name);
    Base::remove(table_name);
}

void CnchStorageCache::remove(const String & db, const String & table)
{
    std::unique_lock<std::shared_mutex> lock(cache_mutex);
    removeUnlock(db, table);
}

void CnchStorageCache::remove(const String & db)
{
    std::unique_lock<std::shared_mutex> lock(cache_mutex);
    if (!inner_container)
        return;

    const auto & tables = inner_container->getKeys(db);
    for (const auto & table : tables)
        removeUnlock(table.first, table.second);
}

void CnchStorageCache::clear()
{
    std::unique_lock<std::shared_mutex> lock(cache_mutex);
    Base::reset();
}

size_t CnchStorageCache::uuidNameMappingSize()
{
    std::unique_lock<std::shared_mutex> lock(cache_mutex);
    return uuid_to_table_names.size();
}

void CnchStorageCache::onEvict(const Key & key)
{
    // Note, We already get cache lock when onEvict calls. No need to get lock here
    uuid_to_table_names.right.erase(key);
}

}
