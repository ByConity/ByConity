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

#include <Storages/PartCacheManager.h>

#include <chrono>
#include <iterator>
#include <Catalog/Catalog.h>
#include <Catalog/CatalogFactory.h>
#include <CloudServices/CnchServerClient.h>
#include <Core/Types.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <MergeTreeCommon/CnchTopologyMaster.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Storages/CnchDataPartCache.h>
#include <Storages/CnchStorageCache.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/TableMetaEntry.h>
#include <Common/ConsistentHashUtils/Hash.h>
#include <Common/HostWithPorts.h>
#include <Common/RWLock.h>
#include <Common/RpcClientPool.h>
#include <Common/Status.h>
#include <Common/serverLocality.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_STORAGE;
    extern const int TIMEOUT_EXCEEDED;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
}

/// Mock function for `getTimestamp`/`tryGetTimestamp`.
constexpr auto dummy_get_timestamp = []() -> UInt64 {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
};

PartCacheManager::PartCacheManager(ContextMutablePtr context_, bool dummy_mode)
    : WithMutableContext(context_), dummy_mode(dummy_mode), table_partition_metrics(context_)
{
    part_cache_ptr = std::make_shared<CnchDataPartCache>(getContext()->getConfigRef().getUInt("size_of_cached_parts", 100000));
    storageCachePtr = std::make_shared<CnchStorageCache>(getContext()->getConfigRef().getUInt("cnch_max_cached_storage", 10000));
    meta_lock_cleaner = getContext()->getSchedulePool().createTask("MetaLockCleaner", [this](){
        try
        {
            cleanMetaLock();
        }
        catch(...)
        {
            tryLogDebugCurrentException(__PRETTY_FUNCTION__);
        }
        /// schedule every hour.
        this->meta_lock_cleaner->scheduleAfter(3 * 1000);
    });
    active_table_loader = getContext()->getSchedulePool().createTask("ActiveTablesLoader", [this](){
        // load tables when server start up.
        try
        {
            loadActiveTables();
        }
        catch(...)
        {
            tryLogDebugCurrentException(__PRETTY_FUNCTION__);
        }
    });
    trashed_active_tables_cleaner = getContext()->getSchedulePool().createTask("TrashedActiveTablesCleaner", [this]() {
        try
        {
            cleanTrashedActiveTables();
        }
        catch (...)
        {
            tryLogDebugCurrentException(__PRETTY_FUNCTION__);
        }
        this->trashed_active_tables_cleaner->scheduleAfter(5 * 60 * 1000);
    });
    if (getContext()->getServerType() == ServerType::cnch_server && !dummy_mode)
    {
        meta_lock_cleaner->activateAndSchedule();
        active_table_loader->activateAndSchedule();
        trashed_active_tables_cleaner->activateAndSchedule();
    }
}

PartCacheManager::~PartCacheManager()
{
    try
    {
        shutDown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void PartCacheManager::mayUpdateTableMeta(const IStorage & storage, const PairInt64 & topology_version)
{
    /* Fetches partitions from metastore if storage is not present in active_tables*/

    /// Only handle MergeTree tables
    const auto * cnch_table = dynamic_cast<const MergeTreeMetaBase*>(&storage);
    if (!cnch_table)
        return;

    // auto load_nhut = [&](TableMetaEntryPtr & meta_ptr)
    // {
    //     if (getContext()->getSettingsRef().server_write_ha)
    //     {
    //         UInt64 pts = getContext()->getPhysicalTimestamp();
    //         if (pts)
    //         {
    //             UInt64 fetched_nhut = getContext()->getCnchCatalog()->getNonHostUpdateTimestampFromByteKV(storage.getStorageUUID());
    //             if (pts - fetched_nhut > 9)
    //                 meta_ptr->cached_non_host_update_ts = fetched_nhut;
    //         }
    //     }
    // };

    auto load_table_partitions = [&](TableMetaEntryPtr & meta_ptr) -> bool
    {
        bool meta_loaded = false;
        auto table_lock = meta_ptr->writeLock();
        /// If other thread finished load, just return
        if (meta_ptr->cache_status == CacheStatus::LOADED)
            return false;
        /// Invalid old cache if any
        part_cache_ptr->dropCache(storage.getStorageUUID());
        storageCachePtr->remove(storage.getDatabaseName(), storage.getTableName());

        try
        {
            meta_ptr->cache_status = CacheStatus::LOADING;
            // No need to load from catalog in dummy mode.
            if (likely(!dummy_mode))
            {
                getContext()->getCnchCatalog()->getPartitionsFromMetastore(*cnch_table, meta_ptr->partitions);
                getContext()->getCnchCatalog()->getTableClusterStatus(storage.getStorageUUID(), meta_ptr->is_clustered);
                getContext()->getCnchCatalog()->getTablePreallocateVW(storage.getStorageUUID(), meta_ptr->preallocate_vw);
                meta_loaded = true;
            }
            meta_ptr->table_definition_hash = storage.getTableHashForClusterBy();
            /// Needs make sure no other thread force reload
            UInt32 loading = CacheStatus::LOADING;
            meta_ptr->cache_status.compare_exchange_strong(loading, CacheStatus::LOADED);
            meta_ptr->cache_version.set(topology_version);
            return meta_loaded;
        }
        catch (...)
        {
            /// Handle bytekv exceptions and make sure next time will retry
            tryLogCurrentException(&Poco::Logger::get("PartCacheManager::mayUpdateTableMeta"));
            meta_ptr->cache_status = CacheStatus::UINIT;
            throw;
        }
    };

    UUID uuid = storage.getStorageUUID();
    String server_vw_name = cnch_table->getSettings()->cnch_server_vw;

    TableMetaEntryPtr meta_ptr = nullptr;

    {
        std::unique_lock<std::mutex> lock(cache_mutex);
        auto it = active_tables.find(uuid);

        if (it == active_tables.end() || it->second->server_vw_name != server_vw_name)
        {
            /// table is not in active table list, need load partition info;
            auto meta_lock_it = meta_lock_container.find(uuid);
            /// If the meta lock is already exists, reuse it.
            if (meta_lock_it != meta_lock_container.end())
            {
                meta_ptr = std::make_shared<TableMetaEntry>(
                    storage.getDatabaseName(),
                    storage.getTableName(),
                    UUIDHelpers::UUIDToString(storage.getStorageUUID()),
                    meta_lock_it->second);
            }
            else
            {
                meta_ptr = std::make_shared<TableMetaEntry>(
                    storage.getDatabaseName(), storage.getTableName(), UUIDHelpers::UUIDToString(storage.getStorageUUID()));
                /// insert the new meta lock into lock container.
                meta_lock_container.emplace(uuid, meta_ptr->meta_mutex);
            }
            meta_ptr->server_vw_name = server_vw_name;
            active_tables.emplace(uuid, meta_ptr);
        }
        else
        {
            PairInt64 current_cache_version = it->second->cache_version.get();
            if (it->second->cache_status != CacheStatus::LOADED)
            {
                /// Needs to wait for other thread to load and retry if other thread failed
                meta_ptr = it->second;
            }
            else if (topology_version != PairInt64{0, 0} && current_cache_version != topology_version)
            {
                // update cache version if host server doesn't change in two consecutive topologies.
                if (current_cache_version.low + 1 == topology_version.low)
                    it->second->cache_version.set(topology_version);
                else
                {
                    it->second->cache_status = CacheStatus::UINIT;
                    meta_ptr = it->second;
                    LOG_DEBUG(&Poco::Logger::get("PartCacheManager::MetaEntry"), "Invalid part cache because of cache version mismatch for table {}.{}", meta_ptr->database, meta_ptr->table);
                }
            }
        }
    }

    if (meta_ptr)
    {
        // load_nhut(meta_ptr);
        if (load_table_partitions(meta_ptr) && meta_ptr->partition_metrics_loaded)
        {
            /// A partition undergoing recalculation will not trigger recalculation
            /// again, so the cost of redundant calls is small.
            ///
            /// Even then, we need to avoid meaningless triggers.
            /// So only trigger a forced recalculation when cache updated.
            try
            {
                auto current_time = getContext()->getTimestamp();
                table_partition_metrics.recalculateOrSnapshotPartitionsMetrics(meta_ptr, current_time, true);
            }
            /// Never throw.
            /// Does not interfere with the primary logic.
            catch (...)
            {
                tryLogCurrentException(&Poco::Logger::get("PartCacheManager::mayUpdateTableMeta"));
            }
        }
    }
}

bool PartCacheManager::trySetCachedNHUTForUpdate(const UUID & uuid, const UInt64 & pts)
{
    std::lock_guard<std::mutex> lock(cached_nhut_mutex);
    auto it = cached_nhut_for_update.find(uuid);
    if (it == cached_nhut_for_update.end())
    {
        cached_nhut_for_update.emplace(uuid, pts);
    }
    else
    {
        if (it->second == pts)
            return false;

        /// renew the cached NHUT;
        it->second = pts;
    }
    return true;
}

bool PartCacheManager::checkIfCacheValidWithNHUT(const UUID & uuid, const UInt64 & nhut)
{
    TableMetaEntryPtr table_entry = getTableMeta(uuid);
    if (table_entry)
    {
        UInt64 cached_nhut = table_entry->cached_non_host_update_ts;
        if (cached_nhut == nhut && !table_entry->need_invalid_cache)
            return true;
        else if (cached_nhut < nhut)
        {
            table_entry->cached_non_host_update_ts.compare_exchange_strong(cached_nhut, nhut);
            table_entry->need_invalid_cache = true;
        }

        /// try invalid the part cache if the cached nhut is old enough;
        if (table_entry->need_invalid_cache && getContext()->getPhysicalTimestamp() - table_entry->cached_non_host_update_ts > 9000)
        {
            LOG_DEBUG(&Poco::Logger::get("PartCacheManager::getTableMeta"), "invalid part cache for {}. NHUT is {}", UUIDHelpers::UUIDToString(uuid), table_entry->cached_non_host_update_ts);
            invalidPartCache(uuid);
        }

        return false;
    }
    else
        return true;
}

void PartCacheManager::updateTableNameInMetaEntry(const String & table_uuid, const String & database_name, const String & table_name)
{
    TableMetaEntryPtr table_entry = getTableMeta(UUID(stringToUUID(table_uuid)));
    if (table_entry)
    {
        auto lock = table_entry->writeLock();
        table_entry->database = database_name;
        table_entry->table = table_name;
    }
}

TableMetaEntryPtr PartCacheManager::getTableMeta(const UUID & uuid)
{
    /* If non host server then returns nullptr.
     * If host server, then  returns table metadata.
     * */
    std::unique_lock<std::mutex> lock(cache_mutex);
    if (active_tables.find(uuid) == active_tables.end())
    {
        LOG_TRACE(&Poco::Logger::get("PartCacheManager::getTableMeta"), "Table id {} not found in active_tables", UUIDHelpers::UUIDToString(uuid));
        return nullptr;
    }

    return active_tables[uuid];
}

std::vector<TableMetaEntryPtr> PartCacheManager::getAllActiveTables()
{
    std::vector<TableMetaEntryPtr> res;
    std::unique_lock<std::mutex> lock(cache_mutex);
    for (auto & [uuid, entry] : active_tables)
        res.push_back(entry);
    return res;
}

UInt64 PartCacheManager::getTableLastUpdateTime(const UUID & uuid)
{
    UInt64 last_update_time {0};
    TableMetaEntryPtr table_entry = getTableMeta(uuid);
    if (table_entry)
    {
        {
            auto lock = table_entry->readLock();
            last_update_time = table_entry->last_update_time;
        }
        if (last_update_time == 0)
        {
            UInt64 ts ={};
            if (unlikely(dummy_mode))
            {
                ts = dummy_get_timestamp();
            }
            else
            {
                ts = getContext()->tryGetTimestamp();
            }
            auto lock = table_entry->writeLock();
            if (table_entry->last_update_time == 0)
            {
                if (ts!=TxnTimestamp::maxTS())
                    table_entry->last_update_time = ts;
            }
            last_update_time = table_entry->last_update_time;
        }
    }
    return last_update_time;
}

void PartCacheManager::setTableClusterStatus(const UUID & uuid, const bool clustered)
{
    TableMetaEntryPtr table_entry = getTableMeta(uuid);
    if (table_entry)
    {
        auto lock = table_entry->writeLock();
        table_entry->is_clustered = clustered;
        auto table = getContext()->getCnchCatalog()->getTableByUUID(*getContext(), UUIDHelpers::UUIDToString(uuid), TxnTimestamp::maxTS());
        if (table)
            table_entry->table_definition_hash = table->getTableHashForClusterBy();
    }
}

bool PartCacheManager::getTableClusterStatus(const UUID & uuid)
{
    TableMetaEntryPtr table_entry = getTableMeta(uuid);

    bool clustered;
    if (table_entry)
    {
        auto lock = table_entry->readLock();
        clustered =  table_entry->is_clustered;
    }
    else
        getContext()->getCnchCatalog()->getTableClusterStatus(uuid, clustered);

    return clustered;
}

void PartCacheManager::setTablePreallocateVW(const UUID & uuid, const String vw)
{
    TableMetaEntryPtr table_entry = getTableMeta(uuid);
    if (table_entry)
    {
        auto lock = table_entry->writeLock();
        table_entry->preallocate_vw = vw;
    }
}

String PartCacheManager::getTablePreallocateVW(const UUID & uuid)
{
    TableMetaEntryPtr table_entry = getTableMeta(uuid);

    String vw;
    if (table_entry)
    {
        auto lock = table_entry->readLock();
        vw =  table_entry->preallocate_vw;
    }
    else
        getContext()->getCnchCatalog()->getTablePreallocateVW(uuid, vw);

    return vw;
}

bool PartCacheManager::getPartsInfoMetrics(
    const IStorage & i_storage, std::unordered_map<String, PartitionFullPtr> & partitions, bool require_partition_info)
{
    return table_partition_metrics.getPartsInfoMetrics(i_storage, partitions, require_partition_info);
}

bool PartCacheManager::getPartitionList(
    const IStorage & table,
    std::vector<std::shared_ptr<MergeTreePartition>> & partition_list,
    const PairInt64 & topology_version)
{
    mayUpdateTableMeta(table, topology_version);
    TableMetaEntryPtr meta_ptr = getTableMeta(table.getStorageUUID());
    if (meta_ptr)
    {
        partition_list = meta_ptr->getPartitionList();
        return true;
    }
    return false;
}

bool PartCacheManager::getPartitionIDs(const IStorage & storage, std::vector<String> & partition_ids, const PairInt64 & topology_version)
{
    mayUpdateTableMeta(storage, topology_version);
    TableMetaEntryPtr meta_ptr = getTableMeta(storage.getStorageUUID());

    if (meta_ptr)
    {
        partition_ids = meta_ptr->getPartitionIDs();
        return true;
    }

    return false;
}

void PartCacheManager::invalidPartCache(const UUID & uuid)
{
    std::unique_lock<std::mutex> lock(cache_mutex);
    invalidPartCacheWithoutLock(uuid, lock);
}

void PartCacheManager::invalidCacheWithNewTopology(const CnchServerTopology & topology)
{
    // do nothing if servers is empty
    if (topology.empty())
        return;
    String rpc_port = std::to_string(getContext()->getRPCPort());
    std::unique_lock<std::mutex> lock(cache_mutex);
    for (auto it=active_tables.begin(); it!= active_tables.end();)
    {
        auto server = topology.getTargetServer(UUIDHelpers::UUIDToString(it->first), it->second->server_vw_name);
        if (!isLocalServer(server.getRPCAddress(), rpc_port))
        {
            LOG_DEBUG(&Poco::Logger::get("PartCacheManager::invalidCacheWithNewTopology"), "Dropping part cache of {}", UUIDHelpers::UUIDToString(it->first));
            part_cache_ptr->dropCache(it->first);
            storageCachePtr->remove(it->second->database, it->second->table);

            {
                /// 1. Put it into the trashcan.
                /// 2. Remove it from `active_tables`.
                /// 3. Notify the `TableMetaEntry` to be destructed.
                ///   (to accelerate the destruction of `TableMetaEntry`)
                ///
                /// As a side effect, `it` will be updated to the next element.
                ///
                /// Lock will be held to make sure trash always have the last reference counter.
                std::unique_lock<std::mutex> lock_of_trashcan(trashed_active_tables_mutex);
                {
                    trashed_active_tables.push_back(it->second);
                    it = active_tables.erase(it);
                    trashed_active_tables.back()->forEachPartition([](PartitionInfoPtr ptr) {
                        if (ptr->metrics_ptr)
                            ptr->metrics_ptr->notifyShutDown();
                    });
                }
            }
        }
        else
            it++;
    }
    /// reload active tables when topology change.
    active_table_loader->schedule();
}

void PartCacheManager::invalidPartCacheWithoutLock(const UUID & uuid, std::unique_lock<std::mutex> &)
{
    /// Send to-be-removed `TableMetaEntryPtr` to trash in order to destruct it asynchronously.
    auto it = active_tables.find(uuid);
    if (it != active_tables.end())
    {
        {
            /// 1. Put it into the trashcan.
            /// 2. Remove it from `active_tables`.
            /// 3. Notify the `TableMetaEntry` to be destructed.
            ///   (to accelerate the destruction of `TableMetaEntry`)
            ///
            /// As a side effect, `it` will be invalid.
            ///
            /// Lock will be held to make sure trash always have the last reference counter.
            std::unique_lock<std::mutex> lock_of_trashcan(trashed_active_tables_mutex);
            {
                trashed_active_tables.push_back(it->second);
                /// Make sure that the `TableMetaEntry` won't destruct
                /// before being removed from `active_tables`.
                active_tables.erase(it);
                trashed_active_tables.back()->forEachPartition([](PartitionInfoPtr ptr) {
                    if (ptr->metrics_ptr)
                        ptr->metrics_ptr->notifyShutDown();
                });
            }
        }
    }
    LOG_DEBUG(&Poco::Logger::get("PartCacheManager::invalidPartCacheWithoutLock"), "Dropping part cache of {}", UUIDHelpers::UUIDToString(uuid));
    part_cache_ptr->dropCache(uuid);
}

void PartCacheManager::invalidPartCache(const UUID & uuid, const TableMetaEntryPtr & meta_ptr, const std::unordered_map<String, Strings> & partition_to_parts)
{
    auto lock = meta_ptr->writeLock();

    for (const auto & partition_to_part : partition_to_parts)
    {
        auto cached = part_cache_ptr->get({uuid, partition_to_part.first});

        for (const auto & part_name : partition_to_part.second)
        {
            if (cached)
            {
                auto got = cached->find(part_name);
                if (got != cached->end())
                    cached->erase(part_name);
            }
        }
    }
}

void PartCacheManager::invalidPartCache(const UUID & uuid, const DataPartsVector & parts)
{
    TableMetaEntryPtr meta_ptr = getTableMeta(uuid);

    if (!meta_ptr)
        return;

    std::unordered_map<String, Strings> partition_to_parts;
    Strings partition_ids;
    for (auto & part : parts)
    {
        const String & partition_id = part->info.partition_id;
        auto it = partition_to_parts.find(partition_id);
        if (it != partition_to_parts.end())
        {
            it->second.emplace_back(part->name);
        }
        else
        {
            Strings part_list{part->name};
            partition_to_parts.emplace(partition_id, part_list);
            partition_ids.push_back(partition_id);
        }
    }
    invalidPartCache(uuid, meta_ptr, partition_to_parts);
}

void PartCacheManager::invalidPartCache(const UUID & uuid, const ServerDataPartsVector & parts)
{
    TableMetaEntryPtr meta_ptr = getTableMeta(uuid);

    if (!meta_ptr)
        return;

    std::unordered_map<String, Names> partition_to_parts;
    for (const auto & part : parts)
    {
        const String & partition_id = part->info().partition_id;
        auto it = partition_to_parts.find(partition_id);
        if (it != partition_to_parts.end())
        {
            it->second.emplace_back(part->name());
        }
        else
        {
            Names part_list{part->name()};
            partition_to_parts.emplace(partition_id, part_list);
        }
    }

    invalidPartCache(uuid, meta_ptr, partition_to_parts);
}

void PartCacheManager::invalidPartCache(const UUID & uuid, const Strings & part_names, MergeTreeDataFormatVersion version)
{
    TableMetaEntryPtr meta_ptr = getTableMeta(uuid);

    if (!meta_ptr)
        return;

    std::unordered_map<String, Strings> partition_to_parts;
    for (const auto & part_name : part_names)
    {
        MergeTreePartInfo part_info = MergeTreePartInfo::fromPartName(part_name, version);
        String partition_id = part_info.partition_id;
        auto it = partition_to_parts.find(partition_id);
        if (it != partition_to_parts.end())
        {
            it->second.emplace_back(part_name);
        }
        else
        {
            Strings part_list{part_name};
            partition_to_parts.emplace(partition_id, part_list);
        }
    }
    invalidPartCache(uuid, meta_ptr, partition_to_parts);
}

void PartCacheManager::insertDataPartsIntoCache(
    const IStorage & table,
    const pb::RepeatedPtrField<Protos::DataModelPart> & parts_model,
    const bool is_merged_parts,
    const bool should_update_metrics,
    const PairInt64 & topology_version)
{
    /// Only cache MergeTree tables
    if (!dynamic_cast<const MergeTreeMetaBase*>(&table))
        return;

    mayUpdateTableMeta(table, topology_version);
    UUID uuid = table.getStorageUUID();
    TableMetaEntryPtr meta_ptr = getTableMeta(uuid);
    if (!meta_ptr)
    {
        throw Exception("Table is not initialized before save parts into cache.", ErrorCodes::UNKNOWN_STORAGE);
    }

    /// Do all parse jobs outside lock
    std::unordered_map<String, DataModelPartWrapperVector> partitionid_to_parts;
    std::unordered_map<String, std::shared_ptr<MergeTreePartition>> partitionid_to_partition;
    auto & storage = dynamic_cast<const MergeTreeMetaBase &>(table);
    for (auto & part_model : parts_model)
    {
        auto part_wrapper_ptr = createPartWrapperFromModel(storage, Protos::DataModelPart(part_model));
        const auto & partition_id = part_wrapper_ptr->info->partition_id;
        if (!partitionid_to_partition.contains(partition_id))
            partitionid_to_partition[partition_id] = createPartitionFromMetaString(storage, part_model.partition_minmax());
        auto it = partitionid_to_parts.find(partition_id);
        if (it != partitionid_to_parts.end())
            it->second.emplace_back(part_wrapper_ptr);
        else
            partitionid_to_parts[partition_id] = DataModelPartWrapperVector{part_wrapper_ptr};
    }

    UInt64 ts{};
    if (unlikely(dummy_mode))
    {
        ts = dummy_get_timestamp();
    }
    else
    {
        ts = getContext()->tryGetTimestamp();
    }

    /// Get or create partitions from meta_ptr
    Strings partition_ids;
    partition_ids.reserve(partitionid_to_parts.size());
    for (const auto & pair : partitionid_to_parts)
        partition_ids.push_back(pair.first);
    auto meta_partitions = meta_ptr->getPartitions(partition_ids);
    /// Create when there are new partitions
    if (meta_partitions.size() < partitionid_to_parts.size())
    {
        auto & partitions = meta_ptr->partitions;
        for (auto & p : partitionid_to_parts)
        {
            const auto & partition_id = p.first;
            if (meta_partitions.contains(partition_id))
                continue;
            auto it = partitions
                          .emplace(
                              partition_id,
                              std::make_shared<CnchPartitionInfo>(
                                  UUIDHelpers::UUIDToString(uuid), partitionid_to_partition[partition_id], partition_id, true))
                          .first;
            meta_partitions.emplace(partition_id, *it);
        }
    }

    for (auto & [partition_id, parts_wrapper_vector] : partitionid_to_parts)
    {
        auto & partition_info_ptr = meta_partitions[partition_id];
        auto partition_write_lock = partition_info_ptr->writeLock();
        bool need_insert_into_cache = false;
        /// Check if new parts should be inserted into cache; Skip if cache status is UINIT
        if (partition_info_ptr->cache_status != CacheStatus::UINIT)
        {
            need_insert_into_cache = true;
            /// Check whether the partition cache has been invalidate by LRU.
            if (partition_info_ptr->cache_status == CacheStatus::LOADED)
            {
                auto cached = part_cache_ptr->get({uuid, partition_id});
                if (!cached)
                {
                    partition_info_ptr->cache_status = CacheStatus::UINIT;
                    need_insert_into_cache = false;
                }
            }
        }

        if (need_insert_into_cache)
        {
            auto cached = part_cache_ptr->get({uuid, partition_id});
            if (!cached)
            {
                /// directly insert all new parts into cache.
                cached = std::make_shared<DataPartModelsMap>();
                cached->insert(parts_wrapper_vector, [](const DataModelPartWrapperPtr & part_wrapper_ptr) { return part_wrapper_ptr->name; });
                part_cache_ptr->insert({uuid, partition_id}, cached);
            }
            else
            {
                cached->insert(parts_wrapper_vector, [](const DataModelPartWrapperPtr & part_wrapper_ptr) { return part_wrapper_ptr->name; });
                /// Force LRU cache update status (weight/evict).
                part_cache_ptr->insert({uuid, partition_id}, cached);
            }
        }
        if (should_update_metrics)
        {
            table_partition_metrics.updateMetrics(parts_wrapper_vector, partition_info_ptr->metrics_ptr, uuid, ts);
        }
    }

    if (!is_merged_parts)
        meta_ptr->last_update_time = (ts==TxnTimestamp::maxTS()) ? 0: ts;
}

void PartCacheManager::cleanMetaLock()
{
    std::unique_lock<std::mutex> lock(cache_mutex);
    for (auto it = meta_lock_container.begin(); it!=meta_lock_container.end();)
    {
        /// remove the meta_lock if it is not used elsewhere.
        if (it->second.unique())
        {
            it = meta_lock_container.erase(it);
        }
        else
        {
            it++;
        }
    }
}

void PartCacheManager::loadActiveTables()
{
    auto tables_meta = getContext()->getCnchCatalog()->getAllTables();
    if (tables_meta.empty())
        return;
    LOG_DEBUG(&Poco::Logger::get("PartCacheManager"), "Reloading {} active tables.", tables_meta.size());

    auto rpc_port = getContext()->getRPCPort();
    for (auto & table_meta : tables_meta)
    {
        if (table_meta.database() == "cnch_system" || table_meta.database() == "system" || Status::isDeleted(table_meta.status()))
            continue;

        auto entry = getTableMeta(RPCHelpers::createUUID(table_meta.uuid()));
        if (!entry)
        {
            StoragePtr table = Catalog::CatalogFactory::getTableByDataModel(getContext(), &table_meta);

            auto host_port = getContext()->getCnchTopologyMaster()->getTargetServer(UUIDHelpers::UUIDToString(table->getStorageUUID()), table->getServerVwName(), true);
            if (host_port.empty())
                continue;

            if (isLocalServer(host_port.getRPCAddress(), toString(rpc_port)))
                mayUpdateTableMeta(*table, host_port.topology_version);
        }
    }
}

inline static bool isVisible(const DB::DataModelPartWrapperPtr & part_wrapper_ptr, const UInt64 & ts)
{
    return ts == 0
        || (part_wrapper_ptr->txnID() <= ts && part_wrapper_ptr->part_model->commit_time() <= ts);
}

DB::ServerDataPartsVector PartCacheManager::getOrSetServerDataPartsInPartitions(
    const IStorage & table,
    const Strings & partitions,
    PartCacheManager::LoadPartsFunc && load_func,
    const UInt64 & ts,
    const PairInt64 & topology_version)
{
    ServerDataPartsVector res;
    mayUpdateTableMeta(table, topology_version);
    const auto & storage = dynamic_cast<const MergeTreeMetaBase &>(table);
    TableMetaEntryPtr meta_ptr = getTableMeta(table.getStorageUUID());

    if (!meta_ptr)
        return res;

    /// On cnch worker, we disable part cache to avoid cache synchronization with server.
    if (getContext()->getServerType() != ServerType::cnch_server && !dummy_mode)
    {
        DataModelPartWithNameVector fetched = load_func(partitions,  meta_ptr->getPartitionIDs());
        for (auto & ele : fetched)
        {
            auto part_wrapper_ptr = createPartWrapperFromModel(storage, std::move(*(ele->model)), std::move(ele->name));
            if (isVisible(part_wrapper_ptr, ts))
                res.push_back(std::make_shared<ServerDataPart>(part_wrapper_ptr));
        }
        return res;
    }

    if (meta_ptr->load_parts_by_partition)
        res = getServerPartsByPartition(storage, meta_ptr, partitions, load_func, ts);
    else
        res = getServerPartsInternal(storage, meta_ptr, partitions, meta_ptr->getPartitionIDs(), load_func, ts);

    return res;
}

size_t PartCacheManager::getMaxThreads() const
{
    constexpr size_t MAX_THREADS = 16;
    size_t max_threads = 1;
    if (auto thread_group = CurrentThread::getGroup())
    {
        if (auto query_context = thread_group->query_context.lock(); query_context && query_context->getSettingsRef().catalog_enable_multiple_threads)
            max_threads = query_context->getSettingsRef().max_threads;
    }
    else if (getContext()->getSettingsRef().catalog_enable_multiple_threads)
    {
        max_threads = getContext()->getSettingsRef().max_threads;
    }
    return std::min(max_threads, MAX_THREADS);
}

/* Price is higher than expected. Temporary do not log
static const size_t LOG_PARTS_SIZE = 100000;

static void logPartsVector(const MergeTreeMetaBase & storage, const ServerDataPartsVector & res)
{
    if (unlikely(res.size() % LOG_PARTS_SIZE == 0))
        LOG_DEBUG(&Poco::Logger::get("PartCacheManager"), "{} getting parts and now loaded {} parts in memory", storage.getStorageID().getNameForLogs(), res.size());
}
*/

DB::ServerDataPartsVector PartCacheManager::getServerPartsInternal(
    const MergeTreeMetaBase & storage, const TableMetaEntryPtr & meta_ptr, const Strings & partitions,
    const Strings & all_existing_partitions, PartCacheManager::LoadPartsFunc & load_func, const UInt64 & ts)
{
    UUID uuid = storage.getStorageUUID();

    auto meta_partitions = meta_ptr->getPartitions(partitions);

    auto process_partition = [&, thread_group = CurrentThread::getGroup()](const String & partition_id, const PartitionInfoPtr & partition_info_ptr, ServerDataPartsVector & parts, bool & hit_cache)
    {
        hit_cache = false;
        {
            if (partition_info_ptr->cache_status == CacheStatus::LOADED)
            {
                auto cached = part_cache_ptr->get({uuid, partition_id});
                if (cached)
                {
                    hit_cache = true;
                    for (auto it = cached->begin(); it != cached->end(); ++it)
                    {
                        const auto & part_wrapper_ptr = *it;
                        if (isVisible(part_wrapper_ptr, ts))
                        {
                            parts.push_back(std::make_shared<ServerDataPart>(part_wrapper_ptr));
                            //logPartsVector(storage, res);
                        }
                    }
                }
            }
        }
        if (!hit_cache)
        {
            auto partition_write_lock = partition_info_ptr->writeLock();
            partition_info_ptr->cache_status = CacheStatus::LOADING;
        }
    };

    ServerDataPartsVector res;
    std::unordered_map<String, bool> partitions_hit_cache;
    size_t max_threads = getMaxThreads();
    if (meta_partitions.size() < 2 || max_threads < 2)
    {
        for (auto & [partition_id, partition_info_ptr] : meta_partitions)
        {
            process_partition(partition_id, partition_info_ptr, res, partitions_hit_cache[partition_id]);
        }
    }
    else
    {
        max_threads = std::min(max_threads, meta_partitions.size());
        ExceptionHandler exception_handler;
        ThreadPool thread_pool(max_threads);
        std::map<String, ServerDataPartsVector> partition_parts;
        for (auto & [partition_id, partition_info_ptr] : meta_partitions)
        {
            partition_parts[partition_id] = ServerDataPartsVector();
            partitions_hit_cache[partition_id] = false;
        }
        for (auto & [partition_id_, partition_info_ptr_] : meta_partitions)
        {
            thread_pool.scheduleOrThrowOnError(createExceptionHandledJob(
                [&, partition_id = partition_id_, partition_info_ptr = partition_info_ptr_]() {
                    process_partition(partition_id, partition_info_ptr, partition_parts[partition_id], partitions_hit_cache[partition_id]);
                },
                exception_handler));
        }

        LOG_DEBUG(&Poco::Logger::get("PartCacheManager"), "Waiting for loading parts for table {} use {} threads.", storage.getStorageID().getNameForLogs(), max_threads);
        thread_pool.wait();
        exception_handler.throwIfException();

        size_t total_parts_number = 0;
        for (auto & [partition_id, parts] : partition_parts)
            total_parts_number += parts.size();
        res.reserve(total_parts_number);
        for (auto & [partition_id, parts] : partition_parts)
        {
            res.insert(
                res.end(),
                std::make_move_iterator(parts.begin()),
                std::make_move_iterator(parts.end()));
        }
    }

    Strings partitions_not_cached;
    for (auto & [partition_id, hit_cache] : partitions_hit_cache)
    {
        if (!hit_cache)
            partitions_not_cached.push_back(partition_id);
    }

    if (partitions_not_cached.empty())
        return res;

    auto fallback_cache_status = [&partitions_not_cached, &meta_partitions]()
    {
        // change the partitino cache status to UINIT if loading failed.
        for (auto & partition_id : partitions_not_cached)
        {
            auto & partition_info = meta_partitions[partition_id];
            auto partition_write_lock = partition_info->writeLock();
            if (partition_info->cache_status == CacheStatus::LOADING)
                partition_info->cache_status = CacheStatus::UINIT;
        }
    };

    try
    {
        /// Save data part model as well as data part to avoid build them with metaentry lock.
        std::map<String, DataModelPartWrapperVector> partition_to_parts;
        DataModelPartWithNameVector fetched = load_func(partitions_not_cached, all_existing_partitions);

        /// The load_func may include partitions that not in the required `partitions_not_cache`
        /// Need to have an extra filter
        std::unordered_set<String> partitions_set(partitions_not_cached.begin(), partitions_not_cached.end());

        for (auto & ele : fetched)
        {
            auto part_wrapper_ptr = createPartWrapperFromModel(storage, std::move(*(ele->model)), std::move(ele->name));
            const auto & partition_id = part_wrapper_ptr->info->partition_id;
            if (!partitions_set.contains(partition_id))
                continue;
            auto it = partition_to_parts.find(partition_id);
            if (it != partition_to_parts.end())
                it->second.emplace_back(part_wrapper_ptr);
            else
                partition_to_parts[partition_id] = DataModelPartWrapperVector{part_wrapper_ptr};
        }

        {
            /// merge fetched parts with that in cache;
            for (const auto & partition_id : partitions_not_cached)
            {
                auto & partition_info_ptr = meta_partitions[partition_id];
                auto partition_write_lock = partition_info_ptr->writeLock();

                /// Other other task may fail to fetch and change CacheStatus to UINIT before.
                /// If so, do not touch the cache since insert parts may be missed.
                /// Also, other task may success to fetch and change CacheStatus to LOADED before.
                /// If so, no need to modify cache again.
                if (partition_info_ptr->cache_status != CacheStatus::LOADING)
                    continue;

                DataModelPartWrapperVector * parts_wrapper_vector;
                std::optional<DataModelPartWrapperVector> empty_part_vector;
                auto it = partition_to_parts.find(partition_id);
                /// For empty partition, still need to insert an empty vector to cache
                if (it == partition_to_parts.end())
                {
                    empty_part_vector = std::make_optional<DataModelPartWrapperVector>();
                    parts_wrapper_vector =  &empty_part_vector.value();
                }
                else
                {
                    parts_wrapper_vector = &it->second;
                }

                auto cached = part_cache_ptr->get({uuid, partition_id});
                if (!cached)
                {
                    /// directly insert all fetched parts into cache.
                    cached = std::make_shared<DataPartModelsMap>();
                    for (const auto & part_wrapper_ptr : *parts_wrapper_vector)
                    {
                        cached->update(part_wrapper_ptr->name, part_wrapper_ptr);
                    }
                    part_cache_ptr->insert({uuid, partition_id}, cached);
                }
                else
                {
                    /// Its THREAD SAFE to update cache inplace
                    for (const auto & part_wrapper_ptr : *parts_wrapper_vector)
                    {
                        auto it = cached->find(part_wrapper_ptr->name);
                        /// do not update cache if the cached data is newer than kv.
                        if (it == cached->end() || (*it)->part_model->commit_time() < part_wrapper_ptr->part_model->commit_time())
                        {
                            cached->update(part_wrapper_ptr->name, part_wrapper_ptr);
                        }
                    }
                    /// Force LRU cache update status(weight/evict).
                    part_cache_ptr->insert({uuid, partition_id}, cached);
                }

                partition_info_ptr->cache_status = CacheStatus::LOADED;
            }

            /// Add fetched parts to result outside to reduce write lock time
            for (auto & [partition_id, parts_wrapper_vector] : partition_to_parts)
            {
                for (const auto & part_wrapper_ptr : parts_wrapper_vector)
                {
                    if (isVisible(part_wrapper_ptr, ts))
                    {
                        res.push_back(std::make_shared<ServerDataPart>(part_wrapper_ptr));
                        //logPartsVector(storage, res);
                    }
                }
            }
        }
    }
    catch (Exception & e)
    {
        /// fetch parts timeout, we change to fetch by partition;
        if (e.code() == ErrorCodes::TIMEOUT_EXCEEDED)
            meta_ptr->load_parts_by_partition = true;

        fallback_cache_status();
        throw;
    }
    catch (...)
    {
        fallback_cache_status();
        throw;
    }

    return res;
}

ServerDataPartsVector PartCacheManager::getServerPartsByPartition(const MergeTreeMetaBase & storage, const TableMetaEntryPtr & meta_ptr,
    const Strings & partitions, PartCacheManager::LoadPartsFunc & load_func, const UInt64 & ts)
{
    LOG_DEBUG(&Poco::Logger::get("PartCacheManager"), "Get parts by partitions for table : {}", storage.getLogName());
    Stopwatch watch;
    UUID uuid = storage.getStorageUUID();

    auto meta_partitions = meta_ptr->getPartitions(partitions);

    auto process_partition = [&](const String & partition_id, const PartitionInfoPtr & partition_info_ptr, ServerDataPartsVector & parts)
    {
        while (true)
        {
            /// stop if fetch part time exceeds the query max execution time.
            checkTimeLimit(watch);

            bool need_load_parts = false;
            {
                if (partition_info_ptr->cache_status == CacheStatus::LOADED)
                {
                    auto cached = part_cache_ptr->get({uuid, partition_id});
                    if (cached)
                    {
                        for (auto it = cached->begin(); it != cached->end(); ++it)
                        {
                            const auto & part_wrapper_ptr = *it;
                            if (isVisible(part_wrapper_ptr, ts))
                            {
                                parts.push_back(std::make_shared<ServerDataPart>(part_wrapper_ptr));
                                //logPartsVector(storage, res);
                            }
                        }

                        /// already get parts from cache, continue to next partition
                        return;
                    }
                }

                auto partition_write_lock = partition_info_ptr->writeLock();
                /// Double check
                if (partition_info_ptr->cache_status != CacheStatus::LOADING)
                {
                    partition_info_ptr->cache_status = CacheStatus::LOADING;
                    need_load_parts = true;
                }
            }

            /// Now cache status must be LOADING;
            /// need to load parts from metastore
            if (need_load_parts)
            {
                DataModelPartWithNameVector fetched;
                try
                {
                    std::map<String, DataModelPartWrapperVector> partition_to_parts;
                    fetched = load_func({partition_id}, {partition_id});
                    DataModelPartWrapperVector fetched_data;
                    for (auto & ele : fetched)
                    {
                        fetched_data.push_back(createPartWrapperFromModel(storage, std::move(*(ele->model)), std::move(ele->name)));
                    }

                    /// It happens that new parts have been inserted into cache during loading parts from bytekv, we need merge them to make
                    /// sure the cache contains all parts of the partition.
                    auto partition_write_lock = partition_info_ptr->writeLock();
                    auto cached = part_cache_ptr->get({uuid, partition_id});
                    if (!cached)
                    {
                        /// directly insert all fetched parts into cache
                        cached = std::make_shared<DataPartModelsMap>();
                        for (auto & data_wrapper_ptr : fetched_data)
                        {
                            cached->update(data_wrapper_ptr->name, data_wrapper_ptr);
                        }
                        part_cache_ptr->insert({uuid, partition_id}, cached);
                    }
                    else
                    {
                        for (auto & data_wrapper_ptr : fetched_data)
                        {
                            auto it = cached->find(data_wrapper_ptr->name);
                            /// do not update cache if the cached data is newer than bytekv.
                            if (it == cached->end() || (*it)->part_model->commit_time() < data_wrapper_ptr->part_model->commit_time())
                            {
                                cached->update(data_wrapper_ptr->name, data_wrapper_ptr);
                            }
                        }
                        /// Force LRU cache update status(weight/evict).
                        part_cache_ptr->insert({uuid, partition_id}, cached);
                    }

                    partition_info_ptr->cache_status = CacheStatus::LOADED;

                    /// Release partition lock before construct ServerDataPart
                    partition_write_lock.reset();

                    /// Finish fetching parts, notify other waiting tasks if any.
                    meta_ptr->fetch_cv.notify_all();

                    for (auto & data_wrapper_ptr : fetched_data)
                    {
                        /// Only filter the parts when both commit_time and txnid are smaller or equal to ts (txnid is helpful for intermediate parts).
                        if (isVisible(data_wrapper_ptr, ts))
                        {
                            parts.push_back(std::make_shared<ServerDataPart>(data_wrapper_ptr));
                            //logPartsVector(storage, res);
                        }
                    }

                    /// go to next partition;
                    return;
                }
                catch (...)
                {
                    /// change cache status to UINIT if exception occurs during fetch.
                    auto partition_write_lock = partition_info_ptr->writeLock();
                    partition_info_ptr->cache_status = CacheStatus::UINIT;
                    throw;
                }
            }
            else /// other task is fetching parts now, just wait for the result
            {
                {
                    std::unique_lock<std::mutex> lock(meta_ptr->fetch_mutex);
                    if (!meta_ptr->fetch_cv.wait_for(lock, std::chrono::milliseconds(5000)
                        , [&partition_info_ptr]() {return partition_info_ptr->cache_status == CacheStatus::LOADED;}))
                    {
                        LOG_TRACE(&Poco::Logger::get("PartCacheManager"), "Wait timeout 5000ms for other thread loading table: {}, partition: {}", storage.getStorageID().getNameForLogs(), partition_id);
                        continue;
                    }
                }

                if (partition_info_ptr->cache_status == CacheStatus::LOADED)
                {
                    auto cached = part_cache_ptr->get({uuid, partition_id});
                    if (!cached)
                    {
                        throw Exception("Cannot get already loaded parts from cache. Its a logic error.", ErrorCodes::LOGICAL_ERROR);
                    }
                    for (auto it = cached->begin(); it != cached->end(); ++it)
                    {
                        const auto & part_wrapper_ptr = *it;
                        /// Only filter the parts when both commit_time and txnid are smaller or equal to ts (txnid is helpful for intermediate parts).
                        if (isVisible(part_wrapper_ptr, ts))
                        {
                            parts.push_back(std::make_shared<ServerDataPart>(part_wrapper_ptr));
                            //logPartsVector(storage, res);
                        }
                    }
                    return;
                }
                // if cache status does not change to loaded, get parts of current partition again.
            }
        }
    };

    ServerDataPartsVector res;

    size_t max_threads = getMaxThreads();
    if (meta_partitions.size() < 2 || max_threads < 2)
    {
        for (auto & [partition_id, partition_info_ptr] : meta_partitions)
        {
            process_partition(partition_id, partition_info_ptr, res);
        }
    }
    else
    {
        max_threads = std::min(max_threads, meta_partitions.size());
        ExceptionHandler exception_handler;
        ThreadPool thread_pool(max_threads);
        std::map<String, ServerDataPartsVector> partition_parts;
        for (auto & [partition_id, partition_info_ptr] : meta_partitions)
        {
            partition_parts[partition_id] = ServerDataPartsVector();
        }
        for (auto & [partition_id_, partition_info_ptr_] : meta_partitions)
        {
            thread_pool.scheduleOrThrowOnError(createExceptionHandledJob(
                [&, partition_id = partition_id_, partition_info_ptr = partition_info_ptr_]() {
                    process_partition(partition_id, partition_info_ptr, partition_parts[partition_id]);
                },
                exception_handler));
        }

        LOG_DEBUG(&Poco::Logger::get("PartCacheManager"), "Waiting for loading parts for table {} use {} threads.", storage.getStorageID().getNameForLogs(), max_threads);
        thread_pool.wait();
        exception_handler.throwIfException();

        size_t total_parts_number = 0;
        for (auto & [partition_id, parts] : partition_parts)
            total_parts_number += parts.size();
        res.reserve(total_parts_number);
        for (auto & [partition_id, parts] : partition_parts)
        {
            res.insert(
                res.end(),
                std::make_move_iterator(parts.begin()),
                std::make_move_iterator(parts.end()));
        }
    }

    return res;
}

void PartCacheManager::checkTimeLimit(Stopwatch & watch)
{
    auto check_max_time = [this, &watch]()
    {
        auto context = this->getContext();
        /// Add parts get time check for background threads
        if (context->getSettingsRef().cnch_background_task_part_load_max_seconds
            && watch.elapsedSeconds() > context->getSettingsRef().cnch_background_task_part_load_max_seconds)
        {
            throw Exception("Get parts timeout over " + std::to_string(context->getSettingsRef().cnch_background_task_part_load_max_seconds) + "s."
                , ErrorCodes::TIMEOUT_EXCEEDED);
        }
    };

    if (CurrentThread::getQueryId().toString().empty())
    {
        check_max_time();
        return;
    }

    auto thread_group = CurrentThread::getGroup();

    if (!thread_group)
    {
        check_max_time();
        return;
    }

    if (auto query_context = thread_group->query_context.lock())
    {
        if (query_context->getSettingsRef().max_execution_time.totalSeconds() < watch.elapsedSeconds())
        {
            throw Exception("Get parts timeout over query max execution time.", ErrorCodes::TIMEOUT_EXCEEDED);
        }
    }
    else
    {
        check_max_time();
    }
}

std::pair<UInt64, UInt64> PartCacheManager::dumpPartCache()
{
    std::unique_lock<std::mutex> lock(cache_mutex);
    return {part_cache_ptr->count(), part_cache_ptr->weight()};
}

std::pair<UInt64, UInt64> PartCacheManager::dumpStorageCache()
{
    std::unique_lock<std::mutex> lock(cache_mutex);
    return {storageCachePtr->count(), storageCachePtr->weight()};
}

std::unordered_map<String, std::pair<size_t, size_t>> PartCacheManager::getTableCacheInfo()
{
    CnchDataPartCachePtr cache_ptr;
    {
        std::unique_lock<std::mutex> lock(cache_mutex);
        cache_ptr = part_cache_ptr;
    }
    if (!cache_ptr)
        return {};

    return cache_ptr->getTableCacheInfo();
}

void PartCacheManager::reset()
{
    LOG_DEBUG(&Poco::Logger::get("PartCacheManager::reset"), "Resetting part cache manager.");
    std::unique_lock<std::mutex> lock(cache_mutex);
    {
        /// 1. Remember the current size of the active_tables.
        /// 2. Put all data into the trashcan.
        /// 3. Clear `active_tables`.
        /// 4. Notify these amount `TableMetaEntry` to be destructed.
        ///   (to accelerate the destruction of `TableMetaEntry`)
        ///
        /// As a side effect, `it` will be updated to the next element.
        ///
        /// Lock will be held to make sure trash always have the last reference counter.
        size_t num_of_active_tables = active_tables.size();
        std::unique_lock<std::mutex> lock_of_trashcan(trashed_active_tables_mutex);
        transform(
            active_tables.begin(),
            active_tables.end(),
            std::back_inserter(trashed_active_tables),
            [](std::unordered_map<UUID, TableMetaEntryPtr>::value_type val) { return val.second; });
        active_tables.clear();
        /// [ 0 | ...| i | i + 1 (last new element) | ... | i + n, rbegin() ]
        std::list<TableMetaEntryPtr>::reverse_iterator it = trashed_active_tables.rbegin();
        while (num_of_active_tables > 0 && it != trashed_active_tables.rend())
        {
            (*it)->forEachPartition([](PartitionInfoPtr ptr) {
                if (ptr->metrics_ptr)
                    ptr->metrics_ptr->notifyShutDown();
            });

            num_of_active_tables--;
            it++;
        }
    }

    part_cache_ptr->reset();
    storageCachePtr->reset();
    /// reload active tables when topology change.
    if (!dummy_mode)
        active_table_loader->schedule();
}

size_t PartCacheManager::cleanTrashedActiveTables() {
    size_t count = 0;
    while (true)
    {
        TableMetaEntryPtr cur;
        {
            std::unique_lock<std::mutex> lock(trashed_active_tables_mutex);
            if (trashed_active_tables.size() == 0)
            {
                return count;
            }
            cur = trashed_active_tables.front();
            trashed_active_tables.pop_front();
        }

        /// The `TableMetaEntryPtr` will be released at the end
        /// of the scope without holding the lock.
        count++;
    }
}

void PartCacheManager::shutDown()
{
    LOG_DEBUG(&Poco::Logger::get("PartCacheManager::shutdown"), "Shutdown method of part cache manager called.");
    table_partition_metrics.shutDown(this);
    active_table_loader->deactivate();
    meta_lock_cleaner->deactivate();
    trashed_active_tables_cleaner->deactivate();
}

StoragePtr PartCacheManager::getStorageFromCache(const UUID & uuid, const PairInt64 & topology_version)
{
    StoragePtr res;
    TableMetaEntryPtr table_entry = getTableMeta(uuid);
    if (table_entry && topology_version == table_entry->cache_version.get())
        res = storageCachePtr->get(uuid);
    return res;
}

void PartCacheManager::insertStorageCache(const StorageID & storage_id, const StoragePtr storage, const UInt64 commit_ts, const PairInt64 & topology_version)
{
    TableMetaEntryPtr table_entry = getTableMeta(storage_id.uuid);
    if (table_entry && topology_version == table_entry->cache_version.get())
        storageCachePtr->insert(storage_id, commit_ts, storage);
}

void PartCacheManager::removeStorageCache(const String & database, const String & table)
{
    if (!database.empty() && !table.empty())
        storageCachePtr->remove(database, table);
    else if (!database.empty())
        storageCachePtr->remove(database);
    else
        storageCachePtr->reset();
}

std::unordered_map<UUID, TableMetaEntryPtr> PartCacheManager::getTablesSnapshot()
{
    std::unordered_map<UUID, TableMetaEntryPtr> tables_snapshot;
    {
        std::unique_lock<std::mutex> lock(cache_mutex);
        tables_snapshot = active_tables;
    }
    return tables_snapshot;
}
std::shared_ptr<TableMetrics> PartCacheManager::getTrashItemsInfoMetrics(const IStorage & i_storage)
{
    return table_partition_metrics.getTrashItemsInfoMetrics(i_storage);
}
void PartCacheManager::updateTrashItemsMetrics(const UUID & table_uuid, const Catalog::TrashItems & items, bool positive)
{
    if (auto table_meta_ptr = getTableMeta(table_uuid))
        table_partition_metrics.updateMetrics(items, table_meta_ptr->trash_item_metrics, positive);
}
bool PartCacheManager::forceRecalculate(StoragePtr table)
{
    if (!table)
    {
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Receive a empty table.");
    }

    const auto host_port = getContext()->getCnchTopologyMaster()->getTargetServer(
        UUIDHelpers::UUIDToString(table->getStorageUUID()), table->getServerVwName(), true);
    auto * log = &Poco::Logger::get("PartCacheManager::forceRecalculate");
    if (!host_port.empty() && !isLocalServer(host_port.getRPCAddress(), std::to_string(getContext()->getRPCPort())))
    {
        try
        {
            LOG_DEBUG(log, "Force recalculate table {} on {}", table->getStorageID().getNameForLogs(), host_port.toDebugString());

            getContext()->getCnchServerClientPool().get(host_port)->forceRecalculateMetrics(table->getStorageID());
            return true;
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }

        return true;
    }

    LOG_DEBUG(log, "Force recalculate table {}", table->getStorageID().getNameForLogs());
    auto current_time = getContext()->getTimestamp();
    auto table_meta = getTableMeta(table->getStorageUUID());
    if (table_meta)
    {
        table_partition_metrics.recalculateOrSnapshotPartitionsMetrics(table_meta, current_time, true);
        return true;
    }
    return false;
}

std::vector<Protos::LastModificationTimeHint>
PartCacheManager::getLastModificationTimeHints(const ConstStoragePtr & storage, const bool allow_regression)
{
    if (!storage)
    {
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Receive a empty table.");
    }
    auto rpc_port = getContext()->getRPCPort();
    auto host_port = getContext()->getCnchTopologyMaster()->getTargetServer(
        UUIDHelpers::UUIDToString(storage->getStorageUUID()), storage->getServerVwName(), true);
    if (host_port.empty())
        return {};

    if (isLocalServer(host_port.getRPCAddress(), toString(rpc_port)))
        mayUpdateTableMeta(*storage, host_port.topology_version);
    else
        return {};

    std::vector<Protos::LastModificationTimeHint> ret;
    auto table_meta = getTableMeta(storage->getStorageUUID());
    time_t now = time(nullptr);
    if (table_meta)
    {
        using RetType = std::optional<std::pair<PartitionMetrics::PartitionMetricsStore, bool>>;
        /// Return a partition metrics snapshot.
        std::function<RetType(std::shared_ptr<CnchPartitionInfo>)> load_func;
        if (table_meta->partition_metrics_loaded)
        {
            load_func = [](std::shared_ptr<CnchPartitionInfo> partition_info) -> RetType {
                bool finish_first_recalculation = partition_info->metrics_ptr->finishFirstRecalculation();
                return std::make_optional(std::make_pair(partition_info->metrics_ptr->read(), finish_first_recalculation));
            };
        }
        else
        {
            std::unordered_map<String, std::shared_ptr<PartitionMetrics>> snapshots
                = getContext()->getCnchCatalog()->loadPartitionMetricsSnapshotFromMetastore(
                    UUIDHelpers::UUIDToString(storage->getStorageUUID()));
            load_func = [snapshots = std::move(snapshots)](std::shared_ptr<CnchPartitionInfo> partition_info) -> RetType {
                auto it = snapshots.find(partition_info->partition_id);
                if (it != snapshots.end())
                    return std::make_optional(std::make_pair(it->second->read(), false));
                else
                    return std::nullopt;
            };
        }

        auto & meta_partitions = table_meta->partitions;
        ret.reserve(meta_partitions.size());
        for (auto it = meta_partitions.begin(); it != meta_partitions.end(); it++)
        {
            Protos::LastModificationTimeHint hint = Protos::LastModificationTimeHint{};

            const auto * meta_storage = dynamic_cast<const StorageCnchMergeTree *>(storage.get());
            if (!meta_storage)
                throw Exception("Table is not a Meta Based MergeTree", ErrorCodes::UNKNOWN_TABLE);
            String partition;
            {
                WriteBufferFromString write_buffer(partition);
                (*it)->partition_ptr->store(*meta_storage, write_buffer);
            }

            // Skip if it passes TTL
            auto ttl = meta_storage->getTTLForPartition(*(*it)->partition_ptr);

            if (ttl && ttl < now) {
                continue;
            }

            hint.set_partition_id(partition);

            std::optional<std::pair<PartitionMetrics::PartitionMetricsStore, bool>> data = load_func(*it);

            if (!data.has_value() || (data->first.total_parts_number < 0 || data->first.total_rows_count < 0))
            {
                LOG_WARNING(
                    &Poco::Logger::get("PartCacheManager::getLastModificationTimeHints"),
                    "Can not get partition metrics for partition {} from snapshots.",
                    partition);
                hint.set_last_modification_time(0);
            }
            else if (!allow_regression && !data->second)
            {
                /// not a accurate metrics, so return 0.
                hint.set_last_modification_time(0);
            }
            else if (data->first.total_parts_number == 0 && data->first.total_rows_count == 0)
            {
                continue;
            }
            else
            {
                hint.set_last_modification_time(data->first.last_modification_time);
            }

            ret.push_back(std::move(hint));
        }
    }

    return ret;
}
}
