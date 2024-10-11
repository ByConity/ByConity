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

#include <Protos/data_models.pb.h>
#include <Storages/PartCacheManager.h>

#include <chrono>
#include <iterator>
#include <Catalog/Catalog.h>
#include <Catalog/CatalogFactory.h>
#include <CloudServices/CnchDataAdapter.h>
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
#include "CloudServices/CnchPartsHelper.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_STORAGE;
    extern const int TIMEOUT_EXCEEDED;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
    extern const int BAD_ARGUMENTS;
}

/// Mock function for `getTimestamp`/`tryGetTimestamp`.
static constexpr auto dummy_get_timestamp = []() -> UInt64 {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
};

template <class T> struct DependentFalse : std::false_type {};

PartCacheManager::PartCacheManager(ContextMutablePtr context_, const size_t memory_limit, const bool dummy_mode)
    : WithMutableContext(context_), dummy_mode(dummy_mode), table_partition_metrics(context_)
{
    // Equals to max(100000, 0.03% of memory_limit) unless manually set in config.
    // A part is estimated to be 1500 bytes.
    size_t size_of_cached_parts = 100000;
    if (getContext()->getConfigRef().has("size_of_cached_parts"))
    {
        size_of_cached_parts = getContext()->getConfigRef().getUInt("size_of_cached_parts", 100000);
    }
    else
    {
        size_of_cached_parts = std::max(size_of_cached_parts, static_cast<size_t>(memory_limit * 0.03 / 1500));
    }
    // Equals to max(500000, 0.02% of memory_limit) unless manually set in config.
    // A delete bitmap is estimated to be 200 bytes.
    size_t size_of_cached_delete_bitmaps = 500000;
    if (getContext()->getConfigRef().has("size_of_cached_delete_bitmaps"))
    {
        size_of_cached_delete_bitmaps = getContext()->getConfigRef().getUInt("size_of_cached_delete_bitmaps", 500000);
    }
    else
    {
        size_of_cached_delete_bitmaps = std::max(size_of_cached_parts, static_cast<size_t>(memory_limit * 0.02 / 200));
    }
    size_t size_of_cached_storage = getContext()->getConfigRef().getUInt("cnch_max_cached_storage", 10000);
    size_t data_cache_min_lifetime = getContext()->getConfigRef().getUInt("data_cache_min_lifetime", 1800);
    LOG_DEBUG(
        getLogger("PartCacheManager"),
        "Memory limit is {} bytes, Part cache size is {},  delete bitmap size is {}, storage cache size is {} (in unit).",
        memory_limit,
        size_of_cached_parts,
        size_of_cached_delete_bitmaps,
        size_of_cached_storage);
    part_cache_ptr = std::make_shared<CnchDataPartCache>(size_of_cached_parts, std::chrono::seconds(data_cache_min_lifetime));
    storageCachePtr = std::make_shared<CnchStorageCache>(size_of_cached_storage);
    delete_bitmap_cache_ptr = std::make_shared<CnchDeleteBitmapCache>(size_of_cached_delete_bitmaps, std::chrono::seconds(data_cache_min_lifetime));
    meta_lock_cleaner = getContext()->getSchedulePool().createTask("MetaLockCleaner", [this]() {
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

void PartCacheManager::mayUpdateTableMeta(const IStorage & storage, const PairInt64 & topology_version, const bool on_table_creation)
{
    /* Fetches partitions from metastore if storage is not present in active_tables*/

    /// Only handle MergeTree tables
    const auto * cnch_table = dynamic_cast<const MergeTreeMetaBase*>(&storage);
    if (!cnch_table)
        return;

    auto load_table_partitions = [&](TableMetaEntryPtr & meta_ptr) -> bool
    {
        bool meta_loaded = false;
        auto table_lock = meta_ptr->writeLock();
        /// If other thread finished load, just return
        if (meta_ptr->cache_status == CacheStatus::LOADED)
            return false;
        /// Invalid old cache if any
        part_cache_ptr->dropCache(storage.getStorageUUID());
        delete_bitmap_cache_ptr->dropCache(storage.getStorageUUID());
        storageCachePtr->remove(storage.getDatabaseName(), storage.getTableName());

        try
        {
            meta_ptr->cache_status = CacheStatus::LOADING;
            // No need to load from catalog in dummy mode.
            if (likely(!dummy_mode))
            {
                getContext()->getCnchCatalog()->getPartitionsFromMetastore(*cnch_table, meta_ptr->partitions, meta_ptr->lock_holder);
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
            tryLogCurrentException(getLogger("PartCacheManager::mayUpdateTableMeta"));
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
                    meta_lock_it->second,
                    on_table_creation);
            }
            else
            {
                meta_ptr = std::make_shared<TableMetaEntry>(
                    storage.getDatabaseName(),
                    storage.getTableName(),
                    UUIDHelpers::UUIDToString(storage.getStorageUUID()),
                    nullptr,
                    on_table_creation);
                /// insert the new meta lock into lock container.
                meta_lock_container.emplace(uuid, meta_ptr->lock_holder);
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
                    LOG_DEBUG(getLogger("PartCacheManager::MetaEntry"), "Invalid part cache because of cache version mismatch for table {}.{}", meta_ptr->database, meta_ptr->table);
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
                tryLogCurrentException(getLogger("PartCacheManager::mayUpdateTableMeta"));
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
            LOG_DEBUG(getLogger("PartCacheManager::getTableMeta"), "invalid part cache for {}. NHUT is {}", UUIDHelpers::UUIDToString(uuid), table_entry->cached_non_host_update_ts);
            invalidPartAndDeleteBitmapCache(uuid);
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
        LOG_TRACE(getLogger("PartCacheManager::getTableMeta"), "Table id {} not found in active_tables", UUIDHelpers::UUIDToString(uuid));
        return nullptr;
    }

    return active_tables[uuid];
}

std::vector<TableMetaEntryPtr> PartCacheManager::getAllActiveTables()
{
    std::vector<TableMetaEntryPtr> res;
    std::unique_lock<std::mutex> lock(cache_mutex);
    res.reserve(active_tables.size());
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

void PartCacheManager::setTableClusterStatus(const UUID & uuid, const bool clustered, const TableDefinitionHash & table_definition_hash)
{
    TableMetaEntryPtr table_entry = getTableMeta(uuid);
    if (table_entry)
    {
        auto lock = table_entry->writeLock();
        table_entry->is_clustered = clustered;
        table_entry->table_definition_hash = table_definition_hash;
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

std::pair<Int64, Int64> PartCacheManager::getTotalAndMaxPartsNumber(const IStorage & storage)
{
    Int64 total_parts{0};
    Int64 max_parts_in_partition{0};

    TableMetaEntryPtr table_entry = getTableMeta(storage.getStorageUUID());
    if (table_entry)
    {
        auto & table_partitions = table_entry->partitions;
        for (auto it = table_partitions.begin(); it != table_partitions.end(); it++)
        {
            PartitionFullPtr partition_ptr = std::make_shared<CnchPartitionInfoFull>(*it);
            const auto & partition_data = partition_ptr->partition_info_ptr->metrics_ptr->read();
            bool is_validate = partition_data.validateMetrics();
            if (!is_validate)
            {
                throw Exception(ErrorCodes::INCORRECT_DATA, "Parts info in PartCacheManager is not available now");
            }

            total_parts += partition_data.total_parts_number;
            max_parts_in_partition = std::max(max_parts_in_partition, partition_data.total_parts_number);
        }
    }

    return {total_parts, max_parts_in_partition};
}

Catalog::PartitionMap PartCacheManager::updatePartitionGCTime(const StoragePtr table, const Strings & partitions, UInt32 ts)
{
    TableMetaEntryPtr meta_ptr = getTableMeta(table->getStorageUUID());

    // return directly if the table has not been loaded
    if (!meta_ptr)
        return {};

    Catalog::PartitionMap res;

    auto partition_map = meta_ptr->getPartitions(partitions);
    for (const auto & p : partitions)
    {
        if (auto it = partition_map.find(p); it != partition_map.end())
        {
            // mark partition as deleted
            if (ts)
            {
                UInt64 expected = 0;
                if (it->second->gctime.compare_exchange_strong(expected, ts, std::memory_order_relaxed))
                    res.emplace(it->first, it->second);
            }
            else
            {
                // reset deleting status if ts=0
                it->second->gctime = 0;
            }
        }
    }
    return res;
}

void PartCacheManager::removeDeletedPartitions(const StoragePtr table, const Strings & partitions)
{
    TableMetaEntryPtr meta_ptr = getTableMeta(table->getStorageUUID());
    // return directly if the table has not been loaded
    if (!meta_ptr)
        return;

    Strings keys_to_remove;
    auto partition_map = meta_ptr->getPartitions(partitions);
    for (const String & partition : partitions)
    {
        if (auto it = partition_map.find(partition); it != partition_map.end() && it->second->gctime != 0)
            keys_to_remove.emplace_back(partition);
    }

    meta_ptr->partitions.erase_if(keys_to_remove, [](PartitionInfoPtr & info){ return info->gctime > 0;});
}

std::unordered_set<String> PartCacheManager::getDeletingPartitions(const StoragePtr table)
{
    TableMetaEntryPtr meta_ptr = getTableMeta(table->getStorageUUID());
    // return directly if the table has not been loaded
    if (!meta_ptr)
        return {};
    else
        return meta_ptr->getDeletingPartitions();
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

bool PartCacheManager::getPartitionInfo(const IStorage & storage, Catalog::PartitionMap & partitions, const PairInt64 & topology_version, const Strings & required_partitions)
{
    mayUpdateTableMeta(storage, topology_version);
    TableMetaEntryPtr meta_ptr = getTableMeta(storage.getStorageUUID());

    if (meta_ptr)
    {
        partitions = meta_ptr->getPartitions(required_partitions);
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

void PartCacheManager::invalidPartAndDeleteBitmapCache(const UUID & uuid, bool skip_part_cache, bool skip_delete_bitmap_cache)
{
    std::unique_lock<std::mutex> lock(cache_mutex);
    invalidPartCacheWithoutLock(uuid, lock, skip_part_cache, skip_delete_bitmap_cache);
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
            LOG_DEBUG(getLogger("PartCacheManager::invalidCacheWithNewTopology"), "Dropping part cache of {}", UUIDHelpers::UUIDToString(it->first));
            part_cache_ptr->dropCache(it->first);
            delete_bitmap_cache_ptr->dropCache(it->first);
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

void PartCacheManager::invalidPartCacheWithoutLock(
    const UUID & uuid, std::unique_lock<std::mutex> &, bool skip_part_cache, bool skip_delete_bitmap_cache)
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
    LOG_DEBUG(getLogger("PartCacheManager::invalidPartCacheWithoutLock"), "Dropping part cache of {}", UUIDHelpers::UUIDToString(uuid));
    if (!skip_part_cache)
        part_cache_ptr->dropCache(uuid);
    if (!skip_delete_bitmap_cache)
        delete_bitmap_cache_ptr->dropCache(uuid);
}

template <typename DataCachePtr>
void PartCacheManager::invalidDataCache(
    const UUID & uuid,
    const TableMetaEntryPtr & meta_ptr,
    const std::unordered_map<String, Strings> & partition_to_data_list,
    DataCachePtr cache_ptr)
{
    Strings partitions;
    for (auto & [partition_id, data] : partition_to_data_list)
        partitions.push_back(partition_id);

    auto meta_partitions = meta_ptr->getPartitions(partitions);

    for (const auto & partition_to_data : partition_to_data_list)
    {
        auto & partition_id = partition_to_data.first;
        auto cached = cache_ptr->get({uuid, partition_id});
        if (!cached)
            continue;
        // cannot find partition info, directly drop the data cache of this partition
        if (!meta_partitions.contains(partition_id))
        {
            cache_ptr->remove({uuid, partition_id});
            continue;
        }

        auto partition_info_ptr = meta_partitions[partition_id];
        DataCacheStatus * cache_status = getCacheStatus<DataCachePtr>(partition_info_ptr);
        auto partition_write_lock = partition_info_ptr->writeLock();
        if (cache_status->isLoaded())
        {
            for (const auto & name : partition_to_data.second)
            {
                auto got = cached->find(name);
                if (got != cached->end())
                    cached->erase(name);
            }
        }
        // reset cache status to UInit because other loading threads may load outdated data when deleing happens during the loading.
        else if (cache_status->isLoading())
        {
            cache_status->reset();
            cache_ptr->remove({uuid, partition_id});
        }
    }
}


template <typename Ds, typename Adapter>
void PartCacheManager::invalidDataCache(const UUID & uuid, const Ds & xs)
{
    static_assert(std::is_base_of_v<AdapterInterface, Adapter> == true, "Adapter should be bound by AdapterInterface.");


    TableMetaEntryPtr meta_ptr = getTableMeta(uuid);

    if (!meta_ptr)
        return;
    std::unordered_map<String, Strings> partition_to_data;
    for (auto & x : xs)
    {
        Adapter x_adapter(x);
        const String & partition_id = x_adapter.getPartitionId();
        auto it = partition_to_data.find(partition_id);
        if (it != partition_to_data.end())
        {
            it->second.emplace_back(x_adapter.getName());
        }
        else
        {
            Strings data_list{x_adapter.getName()};
            partition_to_data.emplace(partition_id, data_list);
        }
    }

    if constexpr (std::is_same_v<Adapter, DeleteBitmapAdapter>)
    {
        invalidDataCache(uuid, meta_ptr, partition_to_data, delete_bitmap_cache_ptr);
    }
    else if constexpr (
        std::is_same_v<Adapter, DataPartAdapter> || std::is_same_v<Adapter, ServerDataPartAdapter>
        || std::is_same_v<Adapter, IMergeTreeDataPartAdapter>)
    {
        invalidDataCache(uuid, meta_ptr, partition_to_data, part_cache_ptr);
    }
    else
    {
        static_assert(DependentFalse<Adapter>::value, "invalid template type for Adapter");
    }
}

/// bsp retry assumes worker wont hold part cache, thus only invalidate part cache in host server before retry plan segment instance

void PartCacheManager::invalidPartCache(const UUID & uuid, const DataPartsVector & parts)
{
    invalidDataCache<DataPartsVector, DataPartAdapter>(uuid, parts);
}

void PartCacheManager::invalidPartCache(const UUID & uuid, const ServerDataPartsVector & parts)
{
    invalidDataCache<ServerDataPartsVector, ServerDataPartAdapter>(uuid, parts);
}

void PartCacheManager::invalidPartCache(const UUID & uuid, const IMergeTreeDataPartsVector & parts)
{
    invalidDataCache<IMergeTreeDataPartsVector, IMergeTreeDataPartAdapter>(uuid, parts);
}

void PartCacheManager::invalidDeleteBitmapCache(const UUID & uuid, const DeleteBitmapMetaPtrVector & parts)
{
    invalidDataCache<DeleteBitmapMetaPtrVector, DeleteBitmapAdapter>(uuid, parts);
}

template <typename Adapter, typename InputValueVec, typename ValueVec, typename CachePtr, typename CacheValueMap, typename GetKeyFunc, bool insert_into_cache>
void PartCacheManager::insertDataIntoCache(
    const IStorage & table,
    const InputValueVec & parts_model,
    const bool is_merged_parts,
    const bool should_update_metrics,
    const PairInt64 & topology_version,
    CachePtr cache_ptr,
    GetKeyFunc func,
    [[maybe_unused]] const std::unordered_map<String, String> * extra_partition_info)
{
    /// Only cache MergeTree tables
    if (!dynamic_cast<const MergeTreeMetaBase *>(&table))
        return;

    mayUpdateTableMeta(table, topology_version);
    UUID uuid = table.getStorageUUID();
    TableMetaEntryPtr meta_ptr = getTableMeta(uuid);
    if (!meta_ptr)
    {
        throw Exception("Table is not initialized before save parts into cache.", ErrorCodes::UNKNOWN_STORAGE);
    }

    /// Do all parse jobs outside lock
    std::unordered_map<String, ValueVec> partitionid_to_data_list;
    std::unordered_map<String, std::shared_ptr<MergeTreePartition>> partitionid_to_partition;
    const auto & storage = dynamic_cast<const MergeTreeMetaBase &>(table);
    for (const auto & part_model : parts_model)
    {
        Adapter data_adapter(storage, std::move(part_model));
        const auto & partition_id = data_adapter.getPartitionId();

        /// Only parts contains partition_minmax info by themselves.
        if constexpr (std::is_same_v<CachePtr, CnchDataPartCachePtr>)
        {
            if (!partitionid_to_partition.contains(partition_id))
                partitionid_to_partition[partition_id] = createPartitionFromMetaString(storage, data_adapter.getPartitionMinmax());
        }
        else if constexpr (!std::is_same_v<CachePtr, CnchDeleteBitmapCachePtr>)
        {
            static_assert(DependentFalse<CachePtr>::value, "invalid template type for CachePtr");
        }

        auto it = partitionid_to_data_list.find(partition_id);
        if (it != partitionid_to_data_list.end())
            it->second.emplace_back(data_adapter.getData());
        else
            partitionid_to_data_list[partition_id] = ValueVec{data_adapter.getData()};
    }

    /// For delete bitmaps, we need to use extra_partition_info to get partition_minmax.
    if (extra_partition_info)
    {
        for (const auto & [partition_id, partition_minmax] : *extra_partition_info)
        {
            if (!partitionid_to_partition.contains(partition_id))
            {
                partitionid_to_partition[partition_id] = createPartitionFromMetaString(storage, partition_minmax);
            }
        }
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
    partition_ids.reserve(partitionid_to_data_list.size());
    for (const auto & pair : partitionid_to_data_list)
        partition_ids.push_back(pair.first);
    auto meta_partitions = meta_ptr->getPartitions(partition_ids);

    /// Create when there are new partitions
    if (meta_partitions.size() < partitionid_to_data_list.size())
    {
        auto & partitions = meta_ptr->partitions;
        for (auto & p : partitionid_to_data_list)
        {
            const auto & partition_id = p.first;
            if (meta_partitions.contains(partition_id) || !partitionid_to_partition.contains(partition_id))
                continue;
            auto it = partitions
                          .emplace(
                              partition_id,
                              std::make_shared<CnchPartitionInfo>(
                                  UUIDHelpers::UUIDToString(uuid),
                                  partitionid_to_partition[partition_id],
                                  partition_id,
                                  meta_ptr->lock_holder->getPartitionLock(partition_id),
                                  true))
                          .first;
            meta_partitions.emplace(partition_id, *it);
        }
    }

    if (!insert_into_cache)
        return;

    for (auto & [partition_id, data_wrapper_vector] : partitionid_to_data_list)
    {
        if constexpr (std::is_same_v<CachePtr, CnchDeleteBitmapCachePtr>)
        {
            /// For delete bitmaps, partition_info_ptr are not always exists.
            /// It's ok to skip since the cache is not loaded anyway.
            if (!meta_partitions.contains(partition_id))
            {
                continue;
            }
        }
        else if constexpr (!std::is_same_v<CachePtr, CnchDataPartCachePtr>)
        {
            static_assert(DependentFalse<CachePtr>::value, "invalid template type for CachePtr");
        }
        auto & partition_info_ptr = meta_partitions[partition_id];
        DataCacheStatus * cache_status = nullptr;
        if constexpr (std::is_same_v<CachePtr, CnchDataPartCachePtr>)
        {
            cache_status = &partition_info_ptr->part_cache_status;
        }
        else if constexpr (std::is_same_v<CachePtr, CnchDeleteBitmapCachePtr>)
        {
            cache_status = &partition_info_ptr->delete_bitmap_cache_status;
        }
        else
        {
            static_assert(DependentFalse<CachePtr>::value, "invalid template type for CachePtr");
        }

        auto partition_write_lock = partition_info_ptr->writeLock();
        bool need_insert_into_cache = false;
        /// Check if new parts should be inserted into cache; Skip if cache status is UINIT
        if (!cache_status->isUnInit())
        {
            need_insert_into_cache = true;
            /// Check whether the partition cache has been invalidate by LRU.
            if (cache_status->isLoaded())
            {
                auto cached = cache_ptr->get({uuid, partition_id});
                if (!cached)
                {
                    cache_status->reset();
                    need_insert_into_cache = false;
                }
            }
        }

        if (need_insert_into_cache)
        {
            auto cached = cache_ptr->get({uuid, partition_id});
            if (!cached)
            {
                /// directly insert all new parts into cache.
                cached = std::make_shared<CacheValueMap>();
                cached->insert(data_wrapper_vector, std::move(func));
                cache_ptr->insert({uuid, partition_id}, cached);
            }
            else
            {
                cached->insert(data_wrapper_vector, std::move(func));
                /// Force LRU cache update status (weight/evict).
                cache_ptr->insert({uuid, partition_id}, cached);
            }
        }
        if constexpr (std::is_same_v<CachePtr, CnchDataPartCachePtr>)
        {
            if (should_update_metrics)
            {
                table_partition_metrics.updateMetrics(data_wrapper_vector, partition_info_ptr->metrics_ptr, uuid, ts);
            }
        }
    }

    if constexpr (std::is_same_v<CachePtr, CnchDataPartCachePtr>)
    {
        if (!is_merged_parts)
            meta_ptr->last_update_time = (ts == TxnTimestamp::maxTS()) ? 0 : ts;
    }
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

inline bool PartCacheManager::isVisible(const DB::DataModelPartWrapperPtr & part_wrapper_ptr, const UInt64 & ts)
{
    return ts == 0 || (part_wrapper_ptr->txnID() <= ts && part_wrapper_ptr->part_model->commit_time() <= ts);
}
inline bool PartCacheManager::isVisible(const ServerDataPartPtr & data_part, const UInt64 & ts)
{
    return ts == 0 || (data_part->txnID() <= ts && data_part->getCommitTime() <= ts);
}
inline bool PartCacheManager::isVisible(const DB::DeleteBitmapMetaPtr & bitmap, const UInt64 & ts)
{
    return ts == 0 || (bitmap->getTxnId() <= ts && bitmap->getCommitTime() <= ts);
}
inline bool PartCacheManager::isVisible(const DB::DataModelDeleteBitmapPtr & bitmap, const UInt64 & ts)
{
    return ts == 0 || (bitmap->txn_id() <= ts && bitmap->commit_time() <= ts);
}

template <
    typename CachePtr,
    typename RetValue,
    typename CacheValueMap,
    typename FetchedValue,
    typename Adapter,
    typename LoadFunc,
    typename RetValueVec>
RetValueVec PartCacheManager::getOrSetDataInPartitions(
    const IStorage & table, const Strings & partitions, LoadFunc && load_func, const UInt64 & ts, const PairInt64 & topology_version)
{
    RetValueVec res;
    mayUpdateTableMeta(table, topology_version);
    const auto & storage = dynamic_cast<const MergeTreeMetaBase &>(table);
    TableMetaEntryPtr meta_ptr = getTableMeta(table.getStorageUUID());

    if (!meta_ptr)
        return res;

    /// On cnch worker, we disable part cache to avoid cache synchronization with server.
    if (getContext()->getServerType() != ServerType::cnch_server && !dummy_mode)
    {
        Vec<FetchedValue> fetched = load_func(partitions, meta_ptr->getPartitionIDs());
        for (auto & ele : fetched)
        {
            Adapter adapter(storage, ele);
            auto part_wrapper_ptr = adapter.getData();
            if (this->isVisible(part_wrapper_ptr, ts))
            {
                res.push_back(adapter.toData());
            }
        }
        return res;
    }

    if (meta_ptr->load_parts_by_partition)
        res = getDataByPartition<CachePtr, RetValue, CacheValueMap, FetchedValue, Adapter, LoadFunc, RetValueVec>(
            storage, meta_ptr, partitions, load_func, ts);
    else
        res = getDataInternal<CachePtr, RetValue, CacheValueMap, FetchedValue, Adapter, LoadFunc, RetValueVec>(
            storage, meta_ptr, partitions, meta_ptr->getPartitionIDs(), load_func, ts);

    return res;
}

void PartCacheManager::loadActiveTables()
{
    auto tables_meta = getContext()->getCnchCatalog()->getAllTables();
    if (tables_meta.empty())
        return;
    LOG_DEBUG(getLogger("PartCacheManager"), "Reloading {} active tables.", tables_meta.size());

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
        LOG_DEBUG(getLogger("PartCacheManager"), "{} getting parts and now loaded {} parts in memory", storage.getStorageID().getNameForLogs(), res.size());
}
*/

/// A helper function for getting right CacheStatus.
template <typename CachePtr>
DataCacheStatus * getCacheStatus(const PartitionInfoPtr & partition_info_ptr)
{
    if (!partition_info_ptr)
        throw Exception("Invailid partition info (nullptr) while getting cache status.", ErrorCodes::BAD_ARGUMENTS);

    if constexpr (std::is_same_v<CachePtr, CnchDataPartCachePtr>)
    {
        return &partition_info_ptr->part_cache_status;
    }
    else if constexpr (std::is_same_v<CachePtr, CnchDeleteBitmapCachePtr>)
    {
        return &partition_info_ptr->delete_bitmap_cache_status;
    }
    else
    {
        static_assert(DependentFalse<CachePtr>::value, "invalid template type for CachePtr");
    }
}

template <
    typename CachePtr,
    typename RetValue,
    typename CacheValueMap,
    typename FetchedValue,
    typename Adapter,
    typename LoadFunc,
    typename RetValueVec>
RetValueVec PartCacheManager::getDataInternal(
    const MergeTreeMetaBase & storage,
    const TableMetaEntryPtr & meta_ptr,
    const Strings & partitions,
    const Strings & all_existing_partitions,
    LoadFunc & load_func,
    const UInt64 & ts)
{
    String type;
    CachePtr cache_ptr = nullptr;
    if constexpr (std::is_same_v<CachePtr, CnchDataPartCachePtr>)
    {
        type = "parts";
        cache_ptr = part_cache_ptr;
    }
    else if constexpr (std::is_same_v<CachePtr, CnchDeleteBitmapCachePtr>)
    {
        type = "delete bitmaps";
        cache_ptr = delete_bitmap_cache_ptr;
    }
    else
    {
        static_assert(DependentFalse<CachePtr>::value, "invalid template type for CachePtr");
    }

    UUID uuid = storage.getStorageUUID();

    auto meta_partitions = meta_ptr->getPartitions(partitions);

    auto process_partition
        = [&, thread_group = CurrentThread::getGroup()](
              const String & partition_id, const PartitionInfoPtr & partition_info_ptr, RetValueVec & parts, bool & hit_cache) {
              DataCacheStatus * cache_status = getCacheStatus<CachePtr>(partition_info_ptr);

              hit_cache = false;
              {
                  if (cache_status->isLoaded())
                  {
                      auto cached = cache_ptr->get({uuid, partition_id});
                      if (cached)
                      {
                          hit_cache = true;
                          for (auto it = cached->begin(); it != cached->end(); ++it)
                          {
                              const auto & part_wrapper_ptr = *it;
                              if (this->isVisible(part_wrapper_ptr, ts))
                              {
                                  Adapter adapter(storage, part_wrapper_ptr);
                                  parts.push_back(adapter.toData());
                                  //logPartsVector(storage, res);
                              }
                          }
                      }
                  }
              }
              if (!hit_cache)
              {
                  auto partition_write_lock = partition_info_ptr->writeLock();
                  if (!cache_status->isLoading())
                    cache_status->setToLoading();
              }
          };

    RetValueVec res;
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
        std::map<String, RetValueVec> partition_parts;
        for (auto & [partition_id, partition_info_ptr] : meta_partitions)
        {
            partition_parts[partition_id] = RetValueVec();
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

        LOG_DEBUG(
            getLogger("PartCacheManager"),
            "Waiting for loading parts for table {} use {} threads.",
            storage.getStorageID().getNameForLogs(),
            max_threads);
        thread_pool.wait();
        exception_handler.throwIfException();

        size_t total_parts_number = 0;
        for (auto & [partition_id, parts] : partition_parts)
            total_parts_number += parts.size();
        res.reserve(total_parts_number);
        for (auto & [partition_id, parts] : partition_parts)
        {
            res.insert(res.end(), std::make_move_iterator(parts.begin()), std::make_move_iterator(parts.end()));
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

    auto fallback_cache_status = [&partitions_not_cached, &meta_partitions, &cache_ptr, &uuid]() {
        // reset partition data cache status to UINIT if loading failed.
        for (auto & partition_id : partitions_not_cached)
        {
            auto & partition_info = meta_partitions[partition_id];
            auto partition_write_lock = partition_info->writeLock();

            DataCacheStatus * cache_status = getCacheStatus<CachePtr>(partition_info);
            if (cache_status->isLoadingByCurrentThread())
            {
                cache_status->reset();
                // clear new inserted data during loading after reset cache status.
                cache_ptr->remove({uuid, partition_id});
            }
        }
    };

    try
    {
        /// Save data part model as well as data part to avoid build them with metaentry lock.
        std::map<String, RetValueVec> partition_to_parts;
        Vec<FetchedValue> fetched = load_func(partitions_not_cached, all_existing_partitions);

        /// The load_func may include partitions that not in the required `partitions_not_cache`
        /// Need to have an extra filter
        std::unordered_set<String> partitions_set(partitions_not_cached.begin(), partitions_not_cached.end());

        for (auto & ele : fetched)
        {
            Adapter adapter(storage, std::move(ele));
            // auto part_wrapper_ptr = createPartWrapperFromModel(storage, std::move(*(ele->model)), std::move(ele->name));
            auto part_wrapper_ptr = adapter.getData();
            const auto & partition_id = adapter.getPartitionId();
            if (!partitions_set.contains(partition_id))
                continue;
            auto it = partition_to_parts.find(partition_id);
            if (it != partition_to_parts.end())
                it->second.emplace_back(adapter.toData());
            else
                partition_to_parts[partition_id] = RetValueVec{adapter.toData()};
        }

        {
            /// merge fetched parts with that in cache;
            for (const auto & partition_id : partitions_not_cached)
            {
                auto & partition_info_ptr = meta_partitions[partition_id];
                auto partition_write_lock = partition_info_ptr->writeLock();

                DataCacheStatus * cache_status = getCacheStatus<CachePtr>(partition_info_ptr);

                /// Other other task may fail to fetch and change CacheStatus to UINIT before.
                /// If so, do not touch the cache since insert parts may be missed.
                /// Also, other task may success to fetch and change CacheStatus to LOADED before.
                /// If so, no need to modify cache again.
                /// If the loading thread has changed, do not update the cache.
                if (!cache_status->isLoadingByCurrentThread())
                    continue;

                RetValueVec * parts_wrapper_vector;
                std::optional<RetValueVec> empty_part_vector;
                auto it = partition_to_parts.find(partition_id);
                /// For empty partition, still need to insert an empty vector to cache
                if (it == partition_to_parts.end())
                {
                    empty_part_vector = std::make_optional<RetValueVec>();
                    parts_wrapper_vector = &empty_part_vector.value();
                }
                else
                {
                    parts_wrapper_vector = &it->second;
                }

                auto cached = cache_ptr->get({uuid, partition_id});
                if (!cached)
                {
                    /// directly insert all fetched parts into cache.
                    cached = std::make_shared<CacheValueMap>();
                    for (const auto & part_wrapper_ptr : *parts_wrapper_vector)
                    {
                        Adapter adapter(part_wrapper_ptr);
                        cached->update(adapter.getName(), adapter.getData());
                    }
                    cache_ptr->insert({uuid, partition_id}, cached);
                }
                else
                {
                    /// Its THREAD SAFE to update cache inplace
                    for (const auto & part_wrapper_ptr : *parts_wrapper_vector)
                    {
                        Adapter adapter(part_wrapper_ptr);
                        auto it_inner = cached->find(adapter.getName());
                        Adapter it_adapter(storage, *it_inner);
                        /// do not update cache if the cached data is newer than kv.
                        if (it_inner == cached->end() || it_adapter.getCommitTime() < adapter.getCommitTime())
                        {
                            cached->update(adapter.getName(), adapter.getData());
                        }
                    }
                    /// Force LRU cache update status(weight/evict).
                    cache_ptr->insert({uuid, partition_id}, cached);
                }

                cache_status->setToLoaded();
            }

            /// Add fetched parts to result outside to reduce write lock time
            for (auto & [partition_id, parts_wrapper_vector] : partition_to_parts)
            {
                for (const auto & part_wrapper_ptr : parts_wrapper_vector)
                {
                    if (this->isVisible(part_wrapper_ptr, ts))
                    {
                        Adapter adapter(part_wrapper_ptr);
                        res.push_back(adapter.toData());
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
template <
        typename CachePtr,
        typename RetValue,
        typename CacheValueMap,
        typename FetchedValue,
        typename Adapter,
        typename LoadFunc,
        typename RetValueVec>
    RetValueVec PartCacheManager::getDataByPartition(
        const MergeTreeMetaBase & storage,
        const TableMetaEntryPtr & meta_ptr,
        const Strings & partitions,
        LoadFunc & load_func,
        const UInt64 & ts)
    {
        String type;
        CachePtr cache_ptr = nullptr;

        if constexpr (std::is_same_v<CachePtr, CnchDataPartCachePtr>)
        {
            type = "parts";
            cache_ptr = part_cache_ptr;
        }
        else if constexpr (std::is_same_v<CachePtr, CnchDeleteBitmapCachePtr>)
        {
            type = "delete bitmaps";
            cache_ptr = delete_bitmap_cache_ptr;
        }
        else
        {
            static_assert(DependentFalse<CachePtr>::value, "invalid template type for CachePtr");
        }

        LOG_DEBUG(getLogger("PartCacheManager"), "Get {} by partitions for table : {}", type, storage.getLogName());
        Stopwatch watch;
        UUID uuid = storage.getStorageUUID();

        auto meta_partitions = meta_ptr->getPartitions(partitions);

        auto process_partition = [&](const String & partition_id, const PartitionInfoPtr & partition_info_ptr, RetValueVec & parts) {
            DataCacheStatus * cache_status = getCacheStatus<CachePtr>(partition_info_ptr);

            while (true)
            {
                /// stop if fetch part time exceeds the query max execution time.
                checkTimeLimit(watch);

                bool need_load_parts = false;
                {
                    if (cache_status->isLoaded())
                    {
                        auto cached = cache_ptr->get({uuid, partition_id});
                        if (cached)
                        {
                            for (auto it = cached->begin(); it != cached->end(); ++it)
                            {
                                const auto & data_wrapper_ptr = *it;
                                if (this->isVisible(data_wrapper_ptr, ts))
                                {
                                    Adapter adapter(storage, data_wrapper_ptr);
                                    parts.push_back(adapter.toData());
                                    //logPartsVector(storage, res);
                                }
                            }

                            /// already get parts from cache, continue to next partition
                            return;
                        }
                    }

                    auto partition_write_lock = partition_info_ptr->writeLock();
                    /// Double check
                    if (!cache_status->isLoading())
                    {
                        cache_status->setToLoading();
                        need_load_parts = true;
                    }
                }

                /// Now cache status must be LOADING;
                /// need to load parts from metastore
                if (need_load_parts)
                {
                    Vec<FetchedValue> fetched;
                    try
                    {
                        std::map<String, Vec<FetchedValue>> partition_to_parts;
                        fetched = load_func({partition_id}, {partition_id});
                        // FetchedValue fetched_data;
                        // for (auto & ele : fetched)
                        // {
                        //     Adapter adapter(storage, ele);
                        //     fetched_data.push_back(adapter.getData());
                        //     // fetched_data.push_back(createPartWrapperFromModel(storage, std::move(*(ele->model)), std::move(ele->name)));
                        // }

                        /// It happens that new parts have been inserted into cache during loading parts from bytekv, we need merge them to make
                        /// sure the cache contains all parts of the partition.
                        auto partition_write_lock = partition_info_ptr->writeLock();
                        if (cache_status->isLoadingByCurrentThread())
                        {
                            auto cached = cache_ptr->get({uuid, partition_id});
                            if (!cached)
                            {
                                /// directly insert all fetched parts into cache
                                cached = std::make_shared<CacheValueMap>();
                                for (auto & data_wrapper_ptr : fetched)
                                {
                                    Adapter adapter(storage, data_wrapper_ptr);
                                    cached->update(adapter.getName(), data_wrapper_ptr);
                                }
                                cache_ptr->insert({uuid, partition_id}, cached);
                            }
                            else
                            {
                                for (auto & data_wrapper_ptr : fetched)
                                {
                                    Adapter adapter(storage, data_wrapper_ptr);
                                    auto it = cached->find(adapter.getName());
                                    Adapter it_adapter(storage, *it);
                                    /// do not update cache if the cached data is newer than bytekv.
                                    if (it == cached->end() || it_adapter.getCommitTime() < adapter.getCommitTime())
                                    {
                                        cached->update(adapter.getName(), data_wrapper_ptr);
                                    }
                                }
                                /// Force LRU cache update status(weight/evict).
                                cache_ptr->insert({uuid, partition_id}, cached);
                            }

                            cache_status->setToLoaded();
                        }

                        /// Release partition lock before construct ServerDataPart
                        partition_write_lock.reset();

                        /// Finish fetching parts, notify other waiting tasks if any.
                        meta_ptr->fetch_cv.notify_all();

                        for (auto & data_wrapper_ptr : fetched)
                        {
                            /// Only filter the parts when both commit_time and txnid are smaller or equal to ts (txnid is helpful for intermediate parts).
                            if (this->isVisible(data_wrapper_ptr, ts))
                            {
                                Adapter adapter(storage, data_wrapper_ptr);
                                parts.push_back(adapter.toData());
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
                        cache_status->reset();
                        cache_ptr->remove({uuid, partition_id});
                        throw;
                    }
                }
                else /// other task is fetching parts now, just wait for the result
                {
                    {
                        std::unique_lock<std::mutex> lock(meta_ptr->fetch_mutex);
                        if (!meta_ptr->fetch_cv.wait_for(
                                lock, std::chrono::milliseconds(5000), [&cache_status]() { return cache_status->isLoaded(); }))
                        {
                            LOG_TRACE(
                                getLogger("PartCacheManager"),
                                "Wait timeout 5000ms for other thread loading table: {}, partition: {}",
                                storage.getStorageID().getNameForLogs(),
                                partition_id);
                            continue;
                        }
                    }

                    if (cache_status->isLoaded())
                    {
                        auto cached = cache_ptr->get({uuid, partition_id});
                        if (!cached)
                        {
                            throw Exception("Cannot get already loaded parts from cache. Its a logic error.", ErrorCodes::LOGICAL_ERROR);
                        }
                        for (auto it = cached->begin(); it != cached->end(); ++it)
                        {
                            const auto & part_wrapper_ptr = *it;
                            /// Only filter the parts when both commit_time and txnid are smaller or equal to ts (txnid is helpful for intermediate parts).
                            if (this->isVisible(part_wrapper_ptr, ts))
                            {
                                Adapter adapter(storage, part_wrapper_ptr);
                                parts.push_back(adapter.toData());
                                //logPartsVector(storage, res);
                            }
                        }
                        return;
                    }
                    // if cache status does not change to loaded, get parts of current partition again.
                }
            }
        };

        RetValueVec res;

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
            std::map<String, RetValueVec> partition_parts;
            for (auto & [partition_id, partition_info_ptr] : meta_partitions)
            {
                partition_parts[partition_id] = RetValueVec();
            }
            for (auto & [partition_id_, partition_info_ptr_] : meta_partitions)
            {
                thread_pool.scheduleOrThrowOnError(createExceptionHandledJob(
                    [&, partition_id = partition_id_, partition_info_ptr = partition_info_ptr_]() {
                        process_partition(partition_id, partition_info_ptr, partition_parts[partition_id]);
                    },
                    exception_handler));
            }

            LOG_DEBUG(
                getLogger("PartCacheManager"),
                "Waiting for loading parts for table {} use {} threads.",
                storage.getStorageID().getNameForLogs(),
                max_threads);
            thread_pool.wait();
            exception_handler.throwIfException();

            size_t total_parts_number = 0;
            for (auto & [partition_id, parts] : partition_parts)
                total_parts_number += parts.size();
            res.reserve(total_parts_number);
            for (auto & [partition_id, parts] : partition_parts)
            {
                res.insert(res.end(), std::make_move_iterator(parts.begin()), std::make_move_iterator(parts.end()));
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

std::tuple<UInt64, UInt64, UInt64> PartCacheManager::dumpPartCache()
{
    return {part_cache_ptr->count(), part_cache_ptr->weight(), part_cache_ptr->innerContainerSize()};
}

std::tuple<UInt64, UInt64, UInt64> PartCacheManager::dumpDeleteBitmapCache()
{
    return {delete_bitmap_cache_ptr->count(), delete_bitmap_cache_ptr->weight(), delete_bitmap_cache_ptr->innerContainerSize()};
}

std::tuple<UInt64, UInt64, UInt64, UInt64> PartCacheManager::dumpStorageCache()
{
    return {storageCachePtr->count(), storageCachePtr->weight(), storageCachePtr->innerContainerSize(), storageCachePtr->uuidNameMappingSize()};
}

std::unordered_map<String, std::pair<size_t, size_t>> PartCacheManager::getTablePartCacheInfo()
{
    CnchDataPartCachePtr cache_ptr = nullptr;
    {
        cache_ptr = part_cache_ptr;
    }
    if (!cache_ptr)
        return {};

    return cache_ptr->getTableCacheInfo();
}

std::unordered_map<String, std::pair<size_t, size_t>> PartCacheManager::getTableDeleteBitmapCacheInfo()
{
    CnchDeleteBitmapCachePtr cache_ptr = nullptr;
    {
        cache_ptr = delete_bitmap_cache_ptr;
    }
    if (!cache_ptr)
        return {};

    return cache_ptr->getTableCacheInfo();
}

void PartCacheManager::reset()
{
    LOG_DEBUG(getLogger("PartCacheManager::reset"), "Resetting part cache manager.");
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
            if (trashed_active_tables.empty())
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
    LOG_DEBUG(getLogger("PartCacheManager::shutdown"), "Shutdown method of part cache manager called.");
    table_partition_metrics.shutDown(this);
    active_table_loader->deactivate();
    meta_lock_cleaner->deactivate();
    trashed_active_tables_cleaner->deactivate();
}

StoragePtr PartCacheManager::getStorageFromCache(const UUID & uuid, const PairInt64 & topology_version, const Context & query_context)
{
    if (query_context.hasSessionTimeZone())
        return nullptr;
    StoragePtr res;
    TableMetaEntryPtr table_entry = getTableMeta(uuid);
    if (table_entry && topology_version == table_entry->cache_version.get())
        res = storageCachePtr->get(uuid);
    return res;
}

void PartCacheManager::insertStorageCache(const StorageID & storage_id, const StoragePtr storage, const UInt64 commit_ts, const PairInt64 & topology_version, const Context & query_context)
{
    if (query_context.hasSessionTimeZone())
        return;
    TableMetaEntryPtr table_entry = getTableMeta(storage_id.uuid);
    // reject insert old version storage into cache
    if (storage && storage->latest_version > commit_ts)
        return;
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
    auto log = getLogger("PartCacheManager::forceRecalculate");
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

        const auto * meta_storage = dynamic_cast<const StorageCnchMergeTree *>(storage.get());
        auto meta_partitions = table_meta->getPartitionList();

        // Skip if it passes TTL
        meta_storage->filterPartitionByTTL(meta_partitions, now);

        ret.reserve(meta_partitions.size());
        for (auto it = meta_partitions.begin(); it != meta_partitions.end(); it++)
        {
            auto partition_info = table_meta->getPartitionInfo((*it)->getID(*meta_storage));
            if (!partition_info)
                continue;

            Protos::LastModificationTimeHint hint = Protos::LastModificationTimeHint{};
            if (!meta_storage)
                throw Exception("Table is not a Meta Based MergeTree", ErrorCodes::UNKNOWN_TABLE);


            String partition = partition_info->getPartitionValue(*meta_storage);
            hint.set_partition_id(partition);

            std::optional<std::pair<PartitionMetrics::PartitionMetricsStore, bool>> data = load_func(partition_info);

            if (!data.has_value() || (data->first.total_parts_number < 0 || data->first.total_rows_count < 0))
            {
                LOG_WARNING(
                    getLogger("PartCacheManager::getLastModificationTimeHints"),
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

void PartCacheManager::invalidPartCache(const UUID & uuid, const Strings & part_names, MergeTreeDataFormatVersion version)
{
    invalidDataCache<PartPlainTextAdapter, MergeTreeDataFormatVersion>(uuid, part_names, version);
}

void PartCacheManager::invalidDeleteBitmapCache(const UUID & uuid, const Strings & delete_bitmap_names)
{
    invalidDataCache<DeleteBitmapPlainTextAdapter>(uuid, delete_bitmap_names);
}

ServerDataPartsVector PartCacheManager::getOrSetServerDataPartsInPartitions(
    const IStorage & table, const Strings & partitions, LoadPartsFunc && load_func, const UInt64 & ts, const PairInt64 & topology_version)
{
    return getOrSetDataInPartitions<
        CnchDataPartCachePtr,
        ServerDataPart,
        DataPartModelsMap,
        DataModelPartWrapper,
        ServerDataPartAdapter,
        LoadPartsFunc,
        Vec<const ServerDataPart>>(table, partitions, std::move(load_func), ts, topology_version);
}

void PartCacheManager::insertDeleteBitmapsIntoCache(
    const IStorage & table,
    const DeleteBitmapMetaPtrVector & delete_bitmaps,
    const PairInt64 & topology_version,
    const Protos::DataModelPartVector & helper_parts,
    const Protos::DataModelPartVector * const helper_staged_parts)
{
    std::unordered_map<String, String> partition_id_to_minmax;

    for (const auto & part : helper_parts.parts())
    {
        if (!partition_id_to_minmax.contains(part.part_info().partition_id()))
        {
            partition_id_to_minmax[part.part_info().partition_id()] = part.partition_minmax();
        }
    }

    if (helper_staged_parts)
    {
        for (const auto & part : helper_staged_parts->parts())
        {
            if (!partition_id_to_minmax.contains(part.part_info().partition_id()))
            {
                partition_id_to_minmax[part.part_info().partition_id()] = part.partition_minmax();
            }
        }
    }

    insertDataIntoCache<
        DeleteBitmapAdapter,
        DeleteBitmapMetaPtrVector,
        DataModelDeleteBitmapPtrVector,
        CnchDeleteBitmapCachePtr,
        DeleteBitmapModelsMap,
        std::function<String(const DataModelDeleteBitmapPtr &)>>(
        table,
        delete_bitmaps,
        false,
        false,
        topology_version,
        delete_bitmap_cache_ptr,
        dataDeleteBitmapGetKeyFunc,
        &partition_id_to_minmax);
}

DeleteBitmapMetaPtrVector PartCacheManager::getOrSetDeleteBitmapInPartitions(
    const IStorage & table,
    const Strings & partitions,
    LoadDeleteBitmapsFunc && load_func,
    const UInt64 & ts,
    const PairInt64 & topology_version)
{
    return getOrSetDataInPartitions<
        CnchDeleteBitmapCachePtr,
        DeleteBitmapMeta,
        DeleteBitmapModelsMap,
        DataModelDeleteBitmap,
        DeleteBitmapAdapter,
        LoadDeleteBitmapsFunc,
        Vec<DeleteBitmapMeta>>(table, partitions, std::move(load_func), ts, topology_version);
}

template <typename Adapter, typename... Args>
void PartCacheManager::invalidDataCache(const UUID & uuid, const Strings & data_names, Args... args)
{
    TableMetaEntryPtr meta_ptr = getTableMeta(uuid);

    if (!meta_ptr)
        return;

    std::unordered_map<String, Strings> partition_to_data;
    for (const auto & data_name : data_names)
    {
        Adapter data_adapter(data_name, args...);
        String partition_id = data_adapter.getPartitionId();
        auto it = partition_to_data.find(partition_id);
        if (it != partition_to_data.end())
        {
            it->second.emplace_back(data_name);
        }
        else
        {
            Strings data_list{data_name};
            partition_to_data.emplace(partition_id, data_list);
        }
    }
    if constexpr (std::is_same_v<Adapter, DeleteBitmapPlainTextAdapter>)
    {
        invalidDataCache(uuid, meta_ptr, partition_to_data, delete_bitmap_cache_ptr);
    }
    else if constexpr (std::is_same_v<Adapter, PartPlainTextAdapter>)
    {
        invalidDataCache(uuid, meta_ptr, partition_to_data, part_cache_ptr);
    }
    else
    {
        static_assert(DependentFalse<Adapter>::value, "invalid template type for Adapter");
    }
}


void PartCacheManager::insertDataPartsIntoCache(
    const IStorage & table,
    const pb::RepeatedPtrField<Protos::DataModelPart> & parts_model,
    const bool is_merged_parts,
    const bool should_update_metrics,
    const PairInt64 & topology_version)
{
    insertDataIntoCache<
        ServerDataPartAdapter,
        pb::RepeatedPtrField<Protos::DataModelPart>,
        DataModelPartWrapperVector,
        CnchDataPartCachePtr,
        DataPartModelsMap,
        std::function<String(const DataModelPartWrapperPtr &)>>(
        table, parts_model, is_merged_parts, should_update_metrics, topology_version, part_cache_ptr, dataPartGetKeyFunc, nullptr);
}

void PartCacheManager::insertStagedPartsIntoCache(
    const IStorage & table, const pb::RepeatedPtrField<Protos::DataModelPart> & parts_model, const PairInt64 & topology_version)
{
    insertDataIntoCache<
        ServerDataPartAdapter,
        pb::RepeatedPtrField<Protos::DataModelPart>,
        DataModelPartWrapperVector,
        CnchDataPartCachePtr,
        DataPartModelsMap,
        std::function<String(const DataModelPartWrapperPtr &)>, false>(
        table, parts_model, false, false, topology_version, part_cache_ptr, dataPartGetKeyFunc, nullptr);
}
}
