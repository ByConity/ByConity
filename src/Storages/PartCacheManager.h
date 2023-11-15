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

#include <Catalog/CatalogUtils.h>
#include <Catalog/DataModelPartWrapper_fwd.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Protos/DataModelHelpers.h>
#include <Storages/CnchPartitionInfo.h>
#include <Storages/CnchTablePartitionMetricsHelper.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Storages/TableMetaEntry.h>
#include <Common/CurrentThread.h>
#include <Common/HostWithPorts.h>
#include <Common/RWLock.h>
#include <Common/ScanWaitFreeMap.h>

namespace DB
{

class CnchDataPartCache;
using CnchDataPartCachePtr = std::shared_ptr<CnchDataPartCache>;
class CnchServerTopology;

class CnchStorageCache;
using CnchStorageCachePtr = std::shared_ptr<CnchStorageCache>;

class PartCacheManager: WithMutableContext
{
public:
    using DataPartPtr = std::shared_ptr<const MergeTreeDataPartCNCH>;
    using DataPartsVector = std::vector<DataPartPtr>;

    explicit PartCacheManager(ContextMutablePtr context_);
    ~PartCacheManager();

    void mayUpdateTableMeta(const IStorage & storage, const PairInt64 & topology_version);

    void updateTableNameInMetaEntry(const String & table_uuid, const String & database_name, const String & table_name);

    std::vector<TableMetaEntryPtr> getAllActiveTables();

    UInt64 getTableLastUpdateTime(const UUID & uuid);

    bool getTableClusterStatus(const UUID & uuid);

    void setTableClusterStatus(const UUID & uuid, bool clustered);

    void setTablePreallocateVW(const UUID & uuid, String vw);

    String getTablePreallocateVW(const UUID & uuid);

    /**
     * @brief Get partition level metrics of parts info with the given table.
     *
     * @param i_storage The table we want to query.
     * @param partitions A reference to the results.
     * @param require_partition_info Return `first_partition` info.
     * @return If there is a valid table_entry.
     */
    bool getPartsInfoMetrics(
        const IStorage & i_storage, std::unordered_map<String, PartitionFullPtr> & partitions, bool require_partition_info = true);
    /**
     * @brief Get table level metrics of trash items info.
     *
     * @param i_storage The table we want to query.
     * @return A `shared_ptr` to metrics info.
     */
    std::shared_ptr<TableMetrics> getTrashItemsInfoMetrics(const IStorage & i_storage);
    /**
     * @brief Update trash items metrics info.
     *
     * @param table_uuid UUID of the table.
     * @param items Added/deleted trash items.
     * @param positive A mark whether it's added or deleted.
     */
    void updateTrashItemsMetrics(const UUID & table_uuid, const Catalog::TrashItems & items, bool positive = true);

    /**
     * @brief Forcely trigger a recalculation for the table.
     */
    bool forceRecalculate(StoragePtr table);

    bool getPartitionList(const IStorage & storage, std::vector<std::shared_ptr<MergeTreePartition>> & partition_list, const PairInt64 & topology_version);

    bool getPartitionIDs(const IStorage & storage, std::vector<String> & partition_ids, const PairInt64 & topology_version);

    void invalidPartCache(const UUID & uuid);

    void invalidCacheWithNewTopology(const CnchServerTopology & topology);

    void invalidPartCacheWithoutLock(const UUID & uuid, std::unique_lock<std::mutex> & lock);

    void invalidPartCache(const UUID & uuid, const TableMetaEntryPtr & meta_ptr, const std::unordered_map<String, Strings> & partition_to_parts);

    void invalidPartCache(const UUID & uuid, const DataPartsVector & parts);

    /**
     * @brief Evict all parts specified.
     */
    void invalidPartCache(const UUID & uuid, const ServerDataPartsVector & parts);

    void invalidPartCache(const UUID & uuid, const Strings & part_names, MergeTreeDataFormatVersion version);

    void insertDataPartsIntoCache(
        const IStorage & table,
        const pb::RepeatedPtrField<Protos::DataModelPart> & parts_model,
        const bool is_merged_parts,
        const bool should_update_metrics,
        const PairInt64 & topology_version);

    /// Get count and weight in Part cache
    std::pair<UInt64, UInt64> dumpPartCache();
    std::pair<UInt64, UInt64> dumpStorageCache();

    std::unordered_map<String, std::pair<size_t, size_t>> getTableCacheInfo();

    using LoadPartsFunc = std::function<DataModelPartPtrVector(const Strings&, const Strings&)>;

    ServerDataPartsVector getOrSetServerDataPartsInPartitions(
        const IStorage & table,
        const Strings & partitions,
        LoadPartsFunc && load_func,
        const UInt64 & ts,
        const PairInt64 & topology_version);

    void mayUpdateTableMeta(const StoragePtr & table);

    bool trySetCachedNHUTForUpdate(const UUID & uuid, const UInt64 & pts);

    bool checkIfCacheValidWithNHUT(const UUID & uuid, const UInt64 & nhut);

    StoragePtr getStorageFromCache(const UUID & uuid, const PairInt64 & topology_version);

    void insertStorageCache(const StorageID & storage_id, const StoragePtr storage, const UInt64 commit_ts, const PairInt64 & topology_version);

    void removeStorageCache(const String & database, const String & table = "");

    void reset();

    void shutDown();

    std::unordered_map<UUID, TableMetaEntryPtr> getTablesSnapshot();
    friend class CnchTablePartitionMetricsHelper;

private:
    mutable std::mutex cache_mutex;
    CnchDataPartCachePtr part_cache_ptr;
    std::unordered_map<UUID, TableMetaEntryPtr> active_tables;
    CnchStorageCachePtr storageCachePtr;

    /// A cache for the NHUT which has been written to bytekv. Do not need to update NHUT each time when non-host server commit parts
    /// bacause tso has 3 seconds interval. We just cache the latest updated NHUT and only write to metastore if current ts is
    /// different from it.
    std::unordered_map<UUID, UInt64> cached_nhut_for_update {};
    std::mutex cached_nhut_mutex;

    /// We manage the table meta locks here to make sure each table has only one meta lock no matter how many different table meta entry it has.
    /// The lock is cleaned by a background task if it is no longer be used by any table meta entry.
    std::unordered_map<UUID, RWLock> meta_lock_container;

    BackgroundSchedulePool::TaskHolder active_table_loader; // Used to load table when server start up, only execute once;
    BackgroundSchedulePool::TaskHolder meta_lock_cleaner; // remove unused meta lock periodically;

    CnchTablePartitionMetricsHelper table_partition_metrics;
    void cleanMetaLock();
    // load tables belongs to current server according to the topology. The task is performed asynchronously.
    void loadActiveTables();
    TableMetaEntryPtr getTableMeta(const UUID & uuid);

    // we supply two implementation for getting parts. Normally, we just use getPartsInternal. If the table parts number is huge we can
    // fetch parts sequentially for each partition by using getPartsByPartition.
    ServerDataPartsVector getServerPartsInternal(const MergeTreeMetaBase & storage, const TableMetaEntryPtr & meta_ptr,
        const Strings & partitions, const Strings & all_existing_partitions, LoadPartsFunc & load_func, const UInt64 & ts);
    ServerDataPartsVector getServerPartsByPartition(const MergeTreeMetaBase & storage, const TableMetaEntryPtr & meta_ptr,
        const Strings & partitions, LoadPartsFunc & load_func, const UInt64 & ts);
    //DataModelPartWrapperVector getPartsModelByPartition(const MergeTreeMetaBase & storage, const TableMetaEntryPtr & meta_ptr,
    //    const Strings & partitions, const Strings & all_existing_partitions, LoadPartsFunc & load_func, const UInt64 & ts);

    void checkTimeLimit(Stopwatch & watch);

    size_t getMaxThreads() const;
};

using PartCacheManagerPtr = std::shared_ptr<PartCacheManager>;

}
