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
#include <Storages/CnchDataPartCache.h>
#include <Storages/CnchPartitionInfo.h>
#include <Storages/CnchTablePartitionMetricsHelper.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Storages/TableMetaEntry.h>
#include <Common/CurrentThread.h>
#include <Common/HostWithPorts.h>
#include <Common/RWLock.h>
#include <Common/ScanWaitFreeMap.h>
#include <Common/SimpleIncrement.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_STORAGE;
}

class CnchServerTopology;

class CnchStorageCache;
using CnchStorageCachePtr = std::shared_ptr<CnchStorageCache>;

class PartCacheManager : WithMutableContext
{
public:
    using DataPartPtr = std::shared_ptr<const MergeTreeDataPartCNCH>;
    using DataPartsVector = std::vector<DataPartPtr>;

    explicit PartCacheManager(ContextMutablePtr context_, size_t memory_limit = 0, bool dummy_mode = false);
    ~PartCacheManager();

    TableMetaEntryPtr getTableMeta(const UUID & uuid);

    /// `on_table_creation` will give the information that there is no need to sync metrics on the new table.
    void mayUpdateTableMeta(const IStorage & storage, const PairInt64 & topology_version, bool on_table_creation = false);

    void updateTableNameInMetaEntry(const String & table_uuid, const String & database_name, const String & table_name);

    std::vector<TableMetaEntryPtr> getAllActiveTables();

    UInt64 getTableLastUpdateTime(const UUID & uuid);

    bool getTableClusterStatus(const UUID & uuid);

    void setTableClusterStatus(const UUID & uuid, bool clustered, const TableDefinitionHash & table_definition_hash);

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

    std::vector<Protos::LastModificationTimeHint>
    getLastModificationTimeHints(const ConstStoragePtr & storage, bool allow_regression = false);

    /**
     * @brief update the GC time for partitions; mark/unmark deleted
     *
     * @param partitions partitions to update
     * @param ts GC time to update
     * @return partitions which have been successfully updated
     */
    Catalog::PartitionMap updatePartitionGCTime(const StoragePtr table, const Strings & partitions, UInt32 ts);
    /**
     * remove deleting partitions entry from table meta entry.
     *
     * @param partitions partitions to be removed
     */
    void removeDeletedPartitions(const StoragePtr table, const Strings & partitions);

    std::unordered_set<String> getDeletingPartitions(const StoragePtr table);

    bool getPartitionList(
        const IStorage & table, std::vector<std::shared_ptr<MergeTreePartition>> & partition_list, const PairInt64 & topology_version);

    bool getPartitionInfo(const IStorage & storage, Catalog::PartitionMap & partitions, const PairInt64 & topology_version, const Strings & required_partitions);

    bool getPartitionIDs(const IStorage & storage, std::vector<String> & partition_ids, const PairInt64 & topology_version);

    void invalidCacheWithNewTopology(const CnchServerTopology & topology);

    std::pair<Int64, Int64> getTotalAndMaxPartsNumber(const IStorage & storage);

    /**
     * @brief Evict all parts specified.
     */
    void invalidPartAndDeleteBitmapCache(const UUID & uuid, bool skip_part_cache = false, bool skip_delete_bitmap_cache = false);

    void invalidPartCache(const UUID & uuid, const DataPartsVector & parts);
    void invalidPartCache(const UUID & uuid, const ServerDataPartsVector & parts);
    void invalidPartCache(const UUID & uuid, const IMergeTreeDataPartsVector & parts);
    void invalidPartCache(const UUID & uuid, const Strings & part_names, MergeTreeDataFormatVersion version);

    void invalidDeleteBitmapCache(const UUID & uuid, const Strings & delete_bitmap_names);
    void invalidDeleteBitmapCache(const UUID & uuid, const DeleteBitmapMetaPtrVector & parts);
    void invalidDeleteBitmapCache(const UUID & uuid)
    {
        std::unique_lock<std::mutex> lock(cache_mutex);
        delete_bitmap_cache_ptr->dropCache(uuid);
    }

    // void invalidPartCache(const UUID & uuid, const Strings & part_names, MergeTreeDataFormatVersion version);
    void insertDataPartsIntoCache(
        const IStorage & table,
        const pb::RepeatedPtrField<Protos::DataModelPart> & parts_model,
        bool is_merged_parts,
        bool should_update_metrics,
        const PairInt64 & topology_version);

    /// Only for create corresponding partition info
    void insertStagedPartsIntoCache(
        const IStorage & table,
        const pb::RepeatedPtrField<Protos::DataModelPart> & parts_model,
        const PairInt64 & topology_version);

    void insertDeleteBitmapsIntoCache(
        const IStorage & table,
        const DeleteBitmapMetaPtrVector & delete_bitmaps,
        const PairInt64 & topology_version,
        const Protos::DataModelPartVector & helper_parts,
        const Protos::DataModelPartVector * helper_staged_parts = nullptr);

    /// Get count and weight in Part cache
    /// return LRUCache count, weight and inner container size
    std::tuple<UInt64, UInt64, UInt64> dumpPartCache();
    std::tuple<UInt64, UInt64, UInt64> dumpDeleteBitmapCache();

    /// return LRUCache count, weight and inner container size, as well as bimap size in storage cache
    std::tuple<UInt64, UInt64, UInt64, UInt64> dumpStorageCache();

    std::unordered_map<String, std::pair<size_t, size_t>> getTablePartCacheInfo();
    std::unordered_map<String, std::pair<size_t, size_t>> getTableDeleteBitmapCacheInfo();


    using LoadPartsFunc = std::function<DataModelPartWrapperVector(const Strings &, const Strings &)>;
    using LoadDeleteBitmapsFunc = std::function<DataModelDeleteBitmapPtrVector(const Strings &, const Strings &)>;

    ServerDataPartsVector getOrSetServerDataPartsInPartitions(
        const IStorage & table,
        const Strings & partitions,
        LoadPartsFunc && load_func,
        const UInt64 & ts,
        const PairInt64 & topology_version);

    DeleteBitmapMetaPtrVector getOrSetDeleteBitmapInPartitions(
        const IStorage & table,
        const Strings & partitions,
        LoadDeleteBitmapsFunc && load_func,
        const UInt64 & ts,
        const PairInt64 & topology_version);

    bool trySetCachedNHUTForUpdate(const UUID & uuid, const UInt64 & pts);

    bool checkIfCacheValidWithNHUT(const UUID & uuid, const UInt64 & nhut);

    StoragePtr getStorageFromCache(const UUID & uuid, const PairInt64 & topology_version, const Context & query_context);

    void insertStorageCache(const StorageID & storage_id, StoragePtr storage, UInt64 commit_ts, const PairInt64 & topology_version, const Context & query_context);

    void removeStorageCache(const String & database, const String & table = "");

    void reset();

    void shutDown();

    std::unordered_map<UUID, TableMetaEntryPtr> getTablesSnapshot();

    size_t cleanTrashedActiveTables();
private:
    bool dummy_mode;
    mutable std::mutex cache_mutex;
    CnchDataPartCachePtr part_cache_ptr;
    CnchDeleteBitmapCachePtr delete_bitmap_cache_ptr;
    std::unordered_map<UUID, TableMetaEntryPtr> active_tables;
    CnchStorageCachePtr storageCachePtr;

    /// Task id for data parts loading.
    mutable SimpleIncrement load_task_increment;

    mutable std::mutex trashed_active_tables_mutex;
    /// Trashed TableMetaEntryPtr, will be cleaned in a background thread.
    std::list<TableMetaEntryPtr> trashed_active_tables;
    /// A cache for the NHUT which has been written to bytekv. Do not need to update NHUT each time when non-host server commit parts
    /// bacause tso has 3 seconds interval. We just cache the latest updated NHUT and only write to metastore if current ts is
    /// different from it.
    std::unordered_map<UUID, UInt64> cached_nhut_for_update{};
    std::mutex cached_nhut_mutex;

    /// We manage the table meta locks here to make sure each table has only one meta lock no matter how many different table meta entry it has.
    /// The lock is cleaned by a background task if it is no longer be used by any table meta entry.
    std::unordered_map<UUID, std::shared_ptr<MetaLockHolder>> meta_lock_container;

    BackgroundSchedulePool::TaskHolder active_table_loader; // Used to load table when server start up, only execute once;
    BackgroundSchedulePool::TaskHolder meta_lock_cleaner; // remove unused meta lock periodically;
    /// release lock periodically;
    BackgroundSchedulePool::TaskHolder trashed_active_tables_cleaner;

    CnchTablePartitionMetricsHelper table_partition_metrics;
    void cleanMetaLock();
    // load tables belongs to current server according to the topology. The task is performed asynchronously.
    void loadActiveTables();

    template <typename T>
    using Vec = std::vector<std::shared_ptr<T>>;

    template <
        typename CachePtr,
        typename RetValue,
        typename CacheValueMap,
        typename FetchedValue,
        typename Adapter,
        typename LoadFunc,
        typename RetValueVec>
    RetValueVec getOrSetDataInPartitions(
        const IStorage & table, const Strings & partitions, LoadFunc && load_func, const UInt64 & ts, const PairInt64 & topology_version);

    // we supply two implementation for getting parts. Normally, we just use getPartsInternal. If the table parts number is huge we can
    // fetch parts sequentially for each partition by using getPartsByPartition.
    template <
        typename CachePtr,
        typename RetValue,
        typename CacheValueMap,
        typename FetchedValue,
        typename Adapter,
        typename LoadFunc,
        typename RetValueVec>
    RetValueVec getDataInternal(
        const MergeTreeMetaBase & storage,
        const TableMetaEntryPtr & meta_ptr,
        const Strings & partitions,
        const Strings & all_existing_partitions,
        LoadFunc & load_func,
        const UInt64 & ts);

    inline static bool isVisible(const DB::DataModelPartWrapperPtr & part_wrapper_ptr, const UInt64 & ts);
    inline static bool isVisible(const ServerDataPartPtr & data_part, const UInt64 & ts);
    inline static bool isVisible(const DB::DeleteBitmapMetaPtr & bitmap, const UInt64 & ts);
    inline static bool isVisible(const DB::DataModelDeleteBitmapPtr & bitmap, const UInt64 & ts);

    template <
        typename CachePtr,
        typename RetValue,
        typename CacheValueMap,
        typename FetchedValue,
        typename Adapter,
        typename LoadFunc,
        typename RetValueVec>
    RetValueVec getDataByPartition(
        const MergeTreeMetaBase & storage,
        const TableMetaEntryPtr & meta_ptr,
        const Strings & partitions,
        LoadFunc & load_func,
        const UInt64 & ts);

    void checkTimeLimit(Stopwatch & watch);

    size_t getMaxThreads() const;

    void invalidPartCacheWithoutLock(const UUID & uuid, std::unique_lock<std::mutex> & lock, bool skip_part_cache = false, bool skip_delete_bitmap_cache = false);

    template <typename DataCachePtr>
    void invalidDataCache(
        const UUID & uuid,
        const TableMetaEntryPtr & meta_ptr,
        const std::unordered_map<String, Strings> & partition_to_data_list,
        DataCachePtr cache_ptr);

    template <typename Ds, typename Adapter>
    void invalidDataCache(const UUID & uuid, const Ds & xs);

    template <typename Adapter, typename... Args>
    void invalidDataCache(const UUID & uuid, const Strings & data_names, Args... args);

    template <typename Adapter, typename InputValueVec, typename ValueVec, typename CachePtr, typename CacheValueMap, typename GetKeyFunc, bool insert_into_cache = true>
    void insertDataIntoCache(
        const IStorage & table,
        const InputValueVec & parts_model,
        bool is_merged_parts,
        bool should_update_metrics,
        const PairInt64 & topology_version,
        CachePtr cache_ptr,
        GetKeyFunc func,
        const std::unordered_map<String, String> * extra_partition_info);

    UInt64 getLoadTaskID() const;
};

using PartCacheManagerPtr = std::shared_ptr<PartCacheManager>;

}
