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

#include <map>
#include <set>
#include <Catalog/CatalogUtils.h>
#include <Catalog/DataModelPartWrapper.h>
#include <Catalog/MetastoreProxy.h>
#include <CloudServices/Checkpoint.h>
#include <Core/Types.h>
#include <Protos/DataModelHelpers.h>
#include <Protos/cnch_common.pb.h>
#include <Protos/cnch_server_rpc.pb.h>
#include <Statistics/ExportSymbols.h>
#include <Statistics/StatisticsBase.h>
// #include <Transaction/ICnchTransaction.h>
#include <ResourceManagement/CommonData.h>
#include <Storages/MergeTree/CnchMergeTreeMutationEntry.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h>
#include <Transaction/TransactionCommon.h>
#include <Transaction/TxnTimestamp.h>
#include <cppkafka/cppkafka.h>
#include <Common/Configurations.h>
#include <Common/DNSResolver.h>
#include <Common/HostWithPorts.h>
#include <Common/Config/MetastoreConfig.h>
#include <common/getFQDNOrHostName.h>
#include "Catalog/IMetastore.h"
// #include <Access/MaskingPolicyDataModel.h>

namespace DB::ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
} // namespace DB::ErrorCodes

namespace DB
{
class AttachFilter;
struct PrunedPartitions;
}

namespace DB::Catalog
{
class Catalog
{
public:
    using DatabasePtr = std::shared_ptr<DB::IDatabase>;
    using DataModelTables = std::vector<Protos::DataModelTable>;
    using DataModelDBs = std::vector<Protos::DataModelDB>;
    using DataModelWorkerGroups = std::vector<Protos::DataModelWorkerGroup>;
    using DataModelDictionaries = std::vector<Protos::DataModelDictionary>;
    // using DataModelMaskingPolicies = std::vector<Protos::DataModelMaskingPolicy>;
    using DataModelUDFs = std::vector<Protos::DataModelUDF>;

    using MetastoreProxyPtr = std::shared_ptr<MetastoreProxy>;

    Catalog(Context & _context, const MetastoreConfig & config, String _name_space = "default");

    ~Catalog() = default;

    MetastoreProxy::MetastorePtr getMetastore();

    /// update optimizer stats
    void updateTableStatistics(const String & uuid, const std::unordered_map<StatisticsTag, StatisticsBasePtr> & data);

    /// get the latest stats
    /// if tag not exists, don't put it into the map
    std::unordered_map<StatisticsTag, StatisticsBasePtr>
    getTableStatistics(const String & uuid, const std::unordered_set<StatisticsTag> & tags);
    /// get all tags
    std::unordered_set<StatisticsTag> getAvailableTableStatisticsTags(const String & uuid);

    /// remove tags
    void removeTableStatistics(const String & uuid, const std::unordered_set<StatisticsTag> & tags);

    /// stats for Column

    /// update optimizer stats
    void updateColumnStatistics(
        const String & uuid, const String & column, const std::unordered_map<StatisticsTag, StatisticsBasePtr> & data);

    /// get the latest stats
    /// if tag not exists, don't put it into the map
    std::unordered_map<StatisticsTag, StatisticsBasePtr>
    getColumnStatistics(const String & uuid, const String & column, const std::unordered_set<StatisticsTag> & tags);
    /// get all tags
    std::unordered_set<StatisticsTag> getAvailableColumnStatisticsTags(const String & uuid, const String & column);
    /// remove tags
    void removeColumnStatistics(const String & uuid, const String & column, const std::unordered_set<StatisticsTag> & tags);
    //////////////

    void updateSQLBinding(const SQLBindingItemPtr data);

    SQLBindings getSQLBindings();

    SQLBindings getReSQLBindings(const bool & is_re_expression);

    SQLBindingItemPtr getSQLBinding(const String & uuid, const bool & is_re_expression);

    void removeSQLBinding(const String & uuid, const bool & is_re_expression);

    ///database related interface
    void createDatabase(const String & database, const UUID & uuid, const TxnTimestamp & txnID, const TxnTimestamp & ts,
                        const String & create_query = "", const String & engine_name = "");

    DatabasePtr getDatabase(const String & database, const ContextPtr & context, const TxnTimestamp & ts = 0);

    bool isDatabaseExists(const String & database, const TxnTimestamp & ts = 0);

    void dropDatabase(const String & database, const TxnTimestamp & previous_version, const TxnTimestamp & txnID, const TxnTimestamp & ts);

    void renameDatabase(const String & from_database, const String & to_database, const TxnTimestamp & txnID, const TxnTimestamp & ts);

    ///currently only used for materialized mysql
    void alterDatabase(const String & alter_database, const TxnTimestamp & txnID, const TxnTimestamp & ts,
                       const String & create_query = "", const String & engine_name = "");

    ///table related interface
    void createTable(
        const StorageID & storage_id,
        const String & create_query,
        const String & virtual_warehouse,
        const TxnTimestamp & txnID,
        const TxnTimestamp & ts);

    void dropTable(
        const StoragePtr & storage, const TxnTimestamp & previous_version, const TxnTimestamp & txnID, const TxnTimestamp & ts);

    void createUDF(const String & db, const String & name, const String & create_query);

    void dropUDF(const String & db, const String & name);

    void detachTable(const String & db, const String & name, const TxnTimestamp & ts);

    void attachTable(const String & db, const String & name, const TxnTimestamp & ts);

    bool isTableExists(const String & db, const String & name, const TxnTimestamp & ts = 0);

    void alterTable(
        const Context & query_context,
        const Settings & query_settings,
        const StoragePtr & storage,
        const String & new_create,
        const TxnTimestamp & previous_version,
        const TxnTimestamp & txnID,
        const TxnTimestamp & ts,
        const bool is_recluster);


    void renameTable(
        const String & from_database,
        const String & from_table,
        const String & to_database,
        const String & to_table,
        const TxnTimestamp & txnID,
        const TxnTimestamp & ts);

    void setWorkerGroupForTable(const String & db, const String & name, const String & worker_group, UInt64 worker_topology_hash);

    StoragePtr getTable(const Context & query_context, const String & database, const String & name, const TxnTimestamp & ts = TxnTimestamp::maxTS());

    StoragePtr tryGetTable(const Context & query_context, const String & database, const String & name, const TxnTimestamp & ts = TxnTimestamp::maxTS());

    StoragePtr tryGetTableByUUID(const Context & query_context, const String & uuid, const TxnTimestamp & ts, bool with_delete = false);

    StoragePtr getTableByUUID(const Context & query_context, const String & uuid, const TxnTimestamp & ts, bool with_delete = false);

    Strings getTablesInDB(const String & database);

    std::vector<StoragePtr> getAllViewsOn(const Context & session_context, const StoragePtr & storage, const TxnTimestamp & ts);

    void setTableActiveness(const StoragePtr & storage, const bool is_active, const TxnTimestamp & ts);
    /// return true if table is active, false otherwise
    bool getTableActiveness(const StoragePtr & storage, const TxnTimestamp & ts);

    ///data parts related interface
    ServerDataPartsVector getServerDataPartsInPartitions(const ConstStoragePtr & storage, const Strings & partitions, const TxnTimestamp & ts, const Context * session_context, bool for_gc = false);

    ServerDataPartsVector getAllServerDataParts(const ConstStoragePtr & storage, const TxnTimestamp & ts, const Context * session_context);
    DataPartsVector getDataPartsByNames(const NameSet & names, const StoragePtr & table, const TxnTimestamp & ts);
    DataPartsVector getStagedDataPartsByNames(const NameSet & names, const StoragePtr & table, const TxnTimestamp & ts);
    DeleteBitmapMetaPtrVector getAllDeleteBitmaps(const MergeTreeMetaBase & storage);

    // return table's committed staged parts. if partitions != null, ignore staged parts not belong to `partitions`.
    DataPartsVector getStagedParts(const StoragePtr & table, const TxnTimestamp & ts, const NameSet * partitions = nullptr);

    /// (UNIQUE KEY) fetch all delete bitmaps <= ts in the given partitions
    DeleteBitmapMetaPtrVector
    getDeleteBitmapsInPartitions(const ConstStoragePtr & storage, const Strings & partitions, const TxnTimestamp & ts = 0);
    /// (UNIQUE KEY) get bitmaps by keys
    DeleteBitmapMetaPtrVector getDeleteBitmapByKeys(const StoragePtr & storage, const NameSet & keys);
    /// (UNIQUE KEY) remove bitmaps meta from KV, used by GC
    void removeDeleteBitmaps(const StoragePtr & storage, const DeleteBitmapMetaPtrVector & bitmaps);

    // V1 part commit API
    void finishCommit(
        const StoragePtr & table,
        const TxnTimestamp & txnID,
        const TxnTimestamp & commit_ts,
        const DataPartsVector & parts,
        const DeleteBitmapMetaPtrVector & delete_bitmaps = {},
        const bool is_merged_parts = false,
        const bool preallocate_mode = false);

    /// APIs for CncnKafka
    void getKafkaOffsets(const String & consumer_group, cppkafka::TopicPartitionList & tpl);
    cppkafka::TopicPartitionList getKafkaOffsets(const String & consumer_group, const String & kafka_topic);
    void clearOffsetsForWholeTopic(const String & topic, const String & consumer_group);
    void setTransactionForKafkaConsumer(const UUID & uuid, const TxnTimestamp & txn_id, size_t consumer_index);
    TxnTimestamp getTransactionForKafkaConsumer(const UUID & uuid, size_t consumer_index);
    void clearKafkaTransactions(const UUID & uuid);

    void dropAllPart(const StoragePtr & storage, const TxnTimestamp & txnID, const TxnTimestamp & ts);

    std::vector<std::shared_ptr<MergeTreePartition>> getPartitionList(const ConstStoragePtr & table, const Context * session_context);

    template<typename Map>
    void getPartitionsFromMetastore(const MergeTreeMetaBase & table, Map & partition_list);

    Strings getPartitionIDs(const ConstStoragePtr & storage, const Context * session_context);

    PrunedPartitions getPartitionsByPredicate(ContextPtr session_context, const ConstStoragePtr & storage, const SelectQueryInfo & query_info, const Names & column_names_to_return);
    /// dictionary related APIs

    void createDictionary(const StorageID & storage_id, const String & create_query);

    ASTPtr getCreateDictionary(const String & database, const String & name);

    void dropDictionary(const String & database, const String & name);

    void detachDictionary(const String & database, const String & name);

    void attachDictionary(const String & database, const String & name);

    /// for backward compatible, the old dictionary doesn't have UUID
    void fixDictionary(const String & database, const String & name);

    Strings getDictionariesInDB(const String & database);

    Protos::DataModelDictionary getDictionary(const String & database, const String & name);

    StoragePtr tryGetDictionary(const String & database, const String & name, ContextPtr context);

    bool isDictionaryExists(const String & db, const String & name);

    /// API for transaction model
    void createTransactionRecord(const TransactionRecord & record);

    void removeTransactionRecord(const TransactionRecord & record);
    void removeTransactionRecords(const std::vector<TxnTimestamp> & txn_ids);

    TransactionRecord getTransactionRecord(const TxnTimestamp & txnID);
    std::optional<TransactionRecord> tryGetTransactionRecord(const TxnTimestamp & txnID);

    /// CAS operation
    /// If success return true.
    /// If fail return false and put current kv value in `target_record`
    bool setTransactionRecord(const TransactionRecord & expected_record, TransactionRecord & record);

    /// CAS operation
    /// Similiar to setTransactionRecord, but we want to pack addition requests (e.g. create a insertion label)
    bool setTransactionRecordWithRequests(
        const TransactionRecord & expected_record, TransactionRecord & record, BatchCommitRequest & request, BatchCommitResponse & response);

    void setTransactionRecordCleanTime(TransactionRecord record, const TxnTimestamp & ts, UInt64 ttl);

    bool setTransactionRecordStatusWithOffsets(
        const TransactionRecord & expected_record,
        TransactionRecord & record,
        const String & consumer_group,
        const cppkafka::TopicPartitionList & tpl);

    bool setTransactionRecordStatusWithBinlog(
        const TransactionRecord & expected_record,
        TransactionRecord & record,
        const String & binlog_name,
        const std::shared_ptr<Protos::MaterializedMySQLBinlogMetadata> & binlog);

    /// just set transaction status to aborted
    void rollbackTransaction(TransactionRecord record);

    /// add lock implementation for Pessimistic mode.
    bool writeIntents(
        const String & intent_prefix,
        const std::vector<WriteIntent> & intents,
        std::map<std::pair<TxnTimestamp, String>, std::vector<String>> & conflictIntents);

    /// used in the following 2 cases.
    /// 1. when meeting with some intents belongs to some zombie transaction.
    /// 2. try to preempt some intents belongs to low-priority running transaction.
    bool tryResetIntents(
        const String & intent_prefix,
        const std::map<std::pair<TxnTimestamp, String>, std::vector<String>> & intentsToReset,
        const TxnTimestamp & newTxnID,
        const String & newLocation);

    bool tryResetIntents(
        const String & intent_prefix,
        const std::vector<WriteIntent> & oldIntents,
        const TxnTimestamp & newTxnID,
        const String & newLocation);

    /// used when current transaction is committed or aborted,
    /// to clear intents in batch.
    void clearIntents(const String & intent_prefix, const std::vector<WriteIntent> & intents);

    /// V2 part commit API.
    /// If the commit time of parts and delete_bitmaps not set, they are invisible unless txn_reocrd is committed.
    void writeParts(
        const StoragePtr & table,
        const TxnTimestamp & txnID,
        const CommitItems & commit_data,
        const bool is_merged_parts = false,
        const bool preallocate_mode = false);

    /// set commit time for parts and delete bitmaps
    void setCommitTime(
        const StoragePtr & table,
        const CommitItems & commit_data,
        const TxnTimestamp & commitTs,
        const UInt64 txn_id = 0);

    void clearParts(
        const StoragePtr & table,
        const CommitItems & commit_data,
        const bool skip_part_cache = false);

    /// write undo buffer before write vfs
    void writeUndoBuffer(const String & uuid, const TxnTimestamp & txnID, const UndoResources & resources);
    void writeUndoBuffer(const String & uuid, const TxnTimestamp & txnID, UndoResources && resources);

    /// clear undo buffer
    void clearUndoBuffer(const TxnTimestamp & txnID);

    /// return storage uuid -> undo resources
    std::unordered_map<String, UndoResources> getUndoBuffer(const TxnTimestamp & txnID);

    /// return txn_id -> undo resources
    std::unordered_map<UInt64, UndoResources> getAllUndoBuffer();

    class UndoBufferIterator
    {
    public:
        UndoBufferIterator(IMetaStore::IteratorPtr metastore_iter, Poco::Logger * log);
        const UndoResource & getUndoResource() const;
        bool next();
        bool is_valid() const /// for testing
        {
            return valid;
        }
    private:
        IMetaStore::IteratorPtr metastore_iter;
        std::optional<UndoResource> cur_undo_resource;
        bool valid = false;
        Poco::Logger * log;
    };

    UndoBufferIterator getUndoBufferIterator() const;

    /// get transaction records, if the records exists, we can check with the transaction coordinator to detect zombie record.
    /// the transaction record will be cleared only after all intents have been cleared and set commit time for all parts.
    /// For zombie record, the intents to be clear can be scanned from intents space with txnid. The parts can be get from undo buffer.
    std::vector<TransactionRecord> getTransactionRecords();
    std::vector<TransactionRecord> getTransactionRecords(const std::vector<TxnTimestamp> & txn_ids, size_t batch_size = 0);
    /// clean zombie records. If the total transaction record number is too large, it may be impossible to get all of them. We can
    /// pass a max_result_number to only get part of them and clean zombie records repeatedlly
    std::vector<TransactionRecord> getTransactionRecordsForGC(size_t max_result_number);

    /// Clear intents written by zombie transaction.
    void clearZombieIntent(const TxnTimestamp & txnID);

    /// Below method provides method to locks a directory during `ALTER ATTACH PARTS FROM ... ` query. If there's a lock record
    /// that mean there's some manipulation are being done on that directory by cnch, so user should not use it in other query
    /// (that will eventualy fail). **NOTES: it DOES NOT belong to general cnch lock system, don't confuse.

    /// write a directory lock
    TxnTimestamp writeFilesysLock(TxnTimestamp txn_id, const String & dir, const String & db, const String & table);
    /// check if a directory is lock
    std::optional<FilesysLock> getFilesysLock(const String & dir);
    /// clean a directory lock
    void clearFilesysLock(const String & dir);
    /// clean a directory lock by transaction id
    void clearFilesysLock(TxnTimestamp txn_id);
    /// get all filesys lock record
    std::vector<FilesysLock> getAllFilesysLock();

    /// For discussion: as parts can be intents, need some extra transaction record check logic. Return all parts and do in server side?
    /// DataPartsVector getDataPartsInPartitions(const StoragePtr & storage, const Strings & partitions, const TxnTimestamp & ts = 0);
    ///DataPartsVector getAllDataParts(const StoragePtr & table, const TxnTimestamp & ts = 0);

    /// End of API for new transaction model (sync with guanzhe)

    void insertTransaction(TxnTimestamp & txnID);

    void removeTransaction(const TxnTimestamp & txnID);

    std::vector<TxnTimestamp> getActiveTransactions();

    /// virtual warehouse related interface
    void updateServerWorkerGroup(const String & vw_name, const String & worker_group_name, const HostWithPortsVec & workers);

    HostWithPortsVec getWorkersInWorkerGroup(const String & worker_group_name);

    /// system tables related interface

    std::optional<DB::Protos::DataModelTable> getTableByID(const Protos::TableIdentifier & identifier);
    DataModelTables getTablesByID(std::vector<std::shared_ptr<Protos::TableIdentifier>> & identifiers);

    DataModelDBs getAllDataBases();

    DataModelTables getAllTables(const String & database_name = "");

    IMetaStore::IteratorPtr getTrashTableIDIterator(uint32_t iterator_internal_batch_size);

    DataModelUDFs getAllUDFs(const String &database_name, const String &function_name);

    DataModelUDFs getUDFByName(const std::unordered_set<String> &function_names);

    std::vector<std::shared_ptr<Protos::TableIdentifier>> getTrashTableID();

    DataModelTables getTablesInTrash();

    DataModelDBs getDatabaseInTrash();

    std::vector<std::shared_ptr<Protos::TableIdentifier>> getAllTablesID(const String & db = "");

    std::shared_ptr<Protos::TableIdentifier> getTableIDByName(const String & db, const String & table);

    DataModelWorkerGroups getAllWorkerGroups();

    DataModelDictionaries getAllDictionaries();

    /// APIs to clear metadata from ByteKV
    void clearDatabaseMeta(const String & database, const UInt64 & ts);

    void clearTableMetaForGC(const String & database, const String & name, const UInt64 & ts);
    void clearDataPartsMeta(const StoragePtr & table, const DataPartsVector & parts, const bool skip_part_cache = false);
    void clearStagePartsMeta(const StoragePtr & table, const DataPartsVector & parts);
    void clearDataPartsMetaForTable(const StoragePtr & table);
    void clearDeleteBitmapsMetaForTable(const StoragePtr & table);

    /**
     * @brief Move specified items into trash.
     *
     * @param skip_part_cache Evict parts caches if set to `false`.
     */
    void moveDataItemsToTrash(const StoragePtr & table, const TrashItems & items, bool skip_part_cache = false);

    /**
     * @brief Delete specified trashed items from catalog.
     */
    void clearTrashItems(const StoragePtr & table, const TrashItems & items);

    /**
     * @brief Get all trashed parts in given table.
     *
     * Trashed items including data parts, staged parts, and deleted bitmaps.
     *
     * @param limit Limit the result retured. Disabled with value `0`.
     * @return Trashed parts.
     */
    TrashItems getDataItemsInTrash(const StoragePtr & storage, const size_t & limit = 0);


    /// APIs to sync data parts for preallocate mode
    std::vector<TxnTimestamp> getSyncList(const StoragePtr & table);
    void clearSyncList(const StoragePtr & table, std::vector<TxnTimestamp> & sync_list);
    ServerDataPartsVector getServerPartsByCommitTime(const StoragePtr & table, std::vector<TxnTimestamp> & sync_list);

    /// APIs for multiple namenode
    void createRootPath(const String & path);
    void deleteRootPath(const String & path);
    std::vector<std::pair<String, UInt32>> getAllRootPath();

    ///APIs for data mutation
    void createMutation(const StorageID & storage_id, const String & mutation_name, const String & mutate_text);
    void removeMutation(const StorageID & storage_id, const String & mutation_name);
    Strings getAllMutations(const StorageID & storage_id);
    std::multimap<String, String> getAllMutations();
    void fillMutationsByStorage(const StorageID & storage_id, std::map<TxnTimestamp, CnchMergeTreeMutationEntry> & out_mutations);


    /// TODO:
    // void addCheckpoint(const StoragePtr &, Checkpoint) { }
    // void markCheckpoint(const StoragePtr &, Checkpoint) { }
    // void removeCheckpoint(const StoragePtr &, Checkpoint) { }
    Checkpoints getCheckpoints() { return {}; }

    /// TODO:
    DataPartsVector getAllDataPartsBetween(const StoragePtr &, const TxnTimestamp &, const TxnTimestamp &) { return {}; }

    void setTableClusterStatus(const UUID & table_uuid, const bool clustered, const UInt64 & table_definition_hash);
    void getTableClusterStatus(const UUID & table_uuid, bool & clustered);
    bool isTableClustered(const UUID & table_uuid);

    /// BackgroundJob related API
    void setBGJobStatus(const UUID & table_uuid, CnchBGThreadType type, CnchBGThreadStatus status);
    std::optional<CnchBGThreadStatus> getBGJobStatus(const UUID & table_uuid, CnchBGThreadType type);
    std::unordered_map<UUID, CnchBGThreadStatus> getBGJobStatuses(CnchBGThreadType type);
    void dropBGJobStatus(const UUID & table_uuid, CnchBGThreadType type);

    void setTablePreallocateVW(const UUID & table_uuid, const String vw);
    void getTablePreallocateVW(const UUID & table_uuid, String & vw);

    /// APIs for MaterializedMySQL
    std::shared_ptr<Protos::MaterializedMySQLManagerMetadata> getOrSetMaterializedMySQLManagerMetadata(const StorageID & storage_id);
    void updateMaterializedMySQLManagerMetadata(const StorageID & storage_id, const Protos::MaterializedMySQLManagerMetadata & metadata);
    void removeMaterializedMySQLManagerMetadata(const UUID & uuid);
    void setMaterializedMySQLBinlogMetadata(const String & binlog_name, const Protos::MaterializedMySQLBinlogMetadata & binlog_data);
    std::shared_ptr<Protos::MaterializedMySQLBinlogMetadata> getMaterializedMySQLBinlogMetadata(const String & binlog_name);
    void removeMaterializedMySQLBinlogMetadata(const String & binlog_name);
    // This API would be used for some DDL actions which need to update both manager metadata as well as binlog metadata
    void updateMaterializedMySQLMetadataInBatch(const Strings & keys, const Strings & values, const Strings & delete_keys);

    /***
     * API to collect all metrics about a table
     */
    /**
     * @brief Get parts metrics (partition level) of a table.
     */
    std::unordered_map<String, PartitionFullPtr> getPartsInfoMetrics(const DB::Protos::DataModelTable & table, bool & is_ready);
    /**
     * @brief Load parts metrics (partition level) snapshots of the table from metastore.
     * It's designed to initialize parts metrics.
     */
    std::unordered_map<String, std::shared_ptr<PartitionMetrics>> loadPartitionMetricsSnapshotFromMetastore(const String & table_uuid);
    /**
     * @brief Persistent a parts metrics (partition level) snapshot to metastore.
     */
    void savePartitionMetricsSnapshotToMetastore(
        const String & table_uuid, const String & partition_id, const Protos::PartitionPartsMetricsSnapshot & snapshot);
    /**
     * @brief Get partition level metrics for a partition by the time `max_commit_time`.
     * This is designed to be called when recalculation happens.
     *
     * @param table_uuid Target table.
     * @param partition_id Target partition id of the table.
     * @param max_commit_time Only the parts that committed before `max_commit_time` will take into account.
     * @param need_abort A lambda function that quickly decide if the callee need to abort ASAP.
     */
    PartitionMetrics::PartitionMetricsStore getPartitionMetricsStoreFromMetastore(
        const String & table_uuid, const String & partition_id, size_t max_commit_time, std::function<bool()> need_abort);
    /**
     * @brief Recalculate the trash items metrics (table level) data of a table from metastore.
     * This is designed to be called when recalculation happens.
     */
    TableMetrics::TableMetricsData getTableTrashItemsMetricsDataFromMetastore(const String & table_uuid, TxnTimestamp ts);
    /**
     * @brief load a trash items (table level) snapshot from metastore.
     * It's designed to initialize trash items metrics.
     */
    Protos::TableTrashItemsMetricsSnapshot loadTableTrashItemsMetricsSnapshotFromMetastore(const String & table_uuid);
    /**
     * @brief Persistent the trash items metrics (table level) snapshot of a table to metastore.
     */
    void saveTableTrashItemsMetricsToMetastore(const String & table_uuid, const Protos::TableTrashItemsMetricsSnapshot & snapshot);

    /// this is periodically called by leader server only
    void updateTopologies(const std::list<CnchServerTopology> & topologies);

    std::list<CnchServerTopology> getTopologies();

    /// Time Travel relate interfaces
    std::vector<UInt64> getTrashDBVersions(const String & database);
    void undropDatabase(const String & database, const UInt64 & ts);

    std::unordered_map<String, UInt64> getTrashTableVersions(const String & database, const String & table);
    void undropTable(const String & database, const String & table, const UInt64 & ts);

    /// Get the key of a insertion label with the KV namespace
    String getInsertionLabelKey(const InsertionLabelPtr & label);
    /// CAS: insert a label with precommitted state
    void precommitInsertionLabel(const InsertionLabelPtr & label);
    /// CAS: change from precommitted state to committed state
    void commitInsertionLabel(InsertionLabelPtr & label);
    void tryCommitInsertionLabel(InsertionLabelPtr & label);
    /// Check the label and delete it
    bool abortInsertionLabel(const InsertionLabelPtr & label, String & message);

    InsertionLabelPtr getInsertionLabel(UUID uuid, const String & name);
    void removeInsertionLabel(UUID uuid, const String & name);
    void removeInsertionLabels(const std::vector<InsertionLabel> & labels);
    /// Scan all the labels under a table
    std::vector<InsertionLabel> scanInsertionLabels(UUID uuid);
    /// Clear all the labels under a table
    void clearInsertionLabels(UUID uuid);

    void createVirtualWarehouse(const String & vw_name, const VirtualWarehouseData & data);
    void alterVirtualWarehouse(const String & vw_name, const VirtualWarehouseData & data);
    bool tryGetVirtualWarehouse(const String & vw_name, VirtualWarehouseData & data);
    std::vector<VirtualWarehouseData> scanVirtualWarehouses();
    void dropVirtualWarehouse(const String & vw_name);

    void createWorkerGroup(const String & worker_group_id, const WorkerGroupData & data);
    void updateWorkerGroup(const String & worker_group_id, const WorkerGroupData & data);
    bool tryGetWorkerGroup(const String & worker_group_id, WorkerGroupData & data);
    std::vector<WorkerGroupData> scanWorkerGroups();
    void dropWorkerGroup(const String & worker_group_id);

    UInt64 getNonHostUpdateTimestampFromByteKV(const UUID & uuid);

    /** Masking policy NOTEs
     * 1. Column masking policy refers to the masking policy data model, it will be applied to multiple columns with 1-n association.
     * 2. Column masking info refers to the relationship between a table's column and the uuid of masking policy applied to it.
     * 3. Database and column in future may have UUID by design instead of name
     */
    // MaskingPolicyExists maskingPolicyExists(const Strings & masking_policy_names);
    // std::vector<std::optional<MaskingPolicyModel>> getMaskingPolicies(const Strings & masking_policy_names);
    // void putMaskingPolicy(MaskingPolicyModel & masking_policy);
    // std::optional<MaskingPolicyModel> tryGetMaskingPolicy(const String & masking_policy_name);
    // MaskingPolicyModel getMaskingPolicy(const String & masking_policy_name);
    // std::vector<MaskingPolicyModel> getAllMaskingPolicy();
    // Strings getMaskingPolicyAppliedTables(const String & masking_policy_name);
    // Strings getAllMaskingPolicyAppliedTables();
    // void dropMaskingPolicies(const Strings & masking_policy_names);

    bool isHostServer(const ConstStoragePtr & storage) const;

    std::pair<bool, HostWithPorts> checkIfHostServer(const StoragePtr & storage) const;

    void setMergeMutateThreadStartTime(const StorageID & storage_id, const UInt64 & startup_time) const;

    UInt64 getMergeMutateThreadStartTime(const StorageID & storage_id) const;

    void setAsyncQueryStatus(const String & id, const Protos::AsyncQueryStatus & status) const;

    void markBatchAsyncQueryStatusFailed(std::vector<Protos::AsyncQueryStatus> & statuses, const String & reason) const;

    /// TODO(WangTao): consider add some scan/delete interfaces for ops.
    bool tryGetAsyncQueryStatus(const String & id, Protos::AsyncQueryStatus & status) const;

    std::vector<Protos::AsyncQueryStatus> getIntermidiateAsyncQueryStatuses() const;

    // Interfaces to support s3 storage
    // Delete detached parts and detached bitmaps from 'from_tbl' with `detached_part_names`.
    // Write to `to_tbl` attached parts with `parts` and `staged_parts`
    // Write to `to_tbl` attached bitmaps with `bitmaps`
    void attachDetachedParts(
        const StoragePtr & from_tbl,
        const StoragePtr & to_tbl,
        const Strings & detached_part_names,
        const IMergeTreeDataPartsVector & parts,
        const IMergeTreeDataPartsVector & staged_parts,
        const DeleteBitmapMetaPtrVector & detached_bitmaps,
        const DeleteBitmapMetaPtrVector & bitmaps);
    // Delete parts from `from_tbl` with `attached_parts` and `attached_staged_parts`, write detached part meta to `to_tbl` with parts, if parts is nullptr, skip this write
    // Delete bitmaps from `from_tbl` with `attached_bitmaps`, write detached bitmaps to `to_tbl` with `bitmaps`
    void detachAttachedParts(
        const StoragePtr & from_tbl,
        const StoragePtr & to_tbl,
        const IMergeTreeDataPartsVector & attached_parts,
        const IMergeTreeDataPartsVector & attached_staged_parts,
        const IMergeTreeDataPartsVector & parts,
        const DeleteBitmapMetaPtrVector & attached_bitmaps,
        const DeleteBitmapMetaPtrVector & bitmaps);
    // Rename part's meta for `tbl`, from detached to active
    // Rename delete bitmap's meta for `tbl`, from detached to active
    void attachDetachedPartsRaw(const StoragePtr & tbl, const std::vector<String> & part_names, const std::vector<String> & bitmap_names);
    // Rename part's meta from `from_tbl` with attached_part_names, and write to `to_uuid`'s detached parts,
    // first element of `detached_part_metas` is detached part name, second element of `detached_part_metas` is detached part meta
    // Rename bitmap's meta from `from_tbl` with attached_bitmap_names, and write to `to_uuid`'s detached bitmaps,
    // first element of `detached_bitmap_metas` is detached bitmap name, second element of `detached_bitmap_metas` is detached bitmap meta
    void detachAttachedPartsRaw(
        const StoragePtr & from_tbl,
        const String & to_uuid,
        const std::vector<String> & attached_part_names,
        const std::vector<std::pair<String, String>> & detached_part_metas,
        const std::vector<String> & attached_bitmap_names,
        const std::vector<std::pair<String, String>> & detached_bitmap_metas);
    ServerDataPartsVector listDetachedParts(const MergeTreeMetaBase & storage, const AttachFilter & filter);

    DeleteBitmapMetaPtrVector listDetachedDeleteBitmaps(const MergeTreeMetaBase & storage, const AttachFilter & filter);

    // Access Entities
    std::optional<AccessEntityModel> tryGetAccessEntity(EntityType type, const String & name);
    std::vector<AccessEntityModel> getAllAccessEntities(EntityType type);
    std::optional<String> tryGetAccessEntityName(const UUID & uuid);
    void dropAccessEntity(EntityType type, const UUID & uuid, const String & name);
    void putAccessEntity(EntityType type, AccessEntityModel & new_access_entity, AccessEntityModel & old_access_entity, bool replace_if_exists = true);

private:
    Poco::Logger * log = &Poco::Logger::get("Catalog");
    Context & context;
    MetastoreProxyPtr meta_proxy;
    const String name_space;
    String topology_key;

    UInt32 max_commit_size_one_batch {2000};
    std::unordered_map<UUID, std::shared_ptr<std::mutex>> nhut_mutex;
    std::mutex all_storage_nhut_mutex;
    UInt32 max_drop_size_one_batch {10000};

    std::shared_ptr<Protos::DataModelDB> tryGetDatabaseFromMetastore(const String & database, const UInt64 & ts);
    std::shared_ptr<Protos::DataModelTable>
    tryGetTableFromMetastore(const String & table_uuid, const UInt64 & ts, bool with_prev_versions = false, bool with_deleted = false);
    Strings tryGetDependency(const ASTPtr & create_query);
    static void replace_definition(Protos::DataModelTable & table, const String & db_name, const String & table_name);
    StoragePtr createTableFromDataModel(const Context & session_context, const Protos::DataModelTable & data_model);
    void detachOrAttachTable(const String & db, const String & name, const TxnTimestamp & ts, bool is_detach);
    DataModelPartPtrVector getDataPartsMetaFromMetastore(
        const ConstStoragePtr & storage, const Strings & required_partitions, const Strings & full_partitions, const TxnTimestamp & ts);
    void detachOrAttachDictionary(const String & db, const String & name, bool is_detach);
    void moveTableIntoTrash(
        Protos::DataModelTable & table,
        Protos::TableIdentifier & table_id,
        const TxnTimestamp & txnID,
        const TxnTimestamp & ts,
        BatchCommitRequest & batchWrite);
    void restoreTableFromTrash(
        std::shared_ptr<Protos::TableIdentifier> table_id, const UInt64 & ts, BatchCommitRequest & batch_write);

    void clearDataPartsMetaInternal(
        const StoragePtr & table, const DataPartsVector & parts, const DeleteBitmapMetaPtrVector & delete_bitmaps = {});

    Strings getPartitionIDsFromMetastore(const ConstStoragePtr & storage);

    void mayUpdateUHUT(const StoragePtr & storage);

    bool canUseCache(const ConstStoragePtr & storage, const Context * session_context);

    void finishCommitInBatch(
        const StoragePtr & storage,
        const TxnTimestamp & txnID,
        const Protos::DataModelPartVector & parts,
        const DeleteBitmapMetaPtrVector & delete_bitmaps,
        const Protos::DataModelPartVector & staged_parts,
        const bool preallocate_mode,
        const std::vector<String> & expected_parts,
        const std::vector<String> & expected_bitmaps,
        const std::vector<String> & expected_staged_parts);

    void finishCommitInternal(
        const StoragePtr & storage,
        const google::protobuf::RepeatedPtrField<Protos::DataModelPart> & parts_to_commit,
        const DeleteBitmapMetaPtrVector & delete_bitmaps,
        const google::protobuf::RepeatedPtrField<Protos::DataModelPart> & staged_parts,
        const UInt64 & txnid,
        const bool preallocate_mode,
        const std::vector<String> & expected_parts,
        const std::vector<String> & expected_bitmaps,
        const std::vector<String> & expected_staged_parts);

    /// check if the table can be dropped safely. It's not allowed to drop a table if other tables rely on it.
    /// If is_dropping_db set to true, we can ignore any dependency under the same database.
    void checkCanbeDropped(Protos::TableIdentifier & table_id, bool is_dropping_db);

    void assertLocalServerThrowIfNot(const StoragePtr & storage) const;

    /// Data models whose key begins with 'PartitionID_' and ends with '_TxnID' can share the following logic,
    /// currently used by data parts and delete bitmaps
    template <typename T>
    std::vector<T> getDataModelsByPartitions(
        const ConstStoragePtr & storage,
        const String & meta_prefix,
        const Strings & partitions,
        const Strings & full_partitions_,
        const std::function<T(const String &)> & create_func,
        const TxnTimestamp & ts,
        UInt32 time_out_ms = 0)
    {
        Stopwatch watch;
        std::vector<T> res;

        String table_uuid = UUIDHelpers::UUIDToString(storage->getStorageID().uuid);
        UInt64 timestamp = ts.toUInt64();

        std::set<String, partition_comparator> request_partitions;
        std::set<String, partition_comparator> full_partitions;
        for (const auto & partition : partitions)
            request_partitions.insert(partition);
        for (const auto & partition : full_partitions_)
            full_partitions.insert(partition);

        auto plist_start = full_partitions.begin();
        auto plist_end = full_partitions.end();

        for (auto partition_it = request_partitions.begin(); partition_it != request_partitions.end();)
        {
            IMetaStore::IteratorPtr mIt;
            while (plist_start != plist_end && *partition_it != *plist_start)
                plist_start++;

            if (plist_start == plist_end)
            {
                mIt = meta_proxy->getMetaInRange(meta_prefix, *partition_it + "_", *partition_it + "_", true, true);
                partition_it++;
            }
            else
            {
                auto & start_partition = *partition_it;
                String end_partition = *partition_it;
                while (plist_start != plist_end && partition_it != request_partitions.end() && *partition_it == *plist_start)
                {
                    end_partition = *partition_it;
                    partition_it++;
                    plist_start++;
                }
                mIt = meta_proxy->getMetaInRange(meta_prefix, start_partition + "_", end_partition + "_", true, true);
            }

            size_t counter = 0;
            while (mIt->next())
            {
                if (time_out_ms && !(++counter%20000) && watch.elapsedMilliseconds()>time_out_ms)
                {
                    throw Exception("Get data from metastore reached timeout " + toString(time_out_ms) + "ms.", ErrorCodes::TIMEOUT_EXCEEDED);
                }
                /// if timestamp is set, only return the meta where commit time <= timestamp
                if (timestamp)
                {
                    const auto & key = mIt->key();
                    auto pos = key.find_last_of('_');
                    if (pos != String::npos)
                    {
                        UInt64 commit_time = std::stoull(key.substr(pos + 1, String::npos), nullptr);
                        if (commit_time > timestamp)
                            continue;
                    }
                }

                T data_model = create_func(mIt->value());
                if (data_model)
                    res.emplace_back(std::move(data_model));
            }
        }

        return res;
    }

    /// Check status for part, or deleted bitmap or staged part
    template <typename T, typename GenerateKeyFunc>
    void checkItemsStatus(const std::vector<T> & items, GenerateKeyFunc&& generateKey, std::vector<size_t> & items_to_remove, std::vector<String> & expected_items)
    {
        auto item_status = meta_proxy->getItemStatus<T>(items, generateKey);
        for (size_t i = 0; i < item_status.size(); i++) {
            if (item_status[i].second == 0) // status 0 indicates not exist
                items_to_remove.push_back(i);
            else
                expected_items.emplace_back(std::move(item_status[i].first));
        }
    }
};

template<typename T>
void remove_not_exist_items(std::vector<T> & items_to_write, std::vector<size_t> & items_not_exist)
{
    size_t item_id {0};
    items_to_write.erase(std::remove_if(items_to_write.begin(), items_to_write.end(), [&](auto & elem) {
        if (item_id != items_not_exist.size() && (&elem - &items_to_write[0]) == static_cast<long>(items_not_exist[item_id])) {
            ++item_id;
            return true;
        }
        return false;
    }), items_to_write.end());
}

using CatalogPtr = std::shared_ptr<Catalog>;
void notifyOtherServersOnAccessEntityChange(const Context & context, EntityType type, const String & tenanted_name, const UUID & uuid, Poco::Logger * log);
void fillUUIDForDictionary(DB::Protos::DataModelDictionary &);
}
