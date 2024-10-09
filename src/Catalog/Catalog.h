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

#include <Common/Logger.h>
#include <atomic>
#include <map>
#include <optional>
#include <set>
#include <Backups/BackupStatus.h>
#include <Catalog/CatalogUtils.h>
#include <Catalog/CatalogSettings.h>
#include <Catalog/DataModelPartWrapper.h>
#include <Catalog/MetastoreProxy.h>
#include <Core/SettingsEnums.h>
#include <Core/Types.h>
#include <Databases/Snapshot.h>
#include <Protos/DataModelHelpers.h>
#include <Protos/cnch_common.pb.h>
#include <Protos/cnch_server_rpc.pb.h>
#include <Protos/data_models.pb.h>
#include <Statistics/ExportSymbols.h>
#include <Statistics/StatisticsBase.h>
// #include <Transaction/ICnchTransaction.h>
#include <Catalog/IMetastore.h>
#include <Interpreters/DistributedStages/PlanSegmentInstance.h>
#include <ResourceManagement/CommonData.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/MergeTree/CnchMergeTreeMutationEntry.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h>
#include <Storages/StorageSnapshot.h>
#include <Transaction/CnchServerTransaction.h>
#include <Storages/TableDefinitionHash.h>
#include <Transaction/TxnTimestamp.h>
#include <cppkafka/cppkafka.h>
#include "common/types.h"
#include <Common/Config/MetastoreConfig.h>
#include <Common/Configurations.h>
#include <Common/DNSResolver.h>
#include <Common/Exception.h>
#include <Common/HostWithPorts.h>
#include <common/getFQDNOrHostName.h>
#include <Storages/TableMetaEntry.h>

namespace DB::ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
} // namespace DB::ErrorCodes

namespace DB
{
class AttachFilter;
struct PrunedPartitions;
using DataModelPartPtr = std::shared_ptr<Protos::DataModelPart>;
using DataModelPartPtrVector = std::vector<DataModelPartPtr>;

struct DataModelPartWithName;
using DataModelPartWithNamePtr = std::shared_ptr<DataModelPartWithName>;
using DataModelPartWithNameVector = std::vector<DataModelPartWithNamePtr>;

using BackupTaskModel = std::shared_ptr<Protos::DataModelBackupTask>;
using BackupTaskModels = std::vector<BackupTaskModel>;
}

namespace DB::Catalog
{

enum class VisibilityLevel
{
    // all items visible to xid (commit_ts <= xid && end_ts > xid)
    Visible,
    // all items committed before xid, including invisible items (end_ts <= xid)
    Committed,
    // all items written before xid, including intermediate uncommitted items
    All
};

class CatalogBackgroundTask;

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

    Catalog(Context & _context, const MetastoreConfig & config, String _name_space = "default", bool writable = true);

    ~Catalog() = default;

    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config);

    MetastoreProxy::MetastorePtr getMetastore();

    /// update optimizer stats
    void updateTableStatistics(const String & uuid, const std::unordered_map<StatisticsTag, StatisticsBasePtr> & data);

    /// get the latest stats
    std::unordered_map<StatisticsTag, StatisticsBasePtr> getTableStatistics(const String & uuid);

    /// remove tags
    void removeTableStatistics(const String & uuid);

    /// stats for Column

    /// update optimizer stats
    void updateColumnStatistics(
        const String & uuid, const String & column, const std::unordered_map<StatisticsTag, StatisticsBasePtr> & data);

    /// get the latest stats
    std::unordered_map<StatisticsTag, StatisticsBasePtr> getColumnStatistics(const String & uuid, const String & column);
    std::unordered_map<String, std::unordered_map<StatisticsTag, StatisticsBasePtr>> getAllColumnStatistics(const String & uuid);

    std::vector<String> getAllColumnStatisticsKey(const String & uuid);

    /// remove tags
    void removeColumnStatistics(const String & uuid, const String & column);
    void removeAllColumnStatistics(const String & uuid);

    //////////////

    void updateSQLBinding(SQLBindingItemPtr data);

    SQLBindings getSQLBindings();

    SQLBindings getReSQLBindings(const bool & is_re_expression);

    SQLBindingItemPtr getSQLBinding(const String & uuid, const String & tenant_id, const bool & is_re_expression);

    void removeSQLBinding(const String & uuid, const String & tenant_id, const bool & is_re_expression);

    void updatePreparedStatement(const PreparedStatementItemPtr & data);

    PreparedStatements getPreparedStatements();

    PreparedStatementItemPtr getPreparedStatement(const String & name);

    void removePreparedStatement(const String & name);

    /////////////////////////////
    /// Database related API
    /////////////////////////////

    void createDatabase(const String & database, const UUID & uuid, const TxnTimestamp & txnID, const TxnTimestamp & ts,
                        const String & create_query = "", const String & engine_name = "", enum TextCaseOption text_case_option = TextCaseOption::MIXED);

    DatabasePtr getDatabase(const String & database, const ContextPtr & context, const TxnTimestamp & ts = 0);

    bool isDatabaseExists(const String & database, const TxnTimestamp & ts = 0);

    void dropDatabase(const String & database, const TxnTimestamp & previous_version, const TxnTimestamp & txnID, const TxnTimestamp & ts);

    void renameDatabase(const UUID & uuid, const String & from_database, const String & to_database, const TxnTimestamp & txnID, const TxnTimestamp & ts);

    ///currently only used for materialized mysql
    void alterDatabase(const String & alter_database, const TxnTimestamp & txnID, const TxnTimestamp & ts,
                       const String & create_query = "", const String & engine_name = "");

    /////////////////////////////
    /// Backup related API
    /////////////////////////////

    void createBackupJob(
        const String & backup_uuid,
        UInt64 create_time,
        BackupStatus backup_status,
        const String & serialized_ast,
        const String & server_address,
        bool enable_auto_recover);

    void updateBackupJobCAS(BackupTaskModel & backup_task, const String & expected_value);

    BackupTaskModel tryGetBackupJob(const String & backup_uuid);

    BackupTaskModels getAllBackupJobs();

    void removeBackupJob(const String & backup_uuid);

    /////////////////////////////
    /// Snapshots related API
    /////////////////////////////

    /// Create snapshot in `db`, with the specified name, ts, etc.
    /// Throws if name already exists.
    void createSnapshot(
        const UUID & db,
        const String & snapshot_name,
        const TxnTimestamp & ts,
        int ttl_in_days,
        UUID bind_table);

    void removeSnapshot(const UUID & db, const String & snapshot_name);

    /// Return snapshot if exist, nullptr otherwise.
    SnapshotPtr tryGetSnapshot(const UUID & db, const String & snapshot_name);

    /// Return all snapshots in `db`, in ascending order of snapshot time.
    /// If `table_filter` is not null, exclude snapshot that binds to other table from the result.
    Snapshots getAllSnapshots(const UUID & db, UUID * table_filter = nullptr);

    /////////////////////////////
    /// Table related API
    /////////////////////////////

    void createTable(
        const UUID & db_uuid,
        const StorageID & storage_id,
        const String & create_query,
        const String & virtual_warehouse,
        const TxnTimestamp & txnID,
        const TxnTimestamp & ts);

    void dropTable(
        const UUID & db_uuid,
        const StoragePtr & storage,
        const TxnTimestamp & previous_version,
        const TxnTimestamp & txnID,
        const TxnTimestamp & ts);

    void createUDF(const String & prefix_name, const String & name, const String & create_query);

    void dropUDF(const String & resolved_name);

    void detachTable(const String & db, const String & name, const TxnTimestamp & ts);

    void attachTable(const String & db, const String & name, const TxnTimestamp & ts);

    bool isTableExists(const String & db, const String & name, const TxnTimestamp & ts = 0);

    void restoreTableHistoryVersion(Protos::DataModelTable history_table);

    void alterTable(
        const Context & query_context,
        const Settings & query_settings,
        const StoragePtr & storage,
        const String & new_create,
        const TxnTimestamp & previous_version,
        const TxnTimestamp & txnID,
        const TxnTimestamp & ts,
        const bool is_modify_cluster_by);

    void renameTable(
        const Settings & query_settings,
        const String & from_database,
        const String & from_table,
        const String & to_database,
        const String & to_table,
        const UUID & to_db_uuid,
        const TxnTimestamp & txnID,
        const TxnTimestamp & ts);

    void setWorkerGroupForTable(const String & db, const String & name, const String & worker_group, UInt64 worker_topology_hash);

    StoragePtr getTable(const Context & query_context, const String & database, const String & name, const TxnTimestamp & ts = TxnTimestamp::maxTS());

    StoragePtr tryGetTable(const Context & query_context, const String & database, const String & name, const TxnTimestamp & ts = TxnTimestamp::maxTS());

    StoragePtr tryGetTableByUUID(const Context & query_context, const String & uuid, const TxnTimestamp & ts, bool with_delete = false);

    StoragePtr getTableByUUID(const Context & query_context, const String & uuid, const TxnTimestamp & ts, bool with_delete = false);

    Strings getTablesInDB(const String & database);

    Strings getTableAllPreviousDefinitions(const String & table_uuid);

    std::vector<StoragePtr> getAllViewsOn(const Context & session_context, const StoragePtr & storage, const TxnTimestamp & ts);

    void setTableActiveness(const StoragePtr & storage, const bool is_active, const TxnTimestamp & ts);
    /// return true if table is active, false otherwise
    bool getTableActiveness(const StoragePtr & storage, const TxnTimestamp & ts);

    /////////////////////////////
    /// Data parts API
    /////////////////////////////

    /**
     * @brief Get the Server Data Parts In Partitions With Delete Bitmap Metas.
     * For data consistency, data parts and delete bitmap metas must use the same transaction records to filter out. Otherwise it will get incorrect result.
     * Please see more detail in doc: https://bytedance.larkoffice.com/docx/Xo52dhoMnofCROxvXUUceyK9nQd
     *
     * @param bucket_numbers If empty fetch all bucket_numbers (by default),
     * otherwise fetch the given bucket_numbers.
     */

    ServerDataPartsWithDBM getAllServerDataPartsWithDBM(
        const ConstStoragePtr & storage,
        const TxnTimestamp & ts,
        const Context * session_context,
        VisibilityLevel visibility = VisibilityLevel::Visible);
    ServerDataPartsWithDBM getServerDataPartsInPartitionsWithDBM(
        const ConstStoragePtr & storage,
        const Strings & partitions,
        const TxnTimestamp & ts,
        const Context * session_context,
        VisibilityLevel visibility = VisibilityLevel::Visible,
        const std::set<Int64> & bucket_numbers = {});

    /// @param bucket_numbers If empty fetch all bucket_numbers, otherwise fetch the given bucket_numbers.
    ServerDataPartsVector getServerDataPartsInPartitions(
        const ConstStoragePtr & storage,
        const Strings & partitions,
        const TxnTimestamp & ts,
        const Context * session_context,
        VisibilityLevel visibility = VisibilityLevel::Visible,
        const std::set<Int64> & bucket_numbers = {});

    ServerDataPartsWithDBM getTrashedPartsInPartitionsWithDBM(const ConstStoragePtr & storage, const Strings & partitions, const TxnTimestamp & ts);

    ServerDataPartsVector getTrashedPartsInPartitions(const ConstStoragePtr & storage, const Strings & partitions, const TxnTimestamp & ts, VisibilityLevel visibility = VisibilityLevel::Visible);

    bool hasTrashedPartsInPartition(const ConstStoragePtr & storage, const String & partition);

    ServerDataPartsVector getAllServerDataParts(const ConstStoragePtr & storage, const TxnTimestamp & ts, const Context * session_context, VisibilityLevel visibility = VisibilityLevel::Visible);
    DataPartsVector getDataPartsByNames(const NameSet & names, const StoragePtr & table, const TxnTimestamp & ts);
    DataPartsVector getStagedDataPartsByNames(const NameSet & names, const StoragePtr & table, const TxnTimestamp & ts);
    DeleteBitmapMetaPtrVector getAllDeleteBitmaps(const MergeTreeMetaBase & storage);

    // return table's committed staged parts. if partitions != null, ignore staged parts not belong to `partitions`.
    DataPartsVector getStagedParts(const ConstStoragePtr & table, const TxnTimestamp & ts, const NameSet * partitions = nullptr);

    ServerDataPartsWithDBM getStagedServerDataPartsWithDBM(const ConstStoragePtr & table, const TxnTimestamp & ts, const NameSet * partitions = nullptr);
    ServerDataPartsVector getStagedServerDataParts(const ConstStoragePtr & table, const TxnTimestamp & ts, const NameSet * partitions = nullptr, VisibilityLevel visibility = VisibilityLevel::Visible);

    /////////////////////////////
    /// Delete bitmaps API (UNIQUE KEY)
    /////////////////////////////

    /// fetch all delete bitmaps <= ts in the given partitions
    ///
    /// @param bucket_numbers If empty fetch all bucket_numbers, otherwise fetch the given bucket_numbers.
    DeleteBitmapMetaPtrVector getDeleteBitmapsInPartitions(
        const ConstStoragePtr & storage,
        const Strings & partitions,
        const TxnTimestamp & ts,
        const Context * session_context = nullptr,
        VisibilityLevel visibility = VisibilityLevel::Visible,
        const std::set<Int64> & bucket_numbers = {});
    DeleteBitmapMetaPtrVector getDeleteBitmapsInPartitionsFromMetastore(
        const ConstStoragePtr & storage, const Strings & partitions, const TxnTimestamp & ts, VisibilityLevel visibility = VisibilityLevel::Visible);
    DeleteBitmapMetaPtrVector getTrashedDeleteBitmapsInPartitions(
        const ConstStoragePtr & storage, const Strings & partitions, const TxnTimestamp & ts, VisibilityLevel visibility = VisibilityLevel::Visible);

    /// get bitmaps by keys
    DeleteBitmapMetaPtrVector getDeleteBitmapByKeys(const StoragePtr & storage, const NameSet & keys);

    // V1 part commit API
    void finishCommit(
        const StoragePtr & table,
        const TxnTimestamp & txnID,
        const TxnTimestamp & commit_ts,
        const DataPartsVector & parts,
        const DeleteBitmapMetaPtrVector & delete_bitmaps = {},
        const bool is_merged_parts = false,
        const bool preallocate_mode = false,
        const bool write_manifest = false);

    /// APIs for CncnKafka
    void getKafkaOffsets(const String & consumer_group, cppkafka::TopicPartitionList & tpl);
    cppkafka::TopicPartitionList getKafkaOffsets(const String & consumer_group, const String & kafka_topic);
    void clearOffsetsForWholeTopic(const String & topic, const String & consumer_group);
    void setTransactionForKafkaConsumer(const UUID & uuid, const TxnTimestamp & txn_id, size_t consumer_index);
    TxnTimestamp getTransactionForKafkaConsumer(const UUID & uuid, size_t consumer_index);
    void clearKafkaTransactions(const UUID & uuid);

    void dropAllPart(const StoragePtr & storage, const TxnTimestamp & txnID, const TxnTimestamp & ts);

    std::vector<std::shared_ptr<MergeTreePartition>> getPartitionList(const ConstStoragePtr & table, const Context * session_context);
    PartitionWithGCStatus getPartitionsWithGCStatus(const StoragePtr & table, const Strings & required_partitions);

    std::vector<Protos::LastModificationTimeHint> getLastModificationTimeHints(const ConstStoragePtr & table);

    /// Caller should garrantee that `lock_holder` lives longer than this call.
    template <typename Map>
    void getPartitionsFromMetastore(const MergeTreeMetaBase & table, Map & partition_list, std::shared_ptr<MetaLockHolder> lock_holder);

    Strings getPartitionIDs(const ConstStoragePtr & storage, const Context * session_context);

    PrunedPartitions getPartitionsByPredicate(ContextPtr session_context, const ConstStoragePtr & storage, const SelectQueryInfo & query_info, const Names & column_names_to_return, const bool & ignore_ttl);
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
    bool commitTransactionWithNewTableVersion(const TransactionCnchPtr & txn, const UInt64 & table_version);

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
        const bool preallocate_mode = false,
        const bool write_manifest = false);

    /// set commit time for parts and delete bitmaps
    void setCommitTime(
        const StoragePtr & table,
        const CommitItems & commit_data,
        const TxnTimestamp & commitTs,
        const UInt64 txn_id = 0);

    void clearParts(const StoragePtr & table, const CommitItems & commit_data);

    /// write undo buffer before write vfs
    void writeUndoBuffer(
        const StorageID & storage_id, const TxnTimestamp & txnID, const UndoResources & resources, PlanSegmentInstanceId instance_id = {});
    void writeUndoBuffer(
        const StorageID & storage_id, const TxnTimestamp & txnID, UndoResources && resources, PlanSegmentInstanceId instance_id = {});

    /// clear undo buffer
    void clearUndoBuffer(const TxnTimestamp & txnID);

    /// clear undo buffer
    void clearUndoBuffer(const TxnTimestamp & txnID, const String & rpc_address, PlanSegmentInstanceId instance_id);

    /**
     * @brief Clean all undo buffers with given keys (in the same table).
     *
     * @param txnID Currently, this will be used to verify if the undo buffers are in the same table.
     * @param keys Keys of the undo buffers.
     */
    void clearUndoBuffersByKeys(const TxnTimestamp & txnID, const std::vector<String> & keys);

    /**
     * @brief get Undo Buffers with there keys (in metastore). These keys can be further used to manipulate the data.
     *
     * @param txnID Transaction ID.
     * @return map<table_uuid, ([keys in metastore of each undo buffer], [undo buffers])>
     */
    std::unordered_map<String, std::pair<std::vector<String>, UndoResources>> getUndoBuffersWithKeys(const TxnTimestamp & txnID);
    /// return storage uuid -> undo resources
    std::unordered_map<String, UndoResources> getUndoBuffer(const TxnTimestamp & txnID);
    std::unordered_map<String, UndoResources>
    getUndoBuffer(const TxnTimestamp & txnID, const String & rpc_address, PlanSegmentInstanceId instance_id);
    /// execute undos, returns whether clean_fs_lock_by_scan is true
    uint32_t applyUndos(
        const TransactionRecord & txn_record, const StoragePtr & table, const UndoResources & resources, bool & clean_fs_lock_by_scan);

    /// return txn_id -> undo resources
    std::unordered_map<UInt64, UndoResources> getAllUndoBuffer();

    class UndoBufferIterator
    {
    public:
        UndoBufferIterator(IMetaStore::IteratorPtr metastore_iter, LoggerPtr log);
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
        LoggerPtr log;
    };

    UndoBufferIterator getUndoBufferIterator() const;


    /**
     * @brief Try to create table meta proactively.
     *        This function is used to help MV to get
     *        table meta without waiting for lazy loading.
     *
     * @param uuid Table uuid.
     * @param host_port Host server for the table.
     */
    void notifyTableCreated(const UUID & uuid, const HostWithPorts & host_port) noexcept;

    /// get transaction records, if the records exists, we can check with the transaction coordinator to detect zombie record.
    /// the transaction record will be cleared only after all intents have been cleared and set commit time for all parts.
    /// For zombie record, the intents to be clear can be scanned from intents space with txnid. The parts can be get from undo buffer.
    std::vector<TransactionRecord> getTransactionRecords();
    std::vector<TransactionRecord> getTransactionRecords(const std::vector<TxnTimestamp> & txn_ids, size_t batch_size = 0);
    /// clean zombie records. If the total transaction record number is too large, it may be impossible to get all of them. We can
    /// pass a max_result_number to only get part of them and clean zombie records repeatedlly
    std::vector<TransactionRecord> getTransactionRecordsForGC(String & start_key, size_t max_result_number);
    TransactionRecords getTransactionRecords(const ServerDataPartsVector & parts, const DeleteBitmapMetaPtrVector & bitmaps);

    /// Clear intents written by zombie transaction.
    void clearZombieIntent(const TxnTimestamp & txnID);

    /// Below method provides method to locks a directory during `ALTER ATTACH PARTS FROM ... ` query. If there's a lock record
    /// that mean there's some manipulation are being done on that directory by cnch, so user should not use it in other query
    /// (that will eventualy fail). **NOTES: it DOES NOT belong to general cnch lock system, don't confuse.

    /// write a directory lock
    TxnTimestamp writeFilesysLock(TxnTimestamp txn_id, const String & dir, const IStorage& storage);
    /// clean a directory lock, if txn_id is not a nullopt, will check if these locks are owned by this txn
    void clearFilesysLocks(const std::vector<String>& dirs, std::optional<TxnTimestamp> txn_id);
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
    DataModelTables getTablesByIDs(const std::vector<std::shared_ptr<Protos::TableIdentifier>> & identifiers);

    DataModelDBs getAllDataBases();

    DataModelTables getAllTables(const String & database_name = "");

    DataModelUDFs getAllUDFs(const String & prefix_name, const String & function_name);

    DataModelUDFs getUDFByName(const std::unordered_set<String> & function_names);

    /////////////////////////////
    /// Trash API
    /////////////////////////////

    IMetaStore::IteratorPtr getTrashTableIDIterator(uint32_t iterator_internal_batch_size);

    std::vector<std::shared_ptr<Protos::TableIdentifier>> getTrashTableID();

    DataModelTables getTablesInTrash();

    /// Get all versions of metadata (in commit ts order) for the given table.
    DataModelTables getTableHistories(const String & table_uuid);

    /**
     * @return ts -> table id of trash table
     */
    std::map<UInt64, std::shared_ptr<Protos::TableIdentifier>> getTrashTableVersions(const String & database, const String & table);

    DataModelDBs getDatabaseInTrash();

    std::vector<std::shared_ptr<Protos::TableIdentifier>> getAllTablesID(const String & db = "");
    std::vector<std::shared_ptr<Protos::TableIdentifier>> getTablesIDByTenant(const String & tenant_id);

    std::shared_ptr<Protos::TableIdentifier> getTableIDByName(const String & db, const String & table);
    std::shared_ptr<std::vector<std::shared_ptr<Protos::TableIdentifier>>> getTableIDsByNames(const std::vector<std::pair<String, String>> & db_table_pairs);

    DataModelWorkerGroups getAllWorkerGroups();

    DataModelDictionaries getAllDictionaries();

    /// APIs to clear metadata from ByteKV
    void clearDatabaseMeta(const String & database, const UInt64 & ts);

    void clearTableMetaForGC(const String & database, const String & name, const UInt64 & ts);
    void clearDataPartsMeta(const StoragePtr & storage, const DataPartsVector & parts);
    void clearStagePartsMeta(const StoragePtr & storage, const ServerDataPartsVector & parts);
    void clearDataPartsMetaForTable(const StoragePtr & table);
    void clearMutationEntriesForTable(const StoragePtr & storage);
    void clearDeleteBitmapsMetaForTable(const StoragePtr & table);

    /**
     * @brief Move specified items into trash.
     *
     * @param is_zombie_with_staging_txn_id If true, just remove items.data_parts' kv entry
     */
    void moveDataItemsToTrash(const StoragePtr & table, const TrashItems & items, bool is_zombie_with_staging_txn_id = false);

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
     * @param start_key When provided, KV scan will start from the `start_key`,
     * and it will be updated after calling this method.
     * @return Trashed parts.
     */
    TrashItems getDataItemsInTrash(const StoragePtr & storage, const size_t & limit = 0, String * start_key = nullptr);

    void markPartitionDeleted(const StoragePtr & table, const Strings & partitions);
    void deletePartitionsMetadata(const StoragePtr & table, const PartitionWithGCStatus & partitions);

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

    void setTableClusterStatus(const UUID & table_uuid, const bool clustered, const TableDefinitionHash & table_definition_hash);
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
    TableMetrics::TableMetricsData
    getTableTrashItemsMetricsDataFromMetastore(const String & table_uuid, TxnTimestamp ts, std::function<bool()> need_abort);
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

    // materialized view meta.
    BatchCommitRequest constructMvMetaRequests(const String & uuid,
            std::vector<std::shared_ptr<Protos::VersionedPartition>> add_partitions, std::vector<std::shared_ptr<Protos::VersionedPartition>> drop_partitions, String mv_version_ts);
    String getMvMetaVersion(const String & uuid);
    std::vector<std::shared_ptr<Protos::VersionedPartitions>> getMvBaseTables(const String & uuid);
    void updateMvMeta(const String & uuid, std::vector<std::shared_ptr<Protos::VersionedPartitions>> versioned_partitions);
    void dropMvMeta(const String & uuid, std::vector<std::shared_ptr<Protos::VersionedPartitions>> versioned_partitions);
    void cleanMvMeta(const String & uuid);

    /// Time Travel relate interfaces
    std::vector<UInt64> getTrashDBVersions(const String & database);
    void undropDatabase(const String & database, const UInt64 & ts);

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

    String getDictionaryBucketUpdateTimeKey(const StorageID & storage_id, Int64 bucket_number) const;

    String getByKey(const String & key);

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
        const DeleteBitmapMetaPtrVector & bitmaps,
        const UInt64 & txn_id);
    // Delete parts from `from_tbl` with `attached_parts` and `attached_staged_parts`, write detached part meta to `to_tbl` with parts, if parts is nullptr, skip this write
    // Delete bitmaps from `from_tbl` with `attached_bitmaps`, write detached bitmaps to `to_tbl` with `bitmaps`
    void detachAttachedParts(
        const StoragePtr & from_tbl,
        const StoragePtr & to_tbl,
        const IMergeTreeDataPartsVector & attached_parts,
        const IMergeTreeDataPartsVector & attached_staged_parts,
        const IMergeTreeDataPartsVector & parts,
        const DeleteBitmapMetaPtrVector & attached_bitmaps,
        const DeleteBitmapMetaPtrVector & bitmaps,
        const UInt64 & txn_id);
    // Rename part's meta for `tbl`, from detached to active
    // Rename delete bitmap's meta for `tbl`, from detached to active
    void attachDetachedPartsRaw(
        const StoragePtr & tbl,
        const std::vector<String> & part_names,
        size_t detached_visible_part_size,
        size_t detached_staged_part_size,
        const std::vector<String> & bitmap_names);
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

    // Append partial object column schema in Txn
    void
    appendObjectPartialSchema(const StoragePtr & table, const TxnTimestamp & txn_id, const MutableMergeTreeDataPartsCNCHVector & parts);
    ObjectAssembledSchema tryGetTableObjectAssembledSchema(const UUID & table_uuid) const;
    std::vector<TxnTimestamp> filterUncommittedObjectPartialSchemas(std::vector<TxnTimestamp> & unfiltered_partial_schema_txnids);
    // @param limit_size -1 means no limit , read all partial schemas as possible
    ObjectPartialSchemas tryGetTableObjectPartialSchemas(const UUID & table_uuid, const int & limit_size = -1) const;
    bool resetObjectAssembledSchemaAndPurgePartialSchemas(
        const UUID & table_uuid,
        const ObjectAssembledSchema & old_assembled_schema,
        const ObjectAssembledSchema & new_assembled_schema,
        const std::vector<TxnTimestamp> & partial_schema_txnids);

    ObjectPartialSchemaStatuses batchGetObjectPartialSchemaStatuses(const std::vector<TxnTimestamp> & txn_ids, const int & batch_size = 10000);
    void batchDeleteObjectPartialSchemaStatus(const std::vector<TxnTimestamp> & txn_ids);
    void commitObjectPartialSchema(const TxnTimestamp & txn_id);
    void abortObjectPartialSchema(const TxnTimestamp & txn_id);
    void initStorageObjectSchema(StoragePtr & res);
    // Sensitive Resources
    void putSensitiveResource(const String & database, const String & table, const String & column, const String & target, bool value);
    std::shared_ptr<Protos::DataModelSensitiveDatabase> getSensitiveResource(const String & database);

    // Access Entities
    std::optional<AccessEntityModel> tryGetAccessEntity(EntityType type, const String & name);
    std::vector<AccessEntityModel> getAllAccessEntities(EntityType type);
    std::optional<String> tryGetAccessEntityName(const UUID & uuid);
    void dropAccessEntity(EntityType type, const UUID & uuid, const String & name);
    void putAccessEntity(EntityType type, AccessEntityModel & new_access_entity, const AccessEntityModel & old_access_entity, bool replace_if_exists = true);
    std::vector<AccessEntityModel> getEntities(EntityType type, const std::unordered_set<UUID> & ids);


    /////////////////////////////
    /// New metadata API
    /////////////////////////////
    std::vector<std::shared_ptr<DB::Protos::ManifestListModel>> getAllTableVersions(const UUID & uuid);
    UInt64 getCurrentTableVersion(const UUID & uuid, const TxnTimestamp & ts);
    // note that will be multiple rpc calls if txn_list size is bigger than 1. consider to use thread pool to fetch concurrently
    DataModelPartWrapperVector getCommittedPartsFromManifest(const MergeTreeMetaBase & storage, const std::vector<UInt64> & txn_list);
    DeleteBitmapMetaPtrVector getDeleteBitmapsFromManifest(const MergeTreeMetaBase & storage, const std::vector<UInt64> & txn_list);
    void commitCheckpointVersion(const UUID & uuid, std::shared_ptr<DB::Protos::ManifestListModel> checkpoint_version);
    void cleanTableVersions(const UUID & uuid, std::vector<std::shared_ptr<DB::Protos::ManifestListModel>> versions_to_clean);

    void shutDown() {bg_task.reset();}

private:
    LoggerPtr log = getLogger("Catalog");
    Context & context;
    MetastoreProxyPtr meta_proxy;
    const String name_space;
    String topology_key;

    std::unordered_map<UUID, std::shared_ptr<std::mutex>> nhut_mutex;
    std::mutex all_storage_nhut_mutex;
    CatalogSettings settings;

    std::shared_ptr<CatalogBackgroundTask> bg_task;

    std::shared_ptr<Protos::DataModelDB> tryGetDatabaseFromMetastore(const String & database, const UInt64 & ts);
    std::shared_ptr<Protos::DataModelTable>
    tryGetTableFromMetastore(const String & table_uuid, const UInt64 & ts, bool with_prev_versions = false, bool with_deleted = false);
    Strings tryGetDependency(const ASTPtr & create_query);
    static void replace_definition(Protos::DataModelTable & table, const String & db_name, const String & table_name);
    StoragePtr createTableFromDataModel(const Context & session_context, const Protos::DataModelTable & data_model);
    void detachOrAttachTable(const String & db, const String & name, const TxnTimestamp & ts, bool is_detach);
    DataModelPartWithNameVector getDataPartsMetaFromMetastore(
        const ConstStoragePtr & storage, const Strings & required_partitions, const Strings & full_partitions, const TxnTimestamp & ts, bool from_trash = false);
    DeleteBitmapMetaPtrVector getDeleteBitmapsInPartitionsImpl(
        const ConstStoragePtr & storage, const Strings & partitions, const TxnTimestamp & ts, bool from_trash = false, VisibilityLevel visibility = VisibilityLevel::Visible);
    DataModelDeleteBitmapPtrVector getDeleteBitmapsInPartitionsImpl(
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
        const bool write_manifest,
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
        const bool write_manifest,
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
        const std::function<T(const String &, const String &)> & create_func,
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

                T data_model = create_func(mIt->key(), mIt->value());
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
void fillUUIDForDictionary(DB::Protos::DataModelDictionary &);
}
