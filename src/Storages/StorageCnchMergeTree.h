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

#include <optional>
#include <type_traits>
#include <Catalog/DataModelPartWrapper_fwd.h>
#include <CloudServices/CnchPartsHelper.h>
#include <MergeTreeCommon/CnchStorageCommon.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Storages/MergeTree/MergeTreeDataPartType.h>
#include <Storages/MergeTree/PartitionPruner.h>
#include <Storages/StorageSnapshot.h>
#include <Transaction/CnchLock.h>
#include <Transaction/TxnTimestamp.h>
#include <common/shared_ptr_helper.h>

namespace DB
{

struct PrepareContextResult;
class ASTSystemQuery;
class MergeTreeBgTaskStatistics;

class StorageCnchMergeTree final : public shared_ptr_helper<StorageCnchMergeTree>, public MergeTreeMetaBase, public CnchStorageCommonHelper
{
    friend struct shared_ptr_helper<StorageCnchMergeTree>;
    friend class InterpreterAlterDiskCacheQuery;

public:
    ~StorageCnchMergeTree() override;

    std::string getName() const override { return "Cnch" + merging_params.getModeName() + "MergeTree"; }

    void loadMutations();
    bool supportsSampling() const override { return true; }
    bool supportsFinal() const override { return true; }
    bool supportsPrewhere() const override { return true; }
    bool supportsIndexForIn() const override { return true; }
    bool supportsMapImplicitColumn() const override { return true; }
    bool supportsDynamicSubcolumns() const override { return true; }
    bool supportsTrivialCount() const override { return true; }
    bool supportIntermedicateResultCache() const override
    {
        return true;
    }

    /// Whether support DELETE FROM. We only support for Unique MergeTree for now.
    bool supportsLightweightDelete() const override { return getInMemoryMetadataPtr()->hasUniqueKey(); }

    std::optional<UInt64> totalRows(const ContextPtr &) const override;
    std::optional<UInt64> totalRowsByPartitionPredicate(const SelectQueryInfo &, ContextPtr) const override;

    StoragePolicyPtr getStoragePolicy(StorageLocation location) const override;
    const String & getRelativeDataPath(StorageLocation location) const override;

    bool isRemote() const override { return true; }

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override;

    void startup() override;
    void shutdown() override;

    Pipe read(
        const Names & /*column_names*/,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & /*query_info*/,
        ContextPtr /*local_context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/) override;

    void read(
        QueryPlan & query_plan,
        const Names & /*column_names*/,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & /*query_info*/,
        ContextPtr /*local_context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/) override;

    PrepareContextResult prepareReadContext(
        const Names & column_names, const StorageMetadataPtr & metadata_snapshot, SelectQueryInfo & query_info, ContextPtr & local_context);

    std::pair<String, const Cluster::ShardInfo *> prepareLocalTableForWrite(ASTInsertQuery * insert_query, ContextPtr local_context, bool enable_staging_area, bool send_query);

    bool supportsOptimizer() const override { return true; }
    bool supportsDistributedRead() const override { return true; }
    StorageID prepareTableRead(const Names & output_columns, SelectQueryInfo & query_info, ContextPtr local_context) override;
    StorageID prepareTableWrite(ContextPtr local_context) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context) override;

    BlockInputStreamPtr writeInWorker(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context);

    HostWithPortsVec getWriteWorkers(const ASTPtr & query, ContextPtr local_context) override;

    bool optimize(
        const ASTPtr & query,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        const ASTPtr & partition,
        bool final,
        bool /*deduplicate*/,
        const Names & /* deduplicate_by_columns */,
        ContextPtr query_context) override;

    CheckResults checkData(const ASTPtr & query, ContextPtr local_context) override;

    CheckResults autoRemoveData(const ASTPtr & query, ContextPtr local_context) override;

    ServerDataPartsWithDBM
    getServerDataPartsWithDBMFromSnapshot(const ContextPtr & local_context, std::optional<Strings> partition_ids, UInt64 snapshot_ts) const;

    std::unordered_set<String> getPartitionIDsFromQuery(const ASTs & asts, ContextPtr local_context) const;

    MinimumDataParts getBackupPartsFromDisk(
        const DiskPtr & backup_disk,
        const String & parts_path_in_backup,
        ContextMutablePtr & local_context,
        std::optional<ASTs> partitions) const;

    void restoreDataFromBackup(BackupTaskPtr & backup_task, const DiskPtr & backup_disk, const String & data_path_in_backup, ContextMutablePtr context, std::optional<ASTs> partitions) override;

    /// TODO: some code in this duplicate with filterPartitionByTTL, refine it later
    time_t getTTLForPartition(const MergeTreePartition & partition) const;

    /**
     * @param snapshot_ts If not zero, specify the snapshot to use
     */
    ServerDataPartsWithDBM
    selectPartsToReadWithDBM(const Names & column_names_to_return, ContextPtr local_context, const SelectQueryInfo & query_info, UInt64 snapshot_ts = 0, bool staging_area = false) const;

    /// Return all base parts and delete bitmap metas in the given partitions.
    /// If `partitions` is empty, return meta for all partitions.
    MergeTreeDataPartsCNCHVector getUniqueTableMeta(TxnTimestamp ts, const Strings & partitions = {}, bool force_bitmap = true, const std::set<Int64> & bucket_numbers = {});

    /// return table's committed staged parts (excluding deleted ones).
    /// if partitions != null, ignore staged parts not belong to `partitions`.
    MergeTreeDataPartsCNCHVector
    getStagedParts(const TxnTimestamp & ts, const NameSet * partitions = nullptr, bool skip_delete_bitmap = false);

    /// Used by the "SYSTEM DEDUP" command to repair unique table by removing duplicate keys in visible parts.
    void executeDedupForRepair(const ASTSystemQuery & query, ContextPtr context);

    /// Used by the "SYSTEM SYNC DEDUP WORKER" command to wait for all staged parts to publish
    void waitForStagedPartsToPublish(ContextPtr context);

    // Allocate parts to workers before we want to do some calculation on the parts, support non-select query.
    void allocateParts(ContextPtr local_context, const ServerDataPartsVector & parts);

    ColumnSizeByName getColumnSizes() const override { return {}; }

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr local_context) const override;
    void alter(const AlterCommands & commands, ContextPtr local_context, TableLockHolder & table_lock_holder) override;
    void checkAlterSettings(const AlterCommands & commands) const;
    void checkAlterVW(const String & vw_name) const;

    void checkAlterPartitionIsPossible(
        const PartitionCommands & commands, const StorageMetadataPtr & metadata_snapshot, const Settings & settings) const override;
    Pipe
    alterPartition(const StorageMetadataPtr & metadata_snapshot, const PartitionCommands & commands, ContextPtr query_context, const ASTPtr & query = nullptr) override;

    void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const override;

    void mutate(const MutationCommands & commands, ContextPtr query_context) override;

    void truncate(
        const ASTPtr & query,
        const StorageMetadataPtr & /* metadata_snapshot */,
        ContextPtr /* local_context */,
        TableExclusiveLockHolder &) override;

    ServerDataPartsWithDBM selectPartsByPartitionCommand(ContextPtr local_context, const PartitionCommand & command);
    void overwritePartitions(const ASTPtr & overwrite_partition, ContextPtr local_context, CnchLockHolderPtrs * lock_holders);
    void dropPartitionOrPart(const PartitionCommand & command, ContextPtr local_context,
        IMergeTreeDataPartsVector* dropped_parts = nullptr, bool do_commit = true, CnchLockHolderPtrs * lock_holders = nullptr, size_t max_threads = 16);

    struct PartitionDropInfo
    {
        Int64 max_block{0};
        size_t rows_count{0}; // rows count in drop range.
        size_t size{0}; // bytes size in drop range.
        size_t parts_count{0}; // covered parts in drop range.
        MergeTreePartition value;
    };
    using PartitionDropInfos = std::unordered_map<String, PartitionDropInfo>;
    MutableDataPartsVector createDropRangesFromPartitions(const PartitionDropInfos & partition_infos, const TransactionCnchPtr & txn);
    MutableDataPartsVector createDropRangesFromParts(ContextPtr query_context, const ServerDataPartsVector & parts_to_drop, const TransactionCnchPtr & txn);
    LocalDeleteBitmaps createDeleteBitmapTombstones(const IMutableMergeTreeDataPartsVector & drop_range_parts, UInt64 txnID);

    StorageCnchMergeTree * checkStructureAndGetCnchMergeTree(const StoragePtr & source_table, ContextPtr local_context) const;

    const String & getLocalStorePath() const;

    void reclusterPartition(const PartitionCommand & command, ContextPtr query_context);

    String genCreateTableQueryForWorker(const String & suffix);

    Strings getPartitionsByPredicate(const ASTPtr & predicate, ContextPtr local_context);

    ServerDataPartsVector
    getServerPartsByPredicate(const ASTPtr & predicate, const std::function<ServerDataPartsVector()> & get_parts, ContextPtr local_context);

    void sendPreloadTasks(ContextPtr local_context, ServerDataPartsVector parts, bool enable_parts_sync_preload = true, UInt64 parts_preload_level = 0, UInt64 ts = {});
    void sendDropDiskCacheTasks(ContextPtr local_context, const ServerDataPartsVector & parts, bool sync = false, bool drop_vw_disk_cache = false);
    void sendDropManifestDiskCacheTasks(ContextPtr local_context, String version = "", bool sync = false);

    PrunedPartitions getPrunedPartitions(const SelectQueryInfo & query_info, const Names & column_names_to_return, ContextPtr local_context, const bool & ignore_ttl) const ;

    void resetObjectColumns(ContextPtr query_context);

    void appendObjectPartialSchema(const TxnTimestamp & txn_id, ObjectPartialSchema partial_schema);
    void resetObjectSchemas(const ObjectAssembledSchema & assembled_schema, const ObjectPartialSchemas & partial_schemas);
    void refreshAssembledSchema(const ObjectAssembledSchema & assembled_schema,  std::vector<TxnTimestamp> txn_ids);
    void checkMetadataValidity(const ColumnsDescription & columns, const ASTPtr & new_settings = nullptr) const override;

    /// parse bucket number set from where clause, only works for single-key cluster by
    std::set<Int64> getRequiredBucketNumbers(const SelectQueryInfo & query_info, ContextPtr context) const;

    // get all Visible Parts
    ServerDataPartsWithDBM getAllPartsWithDBM(ContextPtr local_context) const;

    /// drop the memody_dict_cache of cnch table
    void dropMemoryDictCache(ContextMutablePtr & local_context);

protected:
    StorageCnchMergeTree(
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata_,
        bool attach_,
        ContextMutablePtr context_,
        const String & date_column_name_,
        const MergeTreeMetaBase::MergingParams & merging_params_,
        std::unique_ptr<MergeTreeSettings> settings_);

private:
    friend class DB::Catalog::Catalog;
    // Relative path to auxility storage disk root
    String relative_auxility_storage_path;

    /// Current description of columns of data type Object.
    /// It changes only when set of parts is changed and is
    /// protected by @data_parts_mutex.
    ObjectSchemas object_schemas;

    void overwritePartition(const ASTPtr & overwrite_partition, ContextPtr local_context, CnchLockHolderPtrs * lock_holders);
    CheckResults checkDataCommon(const ASTPtr & query, ContextPtr local_context, ServerDataPartsVector & parts) const;

    /**
     * @param snapshot_ts If not zero, specify the snapshot to use
     */
    ServerDataPartsWithDBM getAllPartsInPartitionsWithDBM(
        const Names & column_names_to_return,
        ContextPtr local_context,
        const SelectQueryInfo & query_info,
        UInt64 snapshot_ts = 0,
        bool staging_area = false) const;

    void filterPartsByPartition(
        ServerDataPartsVector & parts,
        ContextPtr local_context,
        const SelectQueryInfo & query_info,
        const Names & column_names_to_return) const;

    void dropPartsImpl(
        const PartitionCommand & command,
        ServerDataPartsWithDBM & svr_parts_to_drop,
        IMergeTreeDataPartsVector & parts_to_drop,
        ContextPtr local_context,
        bool do_commit,
        size_t max_threads);

    void collectResource(
        ContextPtr local_context,
        UInt64 table_version,
        const ServerDataPartsVector & parts,
        const String & local_table_name,
        const std::set<Int64> & required_bucket_numbers = {},
        const StorageSnapshotPtr & storage_snapshot = nullptr,
        WorkerEngineType engine_type = WorkerEngineType::CLOUD,
        bool replicated = false);

    /// NOTE: No need to implement this for CnchMergeTree as data processing is on CloudMergeTree.
    MutationCommands getFirstAlterMutationCommandsForPart(const DataPartPtr &) const override { return {}; }

    /// For select in interactive transaction session
    ServerDataPartsVector filterPartsInExplicitTransaction(ServerDataPartsVector & data_parts, ContextPtr local_context) const;

    /// Generate view dependency create queries for materialized view writing
    NameSet genViewDependencyCreateQueries(
        const StorageID & storage_id, ContextPtr local_context, const String & table_suffix, std::set<String> & cnch_table_create_queries);

    Pipe ingestPartition(const struct PartitionCommand & command, const ContextPtr local_context);

    /// Check if the ALTER can be performed:
    /// - all needed columns are present.
    /// - all type conversions can be done.
    /// - columns corresponding to primary key, indices, sign, sampling expression and date are not affected.
    /// If something is wrong, throws an exception.
    void checkAlterInCnchServer(const AlterCommands & commands, ContextPtr local_context) const;

    std::unique_ptr<MergeTreeSettings> getDefaultSettings() const override;
};

using StorageCnchMergeTreePtr = std::shared_ptr<StorageCnchMergeTree>;
}
