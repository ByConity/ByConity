#pragma once

#include <optional>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <MergeTreeCommon/CnchStorageCommon.h>
#include <common/shared_ptr_helper.h>
#include "Catalog/DataModelPartWrapper_fwd.h"
#include <Storages/MergeTree/PartitionPruner.h>
#include <Storages/MergeTree/MergeTreeDataPartType.h>

namespace DB
{

struct PrepareContextResult;
class StorageCnchMergeTree final : public shared_ptr_helper<StorageCnchMergeTree>, public MergeTreeMetaBase, public CnchStorageCommonHelper
{
    friend struct shared_ptr_helper<StorageCnchMergeTree>;
public:
    ~StorageCnchMergeTree() override;

    std::string getName() const override { return "Cnch" + merging_params.getModeName() + "MergeTree";}

    bool supportsSampling() const override { return true; }
    bool supportsFinal() const override { return true; }
    bool supportsPrewhere() const override { return true; }
    bool supportsIndexForIn() const override { return true; }
    bool supportsMapImplicitColumn() const override { return true; }
    bool canUseAdaptiveGranularity() const override { return false; }
    StoragePolicyPtr getLocalStoragePolicy() const override;

    bool isRemote() const override { return true; }

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageMetadataPtr &, SelectQueryInfo &) const override;

    void startup() override;
    void shutdown() override;

    Pipe read(
        const Names & /*column_names*/,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & /*query_info*/,
        ContextPtr /*local_context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/) override;

    void read(
        QueryPlan & query_plan,
        const Names & /*column_names*/,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & /*query_info*/,
        ContextPtr /*local_context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/) override;

    PrepareContextResult prepareReadContext(
        const Names & column_names, const StorageMetadataPtr & metadata_snapshot, SelectQueryInfo & query_info, ContextPtr & local_context);

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context) override;

    HostWithPortsVec getWriteWorkers(const ASTPtr & query, ContextPtr local_context) override;

    CheckResults checkData(const ASTPtr & query, ContextPtr local_context) override;

    time_t getTTLForPartition(const MergeTreePartition & partition) const;

    ServerDataPartsVector selectPartsToRead(
        const Names & column_names_to_return,
        ContextPtr local_context,
        const SelectQueryInfo & query_info);

    /// return table's committed staged parts (excluding deleted ones).
    /// if partitions != null, ignore staged parts not belong to `partitions`.
    MergeTreeDataPartsCNCHVector getStagedParts(const TxnTimestamp & ts, const NameSet * partitions = nullptr);

    void getDeleteBitmapMetaForParts(const ServerDataPartsVector & parts, ContextPtr local_context, TxnTimestamp start_time);

    // Allocate parts to workers before we want to do some calculation on the parts, support non-select query.
    void allocateParts(ContextPtr local_context, ServerDataPartsVector & parts, WorkerGroupHandle & worker_group);

    UInt64 getTimeTravelRetention();

    void addCheckpoint(const Protos::Checkpoint & checkpoint);
    void removeCheckpoint(const Protos::Checkpoint & checkpoint);

    ColumnSizeByName getColumnSizes() const override
    {
        return {};
    }


    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr local_context) const override;
    void alter(const AlterCommands & commands, ContextPtr local_context, TableLockHolder & table_lock_holder) override;

    void checkAlterPartitionIsPossible(
        const PartitionCommands & commands, const StorageMetadataPtr & metadata_snapshot, const Settings & settings) const override;
    Pipe alterPartition(
        const StorageMetadataPtr & metadata_snapshot,
        const PartitionCommands & commands,
        ContextPtr query_context) override;

    void truncate(
        const ASTPtr & /*query*/,
        const StorageMetadataPtr & /* metadata_snapshot */,
        ContextPtr /* local_context */,
        TableExclusiveLockHolder &) override;

    ServerDataPartsVector selectPartsByPartitionCommand(ContextPtr local_context, const PartitionCommand & command);

    void dropPartitionOrPart(const PartitionCommand & command, ContextPtr local_context,
        IMergeTreeDataPartsVector* dropped_parts = nullptr);
    Block getBlockWithVirtualPartitionColumns(const std::vector<std::shared_ptr<MergeTreePartition>> & partition_list) const;

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
    MutableDataPartsVector createDropRangesFromParts(const ServerDataPartsVector & parts_to_drop, const TransactionCnchPtr & txn);

    StorageCnchMergeTree & checkStructureAndGetCnchMergeTree(const StoragePtr & source_table) const;

    const String & getLocalStorePath() const;
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
    /// To store some temporary data for cnch
    StoragePolicyPtr local_store_volume;
    String relative_local_store_path;

    CheckResults checkDataCommon(const ASTPtr & query, ContextPtr local_context, ServerDataPartsVector & parts);

    ServerDataPartsVector getAllParts(ContextPtr local_context);

    Strings selectPartitionsByPredicate(
        const SelectQueryInfo & query_info, std::vector<std::shared_ptr<MergeTreePartition>> & partition_list, const Names & column_names_to_return, ContextPtr local_context);

    void filterPartsByPartition(
        ServerDataPartsVector & parts,
        ContextPtr local_context,
        const SelectQueryInfo & query_info,
        const Names & column_names_to_return) const;

    void dropPartsImpl(ServerDataPartsVector& svr_parts_to_drop,
        IMergeTreeDataPartsVector& parts_to_drop, bool detach, ContextPtr local_context);

    void collectResource(ContextPtr local_context, ServerDataPartsVector & parts, const String & local_table_name, const std::set<Int64> & required_bucket_numbers = {});

    MutationCommands getFirstAlterMutationCommandsForPart(const DataPartPtr &) const override { return {}; }

    /// For select in interactive transaction session
    void filterPartsInExplicitTransaction(ServerDataPartsVector & data_parts, ContextPtr local_context);

    /// Generate view dependency create queries for materialized view writing
    Names genViewDependencyCreateQueries(const StorageID & storage_id, ContextPtr local_context, const String & table_suffix);
    String extractTableSuffix(const String & gen_table_name);
    std::set<Int64> getRequiredBucketNumbers(const SelectQueryInfo & query_info, ContextPtr context) const;

};

struct PrepareContextResult
{
    String local_table_name;
    ServerDataPartsVector parts;
};

}
