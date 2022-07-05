#pragma once

#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <MergeTreeCommon/CnchStorageCommon.h>
#include <common/shared_ptr_helper.h>

namespace DB
{

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

    bool isRemote() const override { return true; }

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageMetadataPtr &, SelectQueryInfo &) const override;

    void startup() override;
    void shutdown() override;

    Pipe read(
        const Names & /*column_names*/,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & /*query_info*/,
        ContextPtr /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/) override;

    void read(
        QueryPlan & query_plan,
        const Names & /*column_names*/,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & /*query_info*/,
        ContextPtr /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context) override;

    HostWithPortsVec getWriteWorkers(const ASTPtr & query, ContextPtr context) override;

    CheckResults checkData(const ASTPtr & query, ContextPtr context) override;

    String genCreateTableQueryForWorker(const String & suffix);

    time_t getTTLForPartition(const MergeTreePartition & partition) const;

    ServerDataPartsVector getPrunedServerParts(
        const Names & column_names_to_return,
        ContextPtr context,
        const SelectQueryInfo & query_info);

    /// return table's committed staged parts (excluding deleted ones).
    /// if partitions != null, ignore staged parts not belong to `partitions`.
    MergeTreeDataPartsCNCHVector getStagedParts(const TxnTimestamp & ts, const NameSet * partitions = nullptr);

    void getDeleteBitmapMetaForParts(const ServerDataPartsVector & parts, ContextPtr context, TxnTimestamp start_time);

    // Allocate parts to workers before we want to do some calculation on the parts, support non-select query.
    void allocateParts(ContextPtr context, ServerDataPartsVector & parts, WorkerGroupHandle & worker_group);

    UInt64 getTimeTravelRetention();

    void addCheckpoint(const Protos::Checkpoint & checkpoint);
    void removeCheckpoint(const Protos::Checkpoint & checkpoint);

    ColumnSizeByName getColumnSizes() const override
    {
        return {};
    }


    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;
    void alter(const AlterCommands & commands, ContextPtr context, TableLockHolder & table_lock_holder) override;

    void truncate(
        const ASTPtr & /*query*/,
        const StorageMetadataPtr & /* metadata_snapshot */,
        ContextPtr /* context */,
        TableExclusiveLockHolder &) override;

protected:
    StorageCnchMergeTree(
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata,
        bool attach,
        ContextMutablePtr context_,
        const String & date_column_name,
        const MergeTreeMetaBase::MergingParams & merging_params_,
        std::unique_ptr<MergeTreeSettings> settings_);

private:
    CheckResults checkDataCommon(const ASTPtr & query, ContextPtr context, ServerDataPartsVector & parts);

    ServerDataPartsVector getAllParts(ContextPtr context);

    Strings selectPartitionsByPredicate(
        const SelectQueryInfo & query_info, std::vector<std::shared_ptr<MergeTreePartition>> & partition_list, ContextPtr context);

    void eliminateParts(
        ServerDataPartsVector & parts,
        const ASTPtr & expression,
        ContextPtr context,
        const SelectQueryInfo & query_info) const;

    ServerDataPartsVector & pruneParts(
        const Names & column_names_to_return,
        ServerDataPartsVector & parts,
        ContextPtr context,
        const SelectQueryInfo & query_info) const;

    void allocateImpl(
        ContextPtr context,
        ServerDataPartsVector & parts,
        const String & local_table_name,
        WorkerGroupHandle & group);

    MutationCommands getFirstAlterMutationCommandsForPart(const DataPartPtr &) const override { return {}; }

    /// For select in interactive transaction session
    ServerDataPartsVector filterPartsInExplicitTransaction(ContextPtr query_context, ServerDataPartsVector && data_parts);

    /// Generate view dependency create queries for materialized view writing
    Names genViewDependencyCreateQueries(const StorageID & storage_id, ContextPtr context, const String & table_suffix);
    String extractTableSuffix(const String & gen_table_name);

};

}
