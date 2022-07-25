#pragma once

#include <Storages/MergeTree/MergeTreeCloudData.h>
#include <common/shared_ptr_helper.h>
#include "Storages/MergeTree/MergeTreeDataPartType.h"

namespace DB
{

class StorageCloudMergeTree final : public shared_ptr_helper<StorageCloudMergeTree>, public MergeTreeCloudData
{
    friend struct shared_ptr_helper<StorageCloudMergeTree>;
    friend class CloudMemoryBuffer;
    friend class CloudMergeTreeBlockOutputStream;

public:
    ~StorageCloudMergeTree() override;

    std::string getName() const override { return "CloudMergeTree"; }

    bool supportsSampling() const override { return true; }
    bool supportsFinal() const override { return true; }
    bool supportsPrewhere() const override { return true; }
    bool supportsIndexForIn() const override { return true; }
    bool supportsMapImplicitColumn() const override { return true; }
    bool canUseAdaptiveGranularity() const override { return false; }

    void startup() override {}
    void shutdown() override {}
    void drop() override {}

    const auto & getCnchDatabase() const { return cnch_database_name; }
    const auto & getCnchTable() const { return cnch_table_name; }
    StorageID getCnchStorageID() const { return StorageID(cnch_database_name, cnch_table_name, getStorageUUID()); }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

    ManipulationTaskPtr manipulate(const ManipulationTaskParams & params, ContextPtr task_context) override;

protected:
    MutationCommands getFirstAlterMutationCommandsForPart(const DataPartPtr & part) const override;

    StorageCloudMergeTree(
        const StorageID & table_id_,
        String cnch_database_name_,
        String cnch_table_name_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata_,
        ContextMutablePtr context_,
        const String & date_column_name_,
        const MergeTreeMetaBase::MergingParams & merging_params_,
        std::unique_ptr<MergeTreeSettings> settings_);

    const String cnch_database_name;
    const String cnch_table_name;

private:
    // To store some temporary data for cnch
    StoragePolicyPtr local_store_volume;
    String relative_local_store_path;
};

}
