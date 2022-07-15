#pragma once
#include <Storages/MergeTree/MergeTreeData.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <common/shared_ptr_helper.h>

namespace DB
{

class StorageCloudMergeTree final : public shared_ptr_helper<StorageCloudMergeTree>, public MergeTreeMetaBase
{
    friend struct shared_ptr_helper<StorageCloudMergeTree>;
public:
    std::string getName() const override { return "CloudMergeTree"; }

    /// Load the set of data parts from disk. Call once - immediately after the object is created.
    void loadDataParts(const MutableDataPartsVector & data_parts);

    void manipulate(const ManipulationTaskParams & input_params, ContextPtr task_context) override;

    MutationCommands getFirstAlterMutationCommandsForPart(const DataPartPtr & part) const override;

protected:
    StorageCloudMergeTree(
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata_,
        bool attach,
        ContextMutablePtr context_,
        const String & date_column_name_,
        const MergeTreeData::MergingParams & merging_params_,
        std::unique_ptr<MergeTreeSettings> settings_,
        bool has_force_restore_data_flag);
};

}
