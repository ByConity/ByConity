#pragma once
#include <Storages/MergeTree/MergeTreeData.h>
#include <common/shared_ptr_helper.h>
namespace DB
{

class StorageCnchMergeTree final : public shared_ptr_helper<StorageCnchMergeTree>, public IStorage
{
    friend struct shared_ptr_helper<StorageCnchMergeTree>;
public:
    std::string getName() const override { return /*"Cnch" + merging_params.getModeName() + "MergeTree" */ "CnchMergeTree";}

protected:
    StorageCnchMergeTree(
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata,
        bool attach,
        ContextMutablePtr context_,
        const String & date_column_name,
        const MergeTreeMetaBase::MergingParams & merging_params_,
        std::unique_ptr<MergeTreeSettings> settings_,
        bool has_force_restore_data_flag);
};

}
