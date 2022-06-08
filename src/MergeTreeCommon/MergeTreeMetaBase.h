#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataFormatVersion.h>


/// mock MergeTreeMetaBase for catalog
namespace DB
{

class MergeTreeMetaBase : public MergeTreeData
{
public:
    MergeTreeMetaBase(
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    ContextMutablePtr context_,
    const String & date_column_name,
    const MergeTreeData::MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> storage_settings_,
    bool require_part_metadata_,
    bool attach,
    MergeTreeData::BrokenPartCallback broken_part_callback_
    ) : MergeTreeData(
        table_id_,
        relative_data_path_,
        metadata_,
        context_,
        date_column_name,
        merging_params_,
        std::move(storage_settings_),
        require_part_metadata_,
        attach,
        broken_part_callback_
    ){}

    Block partition_key_sample;

    MergeTreeDataFormatVersion getDataFormatVersion() const { return format_version; }
};
 /// EOF
}
