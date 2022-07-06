#include <Storages/StorageCloudMergeTree.h>

#include <Storages/MutationCommands.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

StorageCloudMergeTree::StorageCloudMergeTree(
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    bool /*attach*/,
    ContextMutablePtr context_,
    const String & date_column_name_,
    const MergeTreeData::MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> settings_,
    bool /* has_force_restore_data_flag */)
    : MergeTreeMetaBase(
        table_id_,
        relative_data_path_,
        metadata_,
        context_,
        date_column_name_,
        merging_params_,
        std::move(settings_),
        false /* FIXME */)
{}

void StorageCloudMergeTree::loadDataParts(const MutableDataPartsVector & /*data_parts*/)
{
    // TODO
}

MutationCommands StorageCloudMergeTree::getFirstAlterMutationCommandsForPart(const DataPartPtr & /*part*/) const
{
    return {};
}

}
