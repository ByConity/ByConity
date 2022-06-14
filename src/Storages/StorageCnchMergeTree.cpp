#include "StorageCnchMergeTree.h"

namespace DB
{

StorageCnchMergeTree::StorageCnchMergeTree(
    const StorageID & table_id_,
    const String & /*relative_data_path_*/,
    const StorageInMemoryMetadata & /* metadata */,
    bool /*attach*/,
    ContextMutablePtr /* context_ */,
    const String & /* date_column_name */,
    const MergeTreeData::MergingParams & /* merging_params_ */,
    std::unique_ptr<MergeTreeSettings> /* settings_ */,
    bool /* has_force_restore_data_flag */)
    : IStorage{table_id_}
{}

}
