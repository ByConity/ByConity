#pragma once

#include <common/types.h>
#include <common/strong_typedef.h>

namespace DB
{

#define MERGE_TREE_STORAGE_LEVEL_1_META_HEADER_SIZE 256
#define MERGE_TREE_STORAGE_LEVEL_1_DATA_HEADER_SIZE 256

STRONG_TYPEDEF(UInt32, MergeTreeDataFormatVersion)

const MergeTreeDataFormatVersion MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING {1};
const MergeTreeDataFormatVersion MERGE_TREE_CHCH_DATA_STORAGTE_VERSION {1};

/// Remote storage version
const MergeTreeDataFormatVersion MERGE_TREE_DATA_STORAGTE_LEVEL_1_VERSION{100};

/// CNCH storage version
#define MERGE_TREE_STORAGE_CNCH_DATA_HEADER_SIZE 256
#define MERGE_TREE_STORAGE_CNCH_DATA_FOOTER_SIZE 256
}
