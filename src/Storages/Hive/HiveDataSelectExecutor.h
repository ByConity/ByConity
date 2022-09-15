#pragma once

#include <Core/QueryProcessingStage.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageCloudHive.h>
#include <Storages/MergeTree/RowGroupsInDataPart.h>

namespace DB
{

/** Executes SELECT queries on data from the hive.
  */
class HiveDataSelectExecutor
{
public:
    HiveDataSelectExecutor(const StorageCloudHive & data_);

    QueryPlanPtr read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        ContextPtr & context,
        UInt64 max_block_size,
        size_t num_streams) const;

// private:
//     Pipe spreadRowGroupsAmongStreams(
//         const Context & context,
//         RowGroupsInDataParts && parts,
//         size_t num_streams,
//         const Names & column_names,
//         const UInt64 max_block_size) const;

private:
    const StorageCloudHive & data;
    Poco::Logger * log;
};

}
