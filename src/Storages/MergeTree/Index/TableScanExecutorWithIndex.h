#pragma once

#include <Common/Logger.h>
#include <QueryPlan/ProjectionStep.h>
#include <QueryPlan/ExecutePlanElement.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include "QueryPlan/IQueryPlanStep.h"
#include "Storages/MergeTree/Index/MergeTreeIndexHelper.h"
#include "Storages/MergeTree/MergeTreeDataSelectExecutor.h"

namespace DB
{

class TableScanStep;

class TableScanExecutorWithIndex
{
public:
    TableScanExecutorWithIndex(TableScanStep & step, ContextPtr context_);

    ExecutePlan buildExecutePlan(const DistributedPipelineSettings & distributed_settings);

private:

    void prunePartsByIndex(MergeTreeData::DataPartsVector & parts) const;

    MergeTreeDataSelectAnalysisResultPtr estimateReadMarks(const PartGroup & part_group) const;

    bool checkIndex(const IMergeTreeDataPartPtr & part) const;
    
    ReadFromMergeTree::IndexStats getIndexStats() const;

    StoragePtr storage;
    StorageMetadataPtr storage_metadata;
    const MergeTreeMetaBase & merge_tree_data;
    MergeTreeDataSelectExecutor merge_tree_reader;
    const SelectQueryInfo & select_query_info;
    ContextPtr context;
    LoggerPtr log;

    DataStream input_stream;
    Names query_required_columns;
    std::shared_ptr<PartitionIdToMaxBlock> max_added_blocks;
    MergeTreeIndexContextPtr index_context;
    Names columns_with_index;

};


}
