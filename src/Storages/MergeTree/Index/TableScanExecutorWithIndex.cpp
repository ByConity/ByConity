#include <MergeTreeCommon/assignCnchParts.h>
#include <QueryPlan/ExecutePlanElement.h>
#include <QueryPlan/TableScanStep.h>
#include <Storages/MergeTree/Index/BitmapIndexHelper.h>
#include <Storages/MergeTree/Index/TableScanExecutorWithIndex.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/StorageReplicatedMergeTree.h>

namespace DB
{

TableScanExecutorWithIndex::TableScanExecutorWithIndex(TableScanStep & step, ContextPtr context_)
: storage(step.getStorage())
, storage_metadata(storage->getInMemoryMetadataPtr())
, merge_tree_data(dynamic_cast<const MergeTreeMetaBase &>(*storage))
, merge_tree_reader(merge_tree_data)
, select_query_info(step.getQueryInfo())
, context(std::move(context_))
, log(&Poco::Logger::get("TableScanExecutorWithIndex"))
{
    input_stream = step.getOutputStream();
    query_required_columns = step.getRequiredColumns();

    const auto & settings = context->getSettingsRef();
    if (settings.select_sequential_consistency)
    {
        if (const StorageReplicatedMergeTree * replicated = dynamic_cast<const StorageReplicatedMergeTree *>(storage.get()))
            max_added_blocks = std::make_shared<PartitionIdToMaxBlock>(replicated->getMaxAddedBlocks());
    }

    index_context = select_query_info.index_context;
    if (index_context)
    {
        if (auto * bitmap_index_info = dynamic_cast<BitmapIndexInfo *>(index_context->get(MergeTreeIndexInfo::Type::BITMAP).get()))
        {
            for (auto & index_name : bitmap_index_info->index_names)
                columns_with_index.insert(columns_with_index.end(), index_name.second.begin(), index_name.second.end());
        }
    }
}

ExecutePlan TableScanExecutorWithIndex::buildExecutePlan(const DistributedPipelineSettings & distributed_settings)
{
    // Do not build execute plan when there isn't bitmap index
    if (columns_with_index.empty())
        return {};

    PartGroups part_groups;
    {
        auto parts = merge_tree_data.getDataPartsVector();
        if (distributed_settings.source_task_filter.isValid())
        {
            auto size_before_filtering = parts.size();
            filterParts(parts, distributed_settings.source_task_filter);
            LOG_TRACE(
                log,
                "After filtering({}) the number of parts of table {} becomes {} from {}",
                distributed_settings.source_task_filter.toString(),
                storage->getTableName(),
                parts.size(),
                size_before_filtering);
        }
        parts.erase(std::remove_if(parts.begin(), parts.end(), [](auto & part) { return part->info.isFakeDropRangePart(); }), parts.end());

        prunePartsByIndex(parts);

        MergeTreeData::DataPartsVector parts_with_index;
        MergeTreeData::DataPartsVector parts_without_index;

        for (const auto & part : parts)
        {
            if (checkIndex(part))
                parts_with_index.push_back(part);
            else
                parts_without_index.push_back(part);
        }

        // LOG_DEBUG(log, fmt::format("parts_with_index size: {}, parts_without_index size: {}", 
        //     parts_with_index.size(), parts_without_index.size()));

        if (!parts_with_index.empty())
            part_groups.push_back({.parts = parts_with_index, .has_bitmap_index = true});
        if (!parts_without_index.empty())
            part_groups.push_back({.parts = parts_without_index});
    }

    ExecutePlan execute_plan;
    auto table_columns = storage_metadata->getColumns().getAllPhysical();

    for (auto & part_group: part_groups)
    {
        auto normal_read_result = estimateReadMarks(part_group);

        bool has_bitmap_index = part_group.hasBitmapIndx();
        auto plan_element = ExecutePlanElement(part_group, std::move(normal_read_result), part_group.parts);
        plan_element.read_bitmap_index = has_bitmap_index;
        execute_plan.push_back(plan_element);
    }

    return execute_plan;
}

void TableScanExecutorWithIndex::prunePartsByIndex(MergeTreeData::DataPartsVector & parts) const
{
    if (parts.empty())
        return;

    auto part_values = MergeTreeDataSelectExecutor::filterPartsByVirtualColumns(
        merge_tree_data,
        parts,
        select_query_info,
        context);

    if (part_values && part_values->empty())
    {
        parts.clear();
        return;
    }

    ReadFromMergeTree::AnalysisResult result;
    MergeTreeDataSelectExecutor::filterPartsByPartition(
        parts,
        part_values,
        storage_metadata,
        merge_tree_data,
        select_query_info,
        context,
        max_added_blocks.get(),
        log,
        result.index_stats);
}

MergeTreeDataSelectAnalysisResultPtr TableScanExecutorWithIndex::estimateReadMarks(const PartGroup & part_group) const
{
    return merge_tree_reader.estimateNumMarksToRead(part_group.parts,
                                                    query_required_columns,
                                                    storage_metadata,
                                                    storage_metadata,
                                                    select_query_info,
                                                    context,
                                                    context->getSettingsRef().max_threads,
                                                    max_added_blocks);
}

bool TableScanExecutorWithIndex::checkIndex(const IMergeTreeDataPartPtr & part) const
{
    bool has_index_for_building_plan = false;
    for (const auto & name : columns_with_index)
    {
        if (!part->hasBitmapIndex(name))
            return false;
        else
            has_index_for_building_plan = true;
    }

    return has_index_for_building_plan;
}

ReadFromMergeTree::IndexStats TableScanExecutorWithIndex::getIndexStats() const
{
    ReadFromMergeTree::IndexStats index_stats;
    for (const auto & name : columns_with_index)
    {
        index_stats.push_back({.type = ReadFromMergeTree::IndexType::Bitmap, .name = name});
    }

    return index_stats;
}

}
