#include <sstream>
#include <Storages/MergeTree/Index/MergeTreeIndexHelper.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/Index/MergeTreeBitmapIndexBoolReader.h>
#include <Storages/MergeTree/Index/MergeTreeBitmapIndex.h>
#include <Storages/MergeTree/Index/MergeTreeSegmentBitmapIndex.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <QueryPlan/ProjectionStep.h>
#include <Common/LinkedHashMap.h>
#include "Optimizer/ExpressionRewriter.h"
#include <fmt/ranges.h>

namespace DB
{

void BitmapIndexChecker::checkBitmapIndex(const IMergeTreeDataPartPtr & part)
{
    for (const auto & column: part->getColumns())
    {
        if (MergeTreeBitmapIndex::isBitmapIndexColumn(column.type)
            && (part->checksums_ptr->has(column.name + BITMAP_IDX_EXTENSION) && part->checksums_ptr->has(column.name + BITMAP_IRK_EXTENSION)))
            part->bitmap_index_checker->emplace(column.name, true);
    }
}

void BitmapIndexChecker::checkSegmentBitmapIndex(const IMergeTreeDataPartPtr & part)
{
    for (const auto & column: part->getColumns())
    {
        if (MergeTreeSegmentBitmapIndex::isSegmentBitmapIndexColumn(column.type)
            && (part->checksums_ptr->has(column.name + SEGMENT_BITMAP_IDX_EXTENSION))
            && (part->checksums_ptr->has(column.name + SEGMENT_BITMAP_TABLE_EXTENSION))
            && (part->checksums_ptr->has(column.name + SEGMENT_BITMAP_DIRECTORY_EXTENSION)))
            part->bitmap_index_checker->emplace(column.name, true);
    }
}

MergeTreeIndexExecutorPtr MergeTreeIndexContext::getIndexExecutor(
    const IMergeTreeDataPartPtr & part,
    const MergeTreeIndexGranularity & index_granularity,
    size_t index_segment_granularity,
    size_t index_serializing_granularity,
    const MarkRanges & mark_ranges)
{
    auto lock = std::shared_lock<std::shared_mutex>(index_lock);

    if (!enable_read_bitmap_index)
        return nullptr;

    auto index_executor = std::make_shared<MergeTreeIndexExecutor>();

    for (auto & index_info : index_infos)
    {
        switch(index_info.first)
        {
            case MergeTreeIndexInfo::Type::UNKNOWN:
                throw Exception("Cannot find index reader", ErrorCodes::LOGICAL_ERROR);
            case MergeTreeIndexInfo::Type::BITMAP:
            {
                index_executor->addReader(
                    index_info.first, 
                    BitmapIndexHelper::getBitmapIndexReader(
                        part,
                        std::dynamic_pointer_cast<BitmapIndexInfo>(index_info.second),
                        index_granularity,
                        index_segment_granularity,
                        index_serializing_granularity,
                        mark_ranges)
                );
            }
        }
    }

    return index_executor;
}

void MergeTreeIndexContext::add(const MergeTreeIndexInfoPtr & info)
{
    auto lock = std::unique_lock<std::shared_mutex>(index_lock);
    index_infos.emplace(info->getType(), info);
}

MergeTreeIndexInfoPtr MergeTreeIndexContext::get(MergeTreeIndexInfo::Type type)
{
    auto lock = std::unique_lock<std::shared_mutex>(index_lock);

    auto it = index_infos.find(type);
    if (it == index_infos.end())
    {
        switch(type)
        {
            case MergeTreeIndexInfo::Type::UNKNOWN:
                throw Exception(fmt::format("Cannot get index info for type {}"), ErrorCodes::LOGICAL_ERROR);
            case MergeTreeIndexInfo::Type::BITMAP:
                it = index_infos.emplace(type, std::make_shared<BitmapIndexInfo>()).first;
        }
    }
    return it->second;
}

bool MergeTreeIndexContext::has(MergeTreeIndexInfo::Type type)
{
    auto lock = std::unique_lock<std::shared_mutex>(index_lock);

    return index_infos.count(type);
}

MergeTreeIndexContextPtr MergeTreeIndexContext::clone()
{
    auto lock = std::unique_lock<std::shared_mutex>(index_lock);

    auto index_context = std::make_shared<MergeTreeIndexContext>();

    index_context->enable_read_bitmap_index = this->enable_read_bitmap_index;
    index_context->index_infos = this->index_infos;
    index_context->project_actions = this->project_actions;
    index_context->project_actions_from_materialized_index = this->project_actions_from_materialized_index;

    return index_context;
}

String MergeTreeIndexContext::toString() const
{
    auto lock = std::unique_lock<std::shared_mutex>(index_lock);

    std::ostringstream ostr;

    ostr << "IndexContext:\n";
    for (const auto & index_info : index_infos)
    {
        ostr << fmt::format("{} : {}", IndexTypeToString(index_info.first), index_info.second->toString()) << ",\n";
    }

    return ostr.str();
}

std::shared_ptr<ProjectionStep> MergeTreeIndexContext::genProjectionForIndex(
    const NamesWithAliases & column_alias,
    const ProjectionStep * pushdown_projection
)
{
    ConstASTMap symbol_map;
    NameToNameMap alias_to_name_map;
    for (const auto & item : column_alias)
    {
        alias_to_name_map[item.second] = item.first;
        symbol_map[std::make_shared<ASTIdentifier>(item.second)] = ConstHashAST::make(std::make_shared<ASTIdentifier>(item.first));
    }

    Assignments new_assignments;
    NameToType name_to_type;
    for (const auto & item : pushdown_projection->getAssignments())
    {
        //auto name = alias_to_name_map.contains(item.first) ? alias_to_name_map[item.first] : item.second->getColumnName();
        auto from_name = alias_to_name_map.contains(item.first) ? alias_to_name_map[item.first] : item.first;
        auto to_ast = alias_to_name_map.contains(item.first) ? std::make_shared<ASTIdentifier>(from_name)
                                                             : ExpressionRewriter::rewrite(item.second, symbol_map);
        new_assignments.emplace_back(from_name, to_ast);
        name_to_type[from_name] = pushdown_projection->getNameToType().at(item.first);
    }

    DataStream stream;
    for (const auto & item : pushdown_projection->getInputStreams()[0].header)
    {
        stream.header.insert(ColumnWithTypeAndName(item.type, alias_to_name_map.contains(item.name) ? alias_to_name_map[item.name] : item.name));
    }

    auto projection = std::make_shared<ProjectionStep>(stream, new_assignments, name_to_type);
    return projection;
}

MergeTreeIndexContextPtr MergeTreeIndexContext::buildFromProjection(const Assignments & inline_expressions, MergeTreeIndexInfo::BuildIndexContext & building_context, const StorageMetadataPtr & metadata_snapshot)
{
    Assignments bitmap_expressions;

    for (const auto & [symbol, expr] : inline_expressions)
    {
        const auto * func = expr->as<ASTFunction>();
        if (func && func->name == "arraySetCheck")
            bitmap_expressions.emplace(symbol, expr);
    }

    auto index_context = std::make_shared<MergeTreeIndexContext>();
    auto index_info = index_context->get(MergeTreeIndexInfo::Type::BITMAP);

    for (const auto & assigment : bitmap_expressions)
    {
        index_info->buildIndexInfo(std::const_pointer_cast<IAST>(assigment.second), building_context, metadata_snapshot);
    }

    for (const auto & assigment : bitmap_expressions)
    {
        index_info->setNonRemovableColumn(assigment.first);
    }

    for (const auto & req_col: building_context.required_columns)
    {
        bitmap_expressions.emplace(req_col.name, std::make_shared<ASTIdentifier>(req_col.name));
        index_info->setNonRemovableColumn(req_col.name);
    }

    auto actions = ProjectionStep::createActions(bitmap_expressions, building_context.input_columns, building_context.context);

    LOG_DEBUG(getLogger("buildFromProjection"),
            fmt::format("actions: {}, index_context: {}", actions->dumpDAG(), index_context->toString()));

    index_context->setProjection(actions);

    return index_context;
}

void MergeTreeIndexContext::makeProjectionForMaterializedIndex(
    const ProjectionStep * projection,
    MergeTreeIndexInfo::BuildIndexContext & building_context)
{
    if (projection)
    {
        auto assignments = projection->getAssignments();

        NamesAndTypesList head_to_project;
        std::map<String, DataTypePtr> name_to_type;
        NameToNameMap name_to_name_map;
        for (const auto & item : building_context.outputs)
        {
            name_to_name_map[item.first] = item.second;
        }
        for (const auto & name_type : building_context.input_columns)
            name_to_type[name_type.name] = name_type.type;

        NamesWithAliases index_output;
        for (auto & assignment : assignments)
        {
            auto from_column = assignment.second->getColumnName();
            auto to_column = assignment.first;
            index_output.emplace_back(from_column, to_column);

            auto jt = name_to_name_map.find(to_column);
            if (jt != name_to_name_map.end())
            {
                name_to_name_map.erase(jt);
            }

            auto jit = name_to_type.find(to_column);
            if (jit != name_to_type.end())
                head_to_project.emplace_back(from_column, jit->second);
        }

        for (auto & item : name_to_name_map)
            index_output.emplace_back(item.first, item.second);

        //ExpressionActionsPtr actions = std::make_shared<ExpressionActions>(head_to_project, building_context.context);
        // TODO: not implement
        auto actions = std::make_shared<ActionsDAG>(head_to_project);
        //actions->add(ExpressionAction::project(index_output));
        
        Names output_columns;
        for (const auto & item : index_output)
        {
            if (!item.second.empty())
                output_columns.emplace_back(item.second);
            else
                output_columns.emplace_back(item.first);
        }

        //actions->finalize(output_columns);

        LOG_DEBUG(getLogger("makeProjectionForMaterializedIndex"),
            fmt::format("index_output: {}, name_to_name_map: {}, output_columns: {}, actions: {}",
            index_output, name_to_name_map, output_columns, actions->dumpDAG()));

        project_actions_from_materialized_index = actions;
    }
}

ActionsDAGPtr MergeTreeIndexContext::getProjectionActions(MergeTreeIndexExecutor * executor) const
{
    if (executor && executor->hasReaders())
    {
        if (executor->valid())
            return project_actions_from_materialized_index;
    }

    return project_actions;
}

ActionsDAGPtr MergeTreeIndexContext::getProjectionActions(bool has_index) const
{
    if (has_index)
        return project_actions_from_materialized_index;
    else
        return project_actions;
}

/**
 * return true if any reader is valid, to optimize the query as much as possible.
 */
bool MergeTreeIndexExecutor::valid() const
{
    for (const auto & index_reader : index_readers)
    {
        if (index_reader.second && index_reader.second->validIndexReader())
            return true;
    }
    return false;
}

size_t MergeTreeIndexExecutor::read(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Columns & res)
{
    size_t result_rows = 0;

    for (auto it = index_readers.begin(); it != index_readers.end(); ++it)
    {
        if (it->second && it->second->validIndexReader())
        {
            result_rows = it->second->read(from_mark, continue_reading, max_rows_to_read, res);
            LOG_TRACE(getLogger("MergeTreeIndexExecutor"), fmt::format("IndexExecutor ({}) read from {} mark, max_rows_to_read {}, result_rows {}",
                IndexTypeToString(it->first), from_mark, max_rows_to_read, result_rows));
        }
    }

    return result_rows;
}

void MergeTreeIndexExecutor::addReader(MergeTreeIndexInfo::Type type, const MergeTreeColumnIndexReaderPtr & index_reader)
{
    index_readers.emplace(type, index_reader);
}

MergeTreeColumnIndexReaderPtr MergeTreeIndexExecutor::getReader(MergeTreeIndexInfo::Type type) const
{
    if (auto it = index_readers.find(type); it != index_readers.end())
        return it->second;
    return nullptr;
}

void MergeTreeIndexExecutor::initReader(MergeTreeIndexInfo::Type type, const NameSet & columns)
{
    if (auto it = index_readers.find(type); it != index_readers.end())
    {
        it->second->initIndexes(columns);
    }
}

}
