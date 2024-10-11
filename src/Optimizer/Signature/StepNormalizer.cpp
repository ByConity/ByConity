#include <Optimizer/Signature/StepNormalizer.h>

#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/NameToType.h>
#include <Core/Names.h>
#include <Interpreters/AggregateDescription.h>
#include <Optimizer/Rewriter/SQLFingerprintRewriter.h>
#include <Optimizer/Signature/ExpressionReorderNormalizer.h>
#include <Optimizer/Utils.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/IAST_fwd.h>
#include <Protos/plan_node.pb.h>
#include <QueryPlan/AggregatingStep.h>
#include <QueryPlan/Assignment.h>
#include <QueryPlan/FilterStep.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/PlanPrinter.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/ProjectionStep.h>
#include <QueryPlan/SymbolMapper.h>
#include <QueryPlan/TableScanStep.h>
#include <common/logger_useful.h>
#include "Core/UUID.h"
#include "Interpreters/DistributedStages/PlanSegment.h"
#include "Interpreters/StorageID.h"
#include "QueryPlan/RemoteExchangeSourceStep.h"
#include "QueryPlan/TableWriteStep.h"

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace DB
{

namespace
{
constexpr const char * NORMAL_SYMBOL_PREFIX = "__$";
// hacky fix, the __grouping_set symbol is hard-coded into the output header, and cannot be symbol-mapped
constexpr const char * GROUPING_SET_SYMBOL = "__grouping_set";
using SymbolMapping = std::unordered_map<std::string, std::string>;

/**
 * create reordered & symbol_mapped input streams according to children specifications,
 * also fills symbol_mapping and increments cumulative_pos for symbol mapper
 */
DataStreams processInputStreams(const DataStreams & input_streams,
                                const StepsAndOutputOrders & normal_children,
                                SymbolMapping & symbol_mapping,
                                size_t & cumulative_pos)
{
    Utils::checkArgument(input_streams.size() == normal_children.size(), "Normalized result must be present for every input stream");

    DataStreams normal_input_streams{};
    for (size_t i = 0; i < input_streams.size(); ++i)
    {
        const Block & original = input_streams[i].header;
        const StepAndOutputOrder & normal_child = normal_children[i];
        ColumnsWithTypeAndName normal_header(original.columns());

        const Block & input_order = normal_child.output_order;
        for (const auto & column : original)
        {
            size_t pos = input_order.getPositionByName(column.name);
            normal_header[pos] = column;
        }

        for (auto & column : normal_header)
        {
            std::string next_symbol = NORMAL_SYMBOL_PREFIX + std::to_string(cumulative_pos);
            ++cumulative_pos;
            symbol_mapping.emplace(column.name, next_symbol);
            column.name = std::move(next_symbol);
        }

        normal_input_streams.emplace_back(DataStream{Block{normal_header}});
    }

    return normal_input_streams;
}

/**
 * create output symbol mapping from different sources.
 * general case is symbols from output_header which does not appear in input stream, but it can also be created from assignments / aggs
 */
void createOutputSymbolMapping(const Block & output_header,
                               SymbolMapping & symbol_mapping,
                               size_t & cumulative_pos)
{
    for (const auto & column : output_header)
    {
        // hacky fix, the __grouping_set symbol is hard-coded into the output header, and cannot be symbol-mapped
        if (!symbol_mapping.contains(column.name) && column.name != GROUPING_SET_SYMBOL)
        {
            symbol_mapping.emplace(column.name, NORMAL_SYMBOL_PREFIX + std::to_string(cumulative_pos));
            ++cumulative_pos;
        }
    }
}

void createOutputSymbolMapping(const Assignments & assignments,
                               SymbolMapping & symbol_mapping,
                               size_t & cumulative_pos)
{
    for (const auto & [symbol, expr] : assignments)
    {
        if (!symbol_mapping.contains(symbol))
        {
            symbol_mapping.emplace(symbol, NORMAL_SYMBOL_PREFIX + std::to_string(cumulative_pos));
            ++cumulative_pos;
        }
    }
}

void createOutputSymbolMapping(const AggregateDescriptions & assignments,
                               SymbolMapping & symbol_mapping,
                               size_t & cumulative_pos)
{
    for (const auto & desc : assignments)
    {
        if (!symbol_mapping.contains(desc.column_name))
        {
            symbol_mapping.emplace(desc.column_name, NORMAL_SYMBOL_PREFIX + std::to_string(cumulative_pos));
            ++cumulative_pos;
        }
    }
}

Block getOutputOrder(const IQueryPlanStep & original, const IQueryPlanStep & normal, SymbolMapper & symbol_mapper)
{
    Utils::checkArgument(original.getOutputStream().header.columns() == normal.getOutputStream().header.columns(),
                         "Normalized step should have the same number of columns.");
    ColumnsWithTypeAndName output_order(original.getOutputStream().header.columns());
    for (const auto & column : original.getOutputStream().header)
    {
        const std::string & original_name = column.name;
        std::string mapped_name = symbol_mapper.map(original_name);
        size_t mapped_pos = normal.getOutputStream().header.getPositionByName(mapped_name);
        output_order[mapped_pos] = column;
    }
    return Block{output_order};
}

} // anonymous namespace

StepAndOutputOrder StepNormalizer::normalize(QueryPlanStepPtr step, StepsAndOutputOrders && inputs)
{
    StepAndOutputOrder res = VisitorUtil::accept(step, *this, inputs);
    if (!step->getHints().empty())
        res.normal_step->setHints(step->getHints());
    return res;
}

StepAndOutputOrder StepNormalizer::visitStep(const IQueryPlanStep & step, StepsAndOutputOrders & inputs)
{
    // phase 1: symbol mapping, will use input_stream + output_stream to create symbol mapper
    SymbolMapping symbol_mapping{};
    size_t cumulative_pos = 0;
    DataStreams normal_input_streams = processInputStreams(step.getInputStreams(), inputs, symbol_mapping, cumulative_pos);
    createOutputSymbolMapping(step.getOutputStream().header, symbol_mapping, cumulative_pos);
    SymbolMapper symbol_mapper = SymbolMapper::simpleMapper(symbol_mapping);
    QueryPlanStepPtr normal_step = symbol_mapper.map(step);

    // replace the input_stream because of reordering
    normal_step->setInputStreams(normal_input_streams);
    Block output_order = getOutputOrder(step, *normal_step, symbol_mapper);
    return StepAndOutputOrder{normal_step, std::move(output_order)};
}

StepAndOutputOrder StepNormalizer::visitTableScanStep(const TableScanStep & step, StepsAndOutputOrders & /*inputs*/)
{
    // phase 1: skipped because there is no input stream
    // phase 2: reorder column_names, column_alias, table_output_header and filters in select_query_info
    Names column_names_sorted = step.getColumnNames();
    ExpressionReorderNormalizer::reorder(column_names_sorted);
    NamesWithAliases column_alias_sorted = step.getColumnAlias();
    ExpressionReorderNormalizer::reorder(column_alias_sorted);

    Block header_sorted{};
    for (const auto & column_alias : column_alias_sorted)
    {
        // for each output symbol sorted, put into the header
        const auto & column = step.getTableOutputStream().header.getByName(column_alias.second);
        header_sorted.insert(column);
    }

    for (const auto & assignment : step.getInlineExpressions())
    {
        const auto & column = step.getTableOutputStream().header.getByName(assignment.first);
        header_sorted.insert(column);
    }

    // phase 3: normalize output symbols, the order is from reordered table_output_header
    SymbolMapping symbol_mapping{};
    size_t cumulative_pos = 0;
    createOutputSymbolMapping(header_sorted, symbol_mapping, cumulative_pos);
    SymbolMapper symbol_mapper = SymbolMapper::simpleMapper(symbol_mapping);
    DataStream mapped_table_output_stream = symbol_mapper.map(DataStream{header_sorted});
    column_alias_sorted = symbol_mapper.map(std::move(column_alias_sorted));

    Assignments inline_assignments;
    for (const auto & assignment : step.getInlineExpressions())
        inline_assignments.emplace(symbol_mapper.map(assignment.first), assignment.second);

    auto reordered_query_info = step.getQueryInfo();
    if (auto query = dynamic_pointer_cast<ASTSelectQuery>(reordered_query_info.query))
    {
        auto new_query = static_pointer_cast<ASTSelectQuery>(query->clone());
        if (new_query->where())
        {
            // normalize literals
            if (normalize_literals)
                SQLFingerprintRewriter::rewriteAST(new_query->refWhere());
            ExpressionReorderNormalizer::reorder(new_query->refWhere());
        }
        if (new_query->prewhere())
        {
            if (normalize_literals)
                SQLFingerprintRewriter::rewriteAST(new_query->refPrewhere());
            ExpressionReorderNormalizer::reorder(new_query->refPrewhere());
        }
        reordered_query_info.query = new_query;
    }

    auto normal_table_scan = std::make_shared<TableScanStep>(
        mapped_table_output_stream,
        step.getStorage(),
        step.getStorageID(),
        step.getMetadataSnapshot(),
        step.getStorageSnapshot(),
        step.getOriginalTable(),
        std::move(column_names_sorted),
        std::move(column_alias_sorted),
        reordered_query_info, // prewhere info
        step.getMaxBlockSize(),
        step.getTableAlias(), // alias
        step.isBucketScan(),
        PlanHints{}, // hints set later
        inline_assignments,
        nullptr, // push down agg
        nullptr, // push down projection
        nullptr, // push down filter
        mapped_table_output_stream);

    if (normalize_storage && step.getStorage())
        normal_table_scan->setOriginalTable(
            StorageCnchMergeTree::getOriginalTableName(normal_table_scan->getStorageID().table_name, context->getCurrentTransactionID()));

    // for table scan, we also need to normalize push downs
    if (const QueryPlanStepPtr & push_down_filter = step.getPushdownFilter())
    {
        StepsAndOutputOrders table_scan_result{StepAndOutputOrder{normal_table_scan, std::move(header_sorted)}};
        StepAndOutputOrder filter_result = normalize(push_down_filter, std::move(table_scan_result));
        normal_table_scan->setPushdownFilter(filter_result.normal_step);
        normal_table_scan->formatOutputStream(context); // format because we use this output stream immediately
        header_sorted = std::move(filter_result.output_order);
    }

    if (const QueryPlanStepPtr & push_down_projection = step.getPushdownProjection())
    {
        StepsAndOutputOrders table_scan_result{StepAndOutputOrder{normal_table_scan, std::move(header_sorted)}};
        StepAndOutputOrder projection_result = normalize(push_down_projection, std::move(table_scan_result));
        normal_table_scan->setPushdownProjection(projection_result.normal_step);
        normal_table_scan->formatOutputStream(context); // format because we use this output stream immediately
        header_sorted = std::move(projection_result.output_order);
    }

    if (const QueryPlanStepPtr & push_down_aggregation = step.getPushdownAggregation())
    {
        StepsAndOutputOrders table_scan_result{StepAndOutputOrder{normal_table_scan, std::move(header_sorted)}};
        StepAndOutputOrder aggregation_result = normalize(push_down_aggregation, std::move(table_scan_result));
        normal_table_scan->setPushdownAggregation(aggregation_result.normal_step);
        normal_table_scan->formatOutputStream(context); // format because we use this output stream immediately
        header_sorted = std::move(aggregation_result.output_order);
    }

    return StepAndOutputOrder{normal_table_scan, std::move(header_sorted)};
}

StepAndOutputOrder StepNormalizer::visitFilterStep(const FilterStep & step, StepsAndOutputOrders & inputs)
{
    if (step.getExpression())
    {
        LOG_DEBUG(log, "Reordering FilterStep with actions_dag is not supported");
        return visitStep(step, inputs);
    }

    // phase 1: normalize input symbols
    SymbolMapping symbol_mapping{};
    size_t cumulative_pos = 0;
    DataStreams normal_input_streams = processInputStreams(step.getInputStreams(), inputs, symbol_mapping, cumulative_pos);
    SymbolMapper symbol_mapper = SymbolMapper::simpleMapper(symbol_mapping);

    // phase 2: reorder and/or
    ASTPtr filter_reordered = symbol_mapper.map(step.getFilter());
    // normalize literals before reordering
    if (normalize_literals)
        SQLFingerprintRewriter::rewriteAST(filter_reordered);
    ExpressionReorderNormalizer::reorder(filter_reordered);

    // no phase 3 because there is no new symbols

    auto normal_filter = std::make_shared<FilterStep>(
        normal_input_streams.front(),
        filter_reordered,
        step.removesFilterColumn());

    Block output_order = getOutputOrder(step, *normal_filter, symbol_mapper);
    return StepAndOutputOrder{normal_filter, std::move(output_order)};
}

StepAndOutputOrder StepNormalizer::visitProjectionStep(const ProjectionStep & step, StepsAndOutputOrders & inputs)
{
    // phase 1: normalize input symbols
    SymbolMapping symbol_mapping{};
    size_t cumulative_pos = 0;
    DataStreams normal_input_streams = processInputStreams(step.getInputStreams(), inputs, symbol_mapping, cumulative_pos);
    SymbolMapper input_symbol_mapper = SymbolMapper::simpleMapper(symbol_mapping);
    // map RHS only because LHS may reuse the same symbol
    Assignments assignments_reordered = step.getAssignments();
    for (auto & pair : assignments_reordered)
    {
        auto new_ast = input_symbol_mapper.map(pair.second);
        if (normalize_literals)
            SQLFingerprintRewriter::rewriteAST(new_ast);
        pair.second = new_ast;
    }

    // phase 2: reorder assignments
    ExpressionReorderNormalizer::reorder(assignments_reordered);

    // phase 3: normalize output symbols, the order is from reordered assignments
    createOutputSymbolMapping(assignments_reordered, symbol_mapping, cumulative_pos);
    SymbolMapper symbol_mapper = SymbolMapper::simpleMapper(symbol_mapping);
    NameToType mapped_name_to_type = symbol_mapper.map(step.getNameToType()); // no need to reorder due to (ordered) map<>
    assignments_reordered = symbol_mapper.map(std::move(assignments_reordered));

    auto normal_projection = std::make_shared<ProjectionStep>(
        normal_input_streams.front(),
        assignments_reordered,
        mapped_name_to_type,
        step.isFinalProject(),
        step.isIndexProject(),
        PlanHints{}); // hints set later

    Block output_order = getOutputOrder(step, *normal_projection, symbol_mapper);
    return StepAndOutputOrder{normal_projection, std::move(output_order)};
}

/**
 * Constructing an AggregatingStep is rather complicated.
 * The input header goes to header_before_aggregation in createParams and finally becomes Params.src_header
 * The keys will be converted to their position in the input header and stored as ColumnNumbers in Params.keys
 * The arguments in aggregates will be cleared and obtained from the position of argument_names in the input header
 * Note that createParams leaves the Params.intermediate_header blank
 * Then in another constructor, the output header is computed by
 * appendGroupingColumns(params_.getHeader(final_), grouping_sets_params_, groupings_, final_),
 * getHeader() first converts keys back to their names by looking at the src_header
 * it then inserts the column_name from each aggregate desc
 * finally, is there are grouping_set_params, a __grouping_set column is added
 */
StepAndOutputOrder StepNormalizer::visitAggregatingStep(const AggregatingStep & step, StepsAndOutputOrders & inputs)
{
    if (!step.getGroupBySortDescription().empty() || !step.getGroupings().empty() || !step.getGroupingSetsParams().empty())
    {
        LOG_DEBUG(log, "Reordering AggregatingStep with groupBySort/groupings/groupingSets is not supported");
        return visitStep(step, inputs);
    }

    // phase 1: normalize input symbols
    SymbolMapping symbol_mapping{};
    size_t cumulative_pos = 0;
    DataStreams normal_input_streams = processInputStreams(step.getInputStreams(), inputs, symbol_mapping, cumulative_pos);
    SymbolMapper input_symbol_mapper = SymbolMapper::simpleMapper(symbol_mapping);

    // phase 2: reorder aggregates
    AggregateDescriptions aggregates_reordered = step.getAggregates();
    for (auto & agg : aggregates_reordered)
    {
        agg.argument_names = input_symbol_mapper.map(agg.argument_names);
        agg.mask_column = input_symbol_mapper.map(agg.mask_column);
    }
    ExpressionReorderNormalizer::reorder(aggregates_reordered);

    // phase 3: normalize all symbols, the order is from reordered aggregates
    createOutputSymbolMapping(aggregates_reordered, symbol_mapping, cumulative_pos);
    SymbolMapper symbol_mapper = SymbolMapper::simpleMapper(symbol_mapping);

    Names mapped_keys = symbol_mapper.map(step.getKeys()); // should not reorder because order is important
    aggregates_reordered = symbol_mapper.map(std::move(aggregates_reordered));

    auto normal_agg = std::make_shared<AggregatingStep>(
        normal_input_streams.front(),
        mapped_keys,
        step.getKeysNotHashed(),
        aggregates_reordered,
        GroupingSetsParamsList{}, // checked empty
        step.isFinal(),
        SortDescription{}, // checked empty
        GroupingDescriptions{}, // checked empty
        false,
        step.shouldProduceResultsInOrderOfBucketNumber(),
        step.isNoShuffle(),
        step.isStreamingForCache(),
        step.getHints());

    Block output_order = getOutputOrder(step, *normal_agg, symbol_mapper);
    return StepAndOutputOrder{normal_agg, std::move(output_order)};
}

// CTERefStep does not have input stream, but has input symbols. dealt with separately
StepAndOutputOrder StepNormalizer::visitCTERefStep(const CTERefStep & step, StepsAndOutputOrders &)
{
    const Block & original_output = step.getOutputStream().header;
    auto input_to_outputs = step.getReverseOutputColumns();
    NamesWithAliases name_alias{input_to_outputs.begin(), input_to_outputs.end()};
    ExpressionReorderNormalizer::reorder(name_alias);

    ColumnsWithTypeAndName output_order;
    ColumnsWithTypeAndName normal_output;
    SymbolMapping normal_output_to_input;

    for (size_t pos = 0; pos < name_alias.size(); ++pos)
    {
        const auto & output_column = original_output.getByName(name_alias[pos].second);
        std::string new_symbol = NORMAL_SYMBOL_PREFIX + std::to_string(pos);
        normal_output_to_input.emplace(new_symbol, name_alias[pos].first);
        output_order.emplace_back(output_column);
        normal_output.emplace_back(ColumnWithTypeAndName{output_column.column, output_column.type, new_symbol});
    }

    auto normal_cte = std::make_shared<CTERefStep>(
        DataStream{Block{normal_output}},
        step.getId(),
        normal_output_to_input,
        step.hasFilter());

    return StepAndOutputOrder{normal_cte, std::move(output_order)};
}

// CTERefStep does not have input stream, but has input symbols. dealt with separately
StepAndOutputOrder StepNormalizer::visitJoinStep(const JoinStep & step, StepsAndOutputOrders & inputs)
{
    // phase 1: symbol mapping, will use input_stream + output_stream to create symbol mapper
    SymbolMapping symbol_mapping;
    size_t cumulative_pos = 0;
    DataStreams normal_input_streams = processInputStreams(step.getInputStreams(), inputs, symbol_mapping, cumulative_pos);
    createOutputSymbolMapping(step.getOutputStream().header, symbol_mapping, cumulative_pos);
    SymbolMapper symbol_mapper = SymbolMapper::simpleMapper(symbol_mapping);
    auto normal_step = symbol_mapper.map(step);

    auto output_header = normal_step->getOutputStream().header.getColumnsWithTypeAndName();
    ExpressionReorderNormalizer::reorder(output_header);
    // replace the input_stream & output_stream because of reordering
    normal_step->setInputStreams(normal_input_streams);
    normal_step->setOutputStream(DataStream{output_header});

    Block output_order = getOutputOrder(step, *normal_step, symbol_mapper);
    return StepAndOutputOrder{normal_step, std::move(output_order)};
}

StepAndOutputOrder StepNormalizer::visitRemoteExchangeSourceStep(const RemoteExchangeSourceStep & step, StepsAndOutputOrders & /*inputs*/)
{
    // phase 1: symbol mapping, order output streams, will use only output_stream to create symbol mapper
    auto header_sorted = step.getOutputStream().header.getColumnsWithTypeAndName();
    ExpressionReorderNormalizer::reorder(header_sorted);
    SymbolMapping symbol_mapping;
    size_t cumulative_pos = 0;
    createOutputSymbolMapping(header_sorted, symbol_mapping, cumulative_pos);
    SymbolMapper symbol_mapper = SymbolMapper::simpleMapper(symbol_mapping);
    DataStream mapped_output_stream = symbol_mapper.map(DataStream{header_sorted});

    PlanSegmentInputs inputs;
    for (const auto & original_input : step.getInput())
    {
        PlanSegmentInputPtr mapped_input
            = std::make_shared<PlanSegmentInput>(symbol_mapper.map(original_input->getHeader()), original_input->getPlanSegmentType());
        inputs.emplace_back(std::move(mapped_input));
    }

    // clear input
    auto normalized_remote_exchange_source
        = std::make_shared<RemoteExchangeSourceStep>(std::move(inputs), mapped_output_stream, step.isAddTotals(), step.isAddExtremes());
    Block output_order = getOutputOrder(step, *normalized_remote_exchange_source, symbol_mapper);

    return StepAndOutputOrder{normalized_remote_exchange_source, std::move(output_order)};
}

StepAndOutputOrder StepNormalizer::visitTableWriteStep(const TableWriteStep & step, StepsAndOutputOrders & inputs)
{
    // phase 1: symbol mapping, will use input_stream + output_stream to create symbol mapper
    SymbolMapping symbol_mapping{};
    size_t cumulative_pos = 0;
    DataStreams normal_input_streams = processInputStreams(step.getInputStreams(), inputs, symbol_mapping, cumulative_pos);
    createOutputSymbolMapping(step.getOutputStream().header, symbol_mapping, cumulative_pos);
    SymbolMapper symbol_mapper = SymbolMapper::simpleMapper(symbol_mapping);
    auto new_target = step.getTarget();
    if (normalize_storage)
    {
        if (auto insert_target = std::dynamic_pointer_cast<TableWriteStep::InsertTarget>(new_target))
        {
            auto original_table_name
                = StorageCnchMergeTree::getOriginalTableName(insert_target->getStorageID().table_name, context->getCurrentTransactionID());
            StorageID new_storage_id
                = StorageID(insert_target->getStorageID().getDatabaseName(), std::move(original_table_name), UUIDHelpers::Nil);
            new_storage_id.clearUUID();
            new_target = std::make_shared<TableWriteStep::InsertTarget>(
                insert_target->getStorage(), new_storage_id, insert_target->getColumns(), insert_target->getQuery());
        }
    }
    auto normal_table_write_step
        = std::make_shared<TableWriteStep>(symbol_mapper.map(step.getInputStreams()[0]), new_target, step.isOutputProfiles(), "inserted_rows");


    // replace the input_stream because of reordering
    normal_table_write_step->setInputStreams(normal_input_streams);
    Block output_order = getOutputOrder(step, *normal_table_write_step, symbol_mapper);
    return StepAndOutputOrder{normal_table_write_step, std::move(output_order)};
}
}
