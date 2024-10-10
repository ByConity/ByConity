/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <Optimizer/Rewriter/ColumnPruning.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/join_common.h>
#include <Optimizer/Correlation.h>
#include <Optimizer/ExpressionDeterminism.h>
#include <Optimizer/ExpressionInterpreter.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/SymbolsExtractor.h>
#include <Parsers/ASTIdentifier.h>
#include <QueryPlan/AggregatingStep.h>
#include <QueryPlan/ApplyStep.h>
#include <QueryPlan/AssignUniqueIdStep.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/DistinctStep.h>
#include <QueryPlan/Dummy.h>
#include <QueryPlan/ExceptStep.h>
#include <QueryPlan/ExpandStep.h>
#include <QueryPlan/FilterStep.h>
#include <QueryPlan/IntersectStep.h>
#include <QueryPlan/JoinStep.h>
#include <QueryPlan/LimitByStep.h>
#include <QueryPlan/MergeSortingStep.h>
#include <QueryPlan/MergingSortedStep.h>
#include <QueryPlan/PartialSortingStep.h>
#include <QueryPlan/ProjectionStep.h>
#include <QueryPlan/UnionStep.h>
#include <QueryPlan/WindowStep.h>
#include <QueryPlan/OutfileWriteStep.h>

namespace DB
{
bool ColumnPruning::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    ColumnPruningVisitor visitor{
        context,
        plan.getCTEInfo(),
        plan.getPlanNode(),
        false,
        false,
        false};
    NameSet require;
    for (const auto & item : plan.getPlanNode()->getStep()->getOutputStream().header)
        require.insert(item.name);
    ColumnPruningContext column_pruning_context{.name_set = require};
    auto result = VisitorUtil::accept(plan.getPlanNode(), visitor, column_pruning_context);
    plan.update(result);
    return true;
}

bool AddProjectionPruning::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    ColumnPruningVisitor visitor{
        context,
        plan.getCTEInfo(),
        plan.getPlanNode(),
        true,
        false,
        false};
    NameSet require;
    for (const auto & item : plan.getPlanNode()->getStep()->getOutputStream().header)
        require.insert(item.name);
    ColumnPruningContext column_pruning_context{.name_set = require};
    auto result = VisitorUtil::accept(plan.getPlanNode(), visitor, column_pruning_context);
    plan.update(result);
    return true;
}

bool DistinctToAggregatePruning::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    ColumnPruningVisitor visitor{
        context,
        plan.getCTEInfo(),
        plan.getPlanNode(),
        false,
        true,
        false};
    NameSet require;
    for (const auto & item : plan.getPlanNode()->getStep()->getOutputStream().header)
        require.insert(item.name);
    ColumnPruningContext column_pruning_context{.name_set = require};
    auto result = VisitorUtil::accept(plan.getPlanNode(), visitor, column_pruning_context);
    plan.update(result);
    return true;
}

bool WindowToSortPruning::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    ColumnPruningVisitor visitor{
        context,
        plan.getCTEInfo(),
        plan.getPlanNode(),
        false,
        false,
        true};
    NameSet require;
    for (const auto & item : plan.getPlanNode()->getStep()->getOutputStream().header)
        require.insert(item.name);
    ColumnPruningContext column_pruning_context{.name_set = require};
    auto result = VisitorUtil::accept(plan.getPlanNode(), visitor, column_pruning_context);
    plan.update(result);
    return true;
}

template <bool require_all>
PlanNodePtr ColumnPruningVisitor::visitDefault(PlanNodeBase & node, ColumnPruningContext & column_pruning_context)
{
    if (node.getChildren().empty())
        return node.shared_from_this();

    NameSet & require = column_pruning_context.name_set;

    if constexpr (require_all)
    {
        // add all output columns into require, to prevent any pruning in source steps
        for (const auto & col_with_name : node.getCurrentDataStream().header)
            require.insert(col_with_name.name);
    }

    PlanNodes children;
    for (const auto & item : node.getChildren())
    {
        auto child_require = require;
        ColumnPruningContext child_column_pruning_context{.name_set = child_require};
        PlanNodePtr child = VisitorUtil::accept(*item, *this, child_column_pruning_context);
        children.emplace_back(child);
    }

    node.replaceChildren(children);
    return node.shared_from_this();
}

PlanNodePtr ColumnPruningVisitor::visitPlanNode(PlanNodeBase & node, ColumnPruningContext & column_pruning_context)
{
    return visitDefault<true>(node, column_pruning_context);
}

PlanNodePtr ColumnPruningVisitor::visitFinishSortingNode(FinishSortingNode & node, ColumnPruningContext & column_pruning_context)
{
    return visitPlanNode(node, column_pruning_context);
}

PlanNodePtr ColumnPruningVisitor::visitOffsetNode(OffsetNode & node, ColumnPruningContext & column_pruning_context)
{
    return visitDefault<false>(node, column_pruning_context);
}

PlanNodePtr ColumnPruningVisitor::visitTableFinishNode(TableFinishNode & node, ColumnPruningContext &)
{
    NameSet require;
    PlanNodePtr child = node.getChildren()[0];
    for (const auto & item : child->getCurrentDataStream().header)
        require.insert(item.name);
    ColumnPruningContext child_column_pruning_context{.name_set = require};
    PlanNodePtr new_child = VisitorUtil::accept(*child, *this, child_column_pruning_context);
    node.replaceChildren({new_child});
    return node.shared_from_this();
}

PlanNodePtr ColumnPruningVisitor::visitOutfileFinishNode(OutfileFinishNode & node, ColumnPruningContext & column_pruning_context)
{
    return visitPlanNode(node, column_pruning_context);
}

PlanNodePtr ColumnPruningVisitor::visitLimitNode(LimitNode & node, ColumnPruningContext & column_pruning_context)
{
    return visitDefault<false>(node, column_pruning_context);
}

PlanNodePtr
ColumnPruningVisitor::visitReadStorageRowCountNode(ReadStorageRowCountNode & node, ColumnPruningContext & column_pruning_context)
{
    return visitDefault<false>(node, column_pruning_context);
}

PlanNodePtr ColumnPruningVisitor::visitIntersectOrExceptNode(IntersectOrExceptNode & node, ColumnPruningContext &)
{
    const auto * step = node.getStep().get();

    std::vector<size_t> require_index;

    size_t index = 0;
    DataStream output_stream;
    for (const auto & item : step->getOutputStream().header)
    {
        require_index.emplace_back(index);
        output_stream.header.insert(item);
        index++;
    }

    /// count(*) requires nothing but we need gave some rows.
    if (require_index.empty())
        require_index.emplace_back(0);

    PlanNodes children;
    DataStreams children_streams;
    for (const auto & child : node.getChildren())
    {
        NameSet child_require;
        for (const auto & item : require_index)
            child_require.insert(child->getStep()->getOutputStream().header.getByPosition(item).name);

        ColumnPruningContext child_column_pruning_context{.name_set = child_require};
        auto new_child = VisitorUtil::accept(child, *this, child_column_pruning_context);
        children_streams.emplace_back(new_child->getStep()->getOutputStream());
        children.emplace_back(new_child);
    }

    auto intersect_except_step = std::make_shared<IntersectOrExceptStep>(children_streams, step->getOperator(), step->getMaxThreads());
    auto intersect_except_node
        = IntersectOrExceptNode::createPlanNode(context->nextNodeId(), std::move(intersect_except_step), children, node.getStatistics());
    return intersect_except_node;
}

PlanNodePtr ColumnPruningVisitor::visitMultiJoinNode(MultiJoinNode & node, ColumnPruningContext & column_pruning_context)
{
    return visitDefault<false>(node, column_pruning_context);
}

PlanNodePtr ColumnPruningVisitor::visitFinalSampleNode(FinalSampleNode & node, ColumnPruningContext & column_pruning_context)
{
    return visitDefault<false>(node, column_pruning_context);
}

PlanNodePtr ColumnPruningVisitor::visitEnforceSingleRowNode(EnforceSingleRowNode & node, ColumnPruningContext & column_pruning_context)
{
    return visitDefault<false>(node, column_pruning_context);
}

PlanNodePtr ColumnPruningVisitor::visitLocalExchangeNode(LocalExchangeNode & node, ColumnPruningContext & column_pruning_context)
{
    return visitDefault<false>(node, column_pruning_context);
}

PlanNodePtr ColumnPruningVisitor::visitValuesNode(ValuesNode & node, ColumnPruningContext & column_pruning_context)
{
    const auto * step = node.getStep().get();
    NameSet & require = column_pruning_context.name_set;

    const auto & fields = step->getFields();
    const auto & output_stream = step->getOutputStream();
    NamesAndTypes header;
    Fields data;
    for (size_t index = 0; index < fields.size(); ++index)
    {
        if (require.empty())
            break;

        auto name_type = output_stream.header.getByPosition(index);
        if (require.contains(name_type.name))
        {
            header.emplace_back(name_type.name, name_type.type);
            data.emplace_back(fields[index]);
        }
    }

    if (header.empty())
    {
        auto name_type = output_stream.header.getByPosition(0);
        header.emplace_back(name_type.name, name_type.type);
        data.emplace_back(fields[0]);
    }

    auto values_step = std::make_shared<ValuesStep>(header, data, step->getRows());
    auto values_node = ValuesNode::createPlanNode(context->nextNodeId(), std::move(values_step), {}, node.getStatistics());
    return values_node;
}

PlanNodePtr ColumnPruningVisitor::visitBufferNode(BufferNode & node, ColumnPruningContext & column_pruning_context)
{
    return visitDefault<false>(node, column_pruning_context);
}

PlanNodePtr ColumnPruningVisitor::visitRemoteExchangeSourceNode(RemoteExchangeSourceNode &, ColumnPruningContext &)
{
    throw Exception("Not impl column pruning", ErrorCodes::NOT_IMPLEMENTED);
}

PlanNodePtr ColumnPruningVisitor::visitReadNothingNode(ReadNothingNode & node, ColumnPruningContext & column_pruning_context)
{
    return visitDefault<false>(node, column_pruning_context);
}

PlanNodePtr ColumnPruningVisitor::visitPartitionTopNNode(PartitionTopNNode & node, ColumnPruningContext & column_pruning_context)
{
    const auto * step = node.getStep().get();
    NameSet & require = column_pruning_context.name_set;

    for (const auto & name : step->getPartition())
        require.insert(name);

    for (const auto & name : step->getOrderBy())
        require.insert(name);

    return visitDefault<false>(node, column_pruning_context);
}

PlanNodePtr ColumnPruningVisitor::visitExtremesNode(ExtremesNode & node, ColumnPruningContext & column_pruning_context)
{
    return visitDefault<false>(node, column_pruning_context);
}

PlanNodePtr ColumnPruningVisitor::visitLimitByNode(LimitByNode & node, ColumnPruningContext & column_pruning_context)
{
    const auto * step = node.getStep().get();
    NameSet & require = column_pruning_context.name_set;

    for (const auto & item : step->getColumns())
        require.insert(item);
    auto child = VisitorUtil::accept(node.getChildren()[0], *this, column_pruning_context);
    auto limit_step = std::make_shared<LimitByStep>(
        child->getStep()->getOutputStream(), step->getGroupLength(), step->getGroupOffset(), step->getColumns());
    return LimitByNode::createPlanNode(context->nextNodeId(), std::move(limit_step), PlanNodes{child}, node.getStatistics());
}

PlanNodePtr ColumnPruningVisitor::visitWindowNode(WindowNode & node, ColumnPruningContext & column_pruning_context)
{
    const auto * step = node.getStep().get();
    NameSet & require = column_pruning_context.name_set;

    NameSet child_require = require;

    std::vector<WindowFunctionDescription> window_functions;
    for (const auto & function : step->getFunctions())
    {
        if (!require.contains(function.column_name))
            continue;

        child_require.erase(function.column_name);

        window_functions.push_back(function);
        child_require.insert(function.argument_names.begin(), function.argument_names.end());
    }
    for (const auto & item : step->getWindow().order_by)
        child_require.insert(item.column_name);

    for (const auto & item : step->getWindow().partition_by)
        child_require.insert(item.column_name);

    for (const auto & item : step->getWindow().full_sort_description)
        child_require.insert(item.column_name);

    ColumnPruningContext child_column_pruning_context{.name_set = child_require};
    auto child = VisitorUtil::accept(node.getChildren()[0], *this, child_column_pruning_context);

    if (window_functions.empty())
        return child;

    auto window_step = std::make_shared<WindowStep>(
        child->getStep()->getOutputStream(), step->getWindow(), window_functions, step->needSort(), step->getPrefixDescription());

    PlanNodes children{child};
    return WindowNode::createPlanNode(context->nextNodeId(), std::move(window_step), children, node.getStatistics());
}

PlanNodePtr ColumnPruningVisitor::visitFilterNode(FilterNode & node, ColumnPruningContext & column_pruning_context)
{
    const auto * step = node.getStep().get();
    NameSet & require = column_pruning_context.name_set;

    bool remove = !require.contains(step->getFilterColumnName());

    NameSet child_require = require;
    const auto & filter = step->getFilter();
    auto symbols = SymbolsExtractor::extract(filter);
    child_require.insert(symbols.begin(), symbols.end());

    ColumnPruningContext child_column_pruning_context{.name_set = child_require};
    auto child = VisitorUtil::accept(node.getChildren()[0], *this, child_column_pruning_context);

    auto expr_step = std::make_shared<FilterStep>(child->getStep()->getOutputStream(), step->getFilter(), remove);
    PlanNodes children{child};
    auto expr_node = FilterNode::createPlanNode(context->nextNodeId(), std::move(expr_step), children, node.getStatistics());
    if (remove && filter_window_to_sort_limit)
        return convertFilterWindowToSortingLimit(expr_node, require);
    if (!column_pruning_context.is_parent_from_projection)
    {
        return addProjection(expr_node, require);
    }
    return expr_node;
}

PlanNodePtr ColumnPruningVisitor::visitArrayJoinNode(ArrayJoinNode & node, ColumnPruningContext & column_pruning_context)
{
    const auto * step = node.getStep().get();
    NameSet & require = column_pruning_context.name_set;

    NameSet child_require = require;
    for (const auto & item : step->getResultNameSet())
        child_require.insert(item);

    ColumnPruningContext child_column_pruning_context{.name_set = child_require};
    auto child = VisitorUtil::accept(node.getChildren()[0], *this, child_column_pruning_context);
    auto array_join_step = std::make_shared<ArrayJoinStep>(child->getCurrentDataStream(), step->arrayJoin());
    return ArrayJoinNode::createPlanNode(context->nextNodeId(), std::move(array_join_step), {child}, node.getStatistics());
}

PlanNodePtr ColumnPruningVisitor::visitProjectionNode(ProjectionNode & node, ColumnPruningContext & column_pruning_context)
{
    const auto * step = node.getStep().get();
    NameSet & require = column_pruning_context.name_set;

    if (step->isFinalProject())
    {
        require = NameSet{};
        for (const auto & item : step->getAssignments())
            require.insert(item.first);
    }

    NameSet child_require;

    Assignments assignments;
    NameToType name_to_type;
    for (const auto & assignment : step->getAssignments())
    {
        if (require.contains(assignment.first) || !ExpressionDeterminism::isDeterministic(assignment.second, context))
        {
            const auto & ast = assignment.second;
            auto symbols = SymbolsExtractor::extract(ast);
            child_require.insert(symbols.begin(), symbols.end());
            assignments.emplace_back(assignment);
            name_to_type[assignment.first] = step->getNameToType().at(assignment.first);
        }
    }
    ColumnPruningContext child_column_pruning_context{.name_set = child_require, .is_parent_from_projection = true};
    auto child = VisitorUtil::accept(node.getChildren()[0], *this, child_column_pruning_context);

    // if empty project return child node.
    if (assignments.empty())
        return child;

    auto expr_step = std::make_shared<ProjectionStep>(
        child->getStep()->getOutputStream(), assignments, name_to_type, step->isFinalProject(), step->isIndexProject());
    PlanNodes children{child};
    auto expr_node = ProjectionNode::createPlanNode(context->nextNodeId(), std::move(expr_step), children, node.getStatistics());
    return expr_node;
}

PlanNodePtr ColumnPruningVisitor::visitExpandNode(ExpandNode & node, ColumnPruningContext & column_pruning_context)
{
    const auto * step = node.getStep().get();
    NameSet & require = column_pruning_context.name_set;

    NameSet child_require;
    for (const auto & column : require)
    {
        if (column != step->getGroupIdSymbol())
        {
            child_require.emplace(column);
        }
    }

    ColumnPruningContext child_column_pruning_context{.name_set = child_require};
    auto child = VisitorUtil::accept(node.getChildren()[0], *this, child_column_pruning_context);

    Assignments assignments;
    for (const auto & assignment : step->getAssignments())
        if (child_require.contains(assignment.first))
            assignments.emplace_back(assignment.first, assignment.second);
    NameToType name_to_type;
    for (const auto & item : step->getNameToType())
        if (child_require.contains(item.first))
            name_to_type.emplace(item.first, item.second);

    auto expr_step = std::make_shared<ExpandStep>(
        child->getStep()->getOutputStream(),
        assignments,
        name_to_type,
        step->getGroupIdSymbol(),
        step->getGroupIdValue(),
        step->getGroupIdNonNullSymbol());
    PlanNodes children{child};
    auto expr_node = ExpandNode::createPlanNode(context->nextNodeId(), std::move(expr_step), children, node.getStatistics());
    return expr_node;
}

PlanNodePtr ColumnPruningVisitor::visitApplyNode(ApplyNode & node, ColumnPruningContext & column_pruning_context)
{
    NameSet right_require;
    for (const auto & item : node.getChildren()[1]->getStep()->getOutputStream().header)
    {
        right_require.insert(item.name);
    }
    ColumnPruningContext right_column_pruning_context{.name_set = right_require};
    auto right = VisitorUtil::accept(node.getChildren()[1], *this, right_column_pruning_context);

    const auto * step = node.getStep().get();
    NameSet & require = column_pruning_context.name_set;

    Names correlation = Correlation::prune(right, step->getCorrelation());

    NameSet left_require = require;
    left_require.insert(correlation.begin(), correlation.end());

    const auto & assignment = step->getAssignment();
    auto ast = assignment.second;
    if (ast && ast->as<ASTFunction>())
    {
        const auto & fun = ast->as<ASTFunction &>();
        if (fun.name == "in" || fun.name == "notIn" || fun.name == "globalIn" || fun.name == "globalNotIn")
        {
            ASTIdentifier & in_left = fun.arguments->getChildren()[0]->as<ASTIdentifier &>();
            left_require.insert(in_left.name());
        }
    }
    else if (ast && ast->as<ASTQuantifiedComparison>())
    {
        const auto & qc = ast->as<ASTQuantifiedComparison &>();
        ASTIdentifier & qc_left = qc.children[0]->as<ASTIdentifier &>();
        left_require.insert(qc_left.name());
    }

    ColumnPruningContext left_column_pruning_context{.name_set = left_require};
    auto left = VisitorUtil::accept(node.getChildren()[0], *this, left_column_pruning_context);

    DataStreams input{left->getStep()->getOutputStream(), right->getStep()->getOutputStream()};

    auto apply_step = std::make_shared<ApplyStep>(
        input,
        correlation,
        step->getApplyType(),
        step->getSubqueryType(),
        step->getAssignment(),
        step->getOuterColumns(),
        step->supportSemiAnti());
    PlanNodes children{left, right};
    auto apply_node = ApplyNode::createPlanNode(context->nextNodeId(), std::move(apply_step), children, node.getStatistics());
    return apply_node;
}

PlanNodePtr ColumnPruningVisitor::visitTableScanNode(TableScanNode & node, ColumnPruningContext & column_pruning_context)
{
    const auto * step = node.getStep().get();
    NameSet & require = column_pruning_context.name_set;

    if (step->getPushdownAggregation() || step->getPushdownProjection() || step->getPushdownFilter())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "TableScan with pushdown steps can not be processed for column pruning.");

    bool contains_all_columns = true;
    NamesWithAliases column_names;
    for (const auto & item : step->getColumnAlias())
    {
        if (require.contains(item.second))
            column_names.emplace_back(item);
        else
            contains_all_columns = false;
    }

    Assignments inline_expressions;
    for (const auto & ass : step->getInlineExpressions())
    {
        if (require.contains(ass.first))
            inline_expressions.emplace(ass.first, ass.second);
        else
            contains_all_columns = false;
    }

    if (contains_all_columns)
        return node.shared_from_this();

    if (column_names.empty() && inline_expressions.empty())
    {
        // select a minimal column from the present columns to be read
        auto storage = step->getStorage();
        auto metadata_snapshot = storage->getInMemoryMetadataPtr();
        const auto & columns_desc = metadata_snapshot->getColumns();
        auto column_to_alias = step->getColumnToAliasMap();
        NamesAndTypesList candidate_columns;

        if (!column_to_alias.empty())
        {
            for (const auto & pair : column_to_alias)
                // Hack: ColumnPruning::selectColumnWithMinSize ignores subcolumn, by checking `NameAndTypePair::subcolumn_delimiter_position`.
                // This is unexpected, so we rebuild the NameAndTypePair
                candidate_columns.emplace_back(
                    pair.first, columns_desc.getColumnOrSubcolumn(GetColumnsOptions::AllPhysical, pair.first).type);
        }
        else
        {
            candidate_columns = columns_desc.getAllPhysical();
        }

        auto min_size_column = selectColumnWithMinSize(std::move(candidate_columns), storage);
        column_names.emplace_back(
            min_size_column,
            column_to_alias.contains(min_size_column) ? column_to_alias[min_size_column]
                                                      : context->getSymbolAllocator()->newSymbol(min_size_column));
    }

    auto read_step = std::make_shared<TableScanStep>(
        context,
        step->getStorageID(),
        column_names,
        step->getQueryInfo(),
        step->getMaxBlockSize(),
        step->getTableAlias(),
        step->isBucketScan(),
        step->getHints(),
        inline_expressions);

    auto read_node = PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(read_step), {}, node.getStatistics());
    return read_node;
}

PlanNodePtr ColumnPruningVisitor::visitAggregatingNode(AggregatingNode & node, ColumnPruningContext & column_pruning_context)
{
    const auto * step = node.getStep().get();
    NameSet & require = column_pruning_context.name_set;

    NameSet child_require{step->getKeys().begin(), step->getKeys().end()};

    AggregateDescriptions aggs;
    NameSet names;
    for (const auto & agg : step->getAggregates())
    {
        if (((agg.argument_names.size() == 1 && require.contains(agg.argument_names[0])) || require.contains(agg.column_name))
            && !names.contains(agg.column_name))
        {
            aggs.push_back(agg);
            child_require.insert(agg.argument_names.begin(), agg.argument_names.end());
            names.insert(agg.column_name);
        }
    }

    ColumnPruningContext child_column_pruning_context{.name_set = child_require};
    auto child = VisitorUtil::accept(node.getChildren()[0], *this, child_column_pruning_context);
    if (aggs.empty() && step->getKeys().empty())
    {
        auto [symbol, plan_node] = createDummyPlanNode(context);
        (void)symbol;
        // require_.insert(symbol);
        return plan_node;
    }

    Names new_keys;
    NameSet new_keys_not_hashed = step->getKeysNotHashed();
    for (const auto & key : step->getKeys())
    {
        if (!require.contains(key) && new_keys_not_hashed.contains(key))
        {
            new_keys_not_hashed.erase(key);
        }
        else
        {
            new_keys.push_back(key);
        }
    }

    auto agg_step = std::make_shared<AggregatingStep>(
        child->getStep()->getOutputStream(),
        new_keys,
        new_keys_not_hashed,
        aggs,
        step->getGroupingSetsParams(),
        step->isFinal(),
        step->getGroupBySortDescription(),
        step->getGroupings(),
        step->needOverflowRow(),
        step->shouldProduceResultsInOrderOfBucketNumber(),
        step->isNoShuffle(),
        step->isStreamingForCache(),
        //        step->getHaving(),
        //        step->getInteresteventsInfoList()
        step->getHints());

    PlanNodes children{child};
    auto agg_node = AggregatingNode::createPlanNode(context->nextNodeId(), std::move(agg_step), children, node.getStatistics());
    return agg_node;
}

PlanNodePtr ColumnPruningVisitor::visitMarkDistinctNode(MarkDistinctNode & node, ColumnPruningContext & column_pruning_context)
{
    const auto * step = node.getStep().get();
    NameSet & require = column_pruning_context.name_set;

    for (const auto & distinct_symbol : step->getDistinctSymbols())
    {
        require.insert(distinct_symbol);
    }
    auto child = VisitorUtil::accept(node.getChildren()[0], *this, column_pruning_context);
    auto mark_distinct_step
        = std::make_shared<MarkDistinctStep>(child->getStep()->getOutputStream(), step->getMarkerSymbol(), step->getDistinctSymbols());
    return MarkDistinctNode::createPlanNode(context->nextNodeId(), std::move(mark_distinct_step), PlanNodes{child}, node.getStatistics());
}

PlanNodePtr ColumnPruningVisitor::visitSortingNode(SortingNode & node, ColumnPruningContext & column_pruning_context)
{
    const auto * step = node.getStep().get();
    NameSet & require = column_pruning_context.name_set;

    for (const auto & item : step->getSortDescription())
    {
        require.insert(item.column_name);
    }
    auto child = addProjection(VisitorUtil::accept(node.getChildren()[0], *this, column_pruning_context), require);
    auto sort_step = std::make_shared<SortingStep>(
        child->getStep()->getOutputStream(), step->getSortDescription(), step->getLimit(), step->getStage(), step->getPrefixDescription());
    return SortingNode::createPlanNode(context->nextNodeId(), std::move(sort_step), PlanNodes{child}, node.getStatistics());
}

PlanNodePtr ColumnPruningVisitor::visitMergeSortingNode(MergeSortingNode & node, ColumnPruningContext & column_pruning_context)
{
    const auto * step = node.getStep().get();
    NameSet & require = column_pruning_context.name_set;

    for (const auto & item : step->getSortDescription())
    {
        require.insert(item.column_name);
    }
    auto child = addProjection(VisitorUtil::accept(node.getChildren()[0], *this, column_pruning_context), require);
    auto sort_step = std::make_shared<MergeSortingStep>(child->getStep()->getOutputStream(), step->getSortDescription(), step->getLimit());
    return MergeSortingNode::createPlanNode(context->nextNodeId(), std::move(sort_step), PlanNodes{child}, node.getStatistics());
}

PlanNodePtr ColumnPruningVisitor::visitMergingSortedNode(MergingSortedNode & node, ColumnPruningContext & column_pruning_context)
{
    const auto * step = node.getStep().get();
    NameSet & require = column_pruning_context.name_set;

    for (const auto & item : step->getSortDescription())
    {
        require.insert(item.column_name);
    }
    auto child = addProjection(VisitorUtil::accept(node.getChildren()[0], *this, column_pruning_context), require);
    auto sort_step = std::make_shared<MergingSortedStep>(
        child->getStep()->getOutputStream(), step->getSortDescription(), step->getMaxBlockSize(), step->getLimit());
    return MergingSortedNode::createPlanNode(context->nextNodeId(), std::move(sort_step), PlanNodes{child}, node.getStatistics());
}

PlanNodePtr ColumnPruningVisitor::visitPartialSortingNode(PartialSortingNode & node, ColumnPruningContext & column_pruning_context)
{
    const auto * step = node.getStep().get();
    NameSet & require = column_pruning_context.name_set;

    for (const auto & item : step->getSortDescription())
    {
        require.insert(item.column_name);
    }
    auto child = addProjection(VisitorUtil::accept(node.getChildren()[0], *this, column_pruning_context), require);
    auto sort_step
        = std::make_shared<PartialSortingStep>(child->getStep()->getOutputStream(), step->getSortDescription(), step->getLimit());
    return PartialSortingNode::createPlanNode(context->nextNodeId(), std::move(sort_step), PlanNodes{child}, node.getStatistics());
}

PlanNodePtr ColumnPruningVisitor::visitJoinNode(JoinNode & node, ColumnPruningContext & column_pruning_context)
{
    const auto * step = node.getStep().get();
    NameSet & require = column_pruning_context.name_set;

    auto filter = step->getFilter()->clone();
    std::set<String> symbols = SymbolsExtractor::extract(filter);
    for (const auto & runtime_filter : step->getRuntimeFilterBuilders())
        symbols.insert(runtime_filter.first);

    NameSet left_require = require;
    left_require.insert(step->getLeftKeys().begin(), step->getLeftKeys().end());
    left_require.insert(symbols.begin(), symbols.end());
    ColumnPruningContext left_column_pruning_context{.name_set = left_require};
    auto left = VisitorUtil::accept(node.getChildren()[0], *this, left_column_pruning_context);

    NameSet right_require = require;
    right_require.insert(step->getRightKeys().begin(), step->getRightKeys().end());
    right_require.insert(symbols.begin(), symbols.end());

    ColumnPruningContext right_column_pruning_context{.name_set = right_require};
    auto right = VisitorUtil::accept(node.getChildren()[1], *this, right_column_pruning_context);

    DataStreams inputs{left->getStep()->getOutputStream(), right->getStep()->getOutputStream()};

    ColumnsWithTypeAndName output_header;
    const auto & left_header = left->getStep()->getOutputStream().header;
    const auto & right_header = right->getStep()->getOutputStream().header;

    // remove un-referenced output symbols
    // todo keep order
    for (const auto & header : left_header)
    {
        if (require.contains(header.name))
        {
            output_header.emplace_back(header);
        }
    }
    for (const auto & header : right_header)
    {
        if (require.contains(header.name))
        {
            output_header.emplace_back(header);
        }
    }

    /// must have one output column
    if (output_header.empty())
    {
        if (left_header.columns() != 0)
        {
            output_header.emplace_back(left_header.getByPosition(0));
            left_require.insert(left_header.getByPosition(0).name);
            ColumnPruningContext left_column_pruning_context_2{.name_set = left_require};
            left = addProjection(VisitorUtil::accept(node.getChildren()[0], *this, left_column_pruning_context_2), left_require);
        }
        else if (right_header.columns() != 0)
        {
            output_header.emplace_back(right_header.getByPosition(0));
            right_require.insert(right_header.getByPosition(0).name);
            ColumnPruningContext right_column_pruning_context_2{.name_set = right_require};
            right = addProjection(VisitorUtil::accept(node.getChildren()[1], *this, right_column_pruning_context_2), right_require);
        }
        else
        {
            throw Exception("Join no input symbols", ErrorCodes::LOGICAL_ERROR);
        }
    }

    // column pruning can't change the output type of join.
    for (auto & output : output_header)
    {
        for (const auto & origin_output : step->getOutputStream().header)
            if (output.name == origin_output.name)
            {
                if (isNullableOrLowCardinalityNullable(origin_output.type))
                {
                    output.type = JoinCommon::tryConvertTypeToNullable(output.type);
                    output.column = makeNullableOrLowCardinalityNullable(output.column);
                }
            }
    }

    auto join_step = std::make_shared<JoinStep>(
        inputs,
        DataStream{output_header},
        step->getKind(),
        step->getStrictness(),
        step->getMaxStreams(),
        step->getKeepLeftReadInOrder(),
        step->getLeftKeys(),
        step->getRightKeys(),
        step->getKeyIdsNullSafe(),
        step->getFilter(),
        step->isHasUsing(),
        step->getRequireRightKeys(),
        step->getAsofInequality(),
        step->getDistributionType(),
        step->getJoinAlgorithm(),
        step->isMagic(),
        step->isOrdered(),
        step->isSimpleReordered(),
        step->getRuntimeFilterBuilders(),
        step->getHints());

    PlanNodes children{left, right};
    auto join_node = JoinNode::createPlanNode(context->nextNodeId(), std::move(join_step), children, node.getStatistics());
    return join_node;
}

PlanNodePtr ColumnPruningVisitor::visitDistinctNode(DistinctNode & node, ColumnPruningContext & column_pruning_context)
{
    const auto * step = node.getStep().get();
    NameSet & require = column_pruning_context.name_set;

    NameSet child_require = require;
    const auto & columns = step->getColumns();
    child_require.insert(columns.begin(), columns.end());

    // If there are non-distinct columns in the require, DistinctStep cannot be converted to group by.
    NameSet distinct_requrie_set;
    for (auto & name_type : step->getOutputStream().getNamesToTypes())
    {
        if (child_require.contains(name_type.first))
            distinct_requrie_set.emplace(name_type.first);
    }
    bool can_convert_group_by = true;
    NameSet distinct_set{columns.begin(), columns.end()};
    for (const auto & require_column : distinct_requrie_set)
    {
        if (!distinct_set.contains(require_column))
        {
            can_convert_group_by = false;
            break;
        }
    }

    child_require.insert(columns.begin(), columns.end());

    ColumnPruningContext child_column_pruning_context{.name_set = child_require};
    auto child = VisitorUtil::accept(node.getChildren()[0], *this, child_column_pruning_context);

    auto distinct_step = std::make_shared<DistinctStep>(
        child->getStep()->getOutputStream(), step->getSetSizeLimits(), step->getLimitHint(), columns, step->preDistinct(), step->canToAgg());

    PlanNodes children{child};
    auto distinct_node = DistinctNode::createPlanNode(context->nextNodeId(), std::move(distinct_step), children, node.getStatistics());

    if (can_convert_group_by && distinct_to_aggregate)
        return convertDistinctToGroupBy(distinct_node);

    return distinct_node;
}

PlanNodePtr ColumnPruningVisitor::visitUnionNode(UnionNode & node, ColumnPruningContext & column_pruning_context)
{
    const auto * step = node.getStep().get();
    NameSet & require = column_pruning_context.name_set;

    // must have one require, if there is not any columns, the children can return random columns, so the result is incorrect.
    if (require.empty())
    {
        require.emplace(step->getOutputStream().header.getByPosition(0).name);
    }

    std::vector<String> require_columns;

    DataStream output_stream;
    std::unordered_map<String, std::vector<String>> output_to_inputs;
    for (const auto & item : step->getOutputStream().header)
    {
        if (require.contains(item.name))
        {
            require_columns.emplace_back(item.name);
            output_stream.header.insert(item);
            output_to_inputs[item.name] = step->getOutToInputs().at(item.name);
        }
    }

    PlanNodes children;
    DataStreams children_streams;
    for (size_t i = 0; i < node.getChildren().size(); i++)
    {
        const auto & child = node.getChildren()[i];

        NameSet child_require;
        for (const auto & item : require_columns)
            child_require.insert(output_to_inputs.at(item)[i]);

        /// count(*) requires nothing but we need gave some rows.
        if (child_require.empty())
            child_require.emplace(child->getStep()->getOutputStream().header.getByPosition(0).name);

        ColumnPruningContext child_column_pruning_context{.name_set = child_require};
        auto new_child = VisitorUtil::accept(child, *this, child_column_pruning_context);
        children_streams.emplace_back(new_child->getStep()->getOutputStream());
        children.emplace_back(new_child);
    }

    auto union_step
        = std::make_shared<UnionStep>(children_streams, output_stream, output_to_inputs, step->getMaxThreads(), step->isLocal());
    auto union_node = UnionNode::createPlanNode(context->nextNodeId(), std::move(union_step), children, node.getStatistics());
    return union_node;
}

PlanNodePtr ColumnPruningVisitor::visitExceptNode(ExceptNode & node, ColumnPruningContext &)
{
    const auto * step = node.getStep().get();

    std::vector<size_t> require_index;

    size_t index = 0;
    DataStream output_stream;
    for (const auto & item : step->getOutputStream().header)
    {
        require_index.emplace_back(index);
        output_stream.header.insert(item);
        index++;
    }

    /// count(*) requires nothing but we need gave some rows.
    if (require_index.empty())
        require_index.emplace_back(0);

    PlanNodes children;
    DataStreams children_streams;
    for (const auto & child : node.getChildren())
    {
        NameSet child_require;
        for (const auto & item : require_index)
            child_require.insert(child->getStep()->getOutputStream().header.getByPosition(item).name);

        ColumnPruningContext child_column_pruning_context{.name_set = child_require};
        auto new_child = VisitorUtil::accept(child, *this, child_column_pruning_context);
        children_streams.emplace_back(new_child->getStep()->getOutputStream());
        children.emplace_back(new_child);
    }

    auto except_step = std::make_shared<ExceptStep>(std::move(children_streams), std::move(output_stream), step->isDistinct());
    auto except_node = ExceptNode::createPlanNode(context->nextNodeId(), std::move(except_step), children, node.getStatistics());
    return except_node;
}

PlanNodePtr ColumnPruningVisitor::visitIntersectNode(IntersectNode & node, ColumnPruningContext &)
{
    const auto * step = node.getStep().get();

    std::vector<size_t> require_index;

    size_t index = 0;
    DataStream output_stream;
    for (const auto & item : step->getOutputStream().header)
    {
        require_index.emplace_back(index);
        output_stream.header.insert(item);
        index++;
    }

    /// count(*) requires nothing but we need gave some rows.
    if (require_index.empty())
        require_index.emplace_back(0);

    PlanNodes children;
    DataStreams children_streams;
    for (const auto & child : node.getChildren())
    {
        NameSet child_require;
        for (const auto & item : require_index)
            child_require.insert(child->getStep()->getOutputStream().header.getByPosition(item).name);

        ColumnPruningContext child_column_pruning_context{.name_set = child_require};
        auto new_child = VisitorUtil::accept(child, *this, child_column_pruning_context);
        children_streams.emplace_back(new_child->getStep()->getOutputStream());
        children.emplace_back(new_child);
    }

    auto intersect_step = std::make_shared<IntersectStep>(std::move(children_streams), std::move(output_stream), step->isDistinct());
    auto intersect_node = IntersectNode::createPlanNode(context->nextNodeId(), std::move(intersect_step), children, node.getStatistics());
    return intersect_node;
}

PlanNodePtr ColumnPruningVisitor::visitAssignUniqueIdNode(AssignUniqueIdNode & node, ColumnPruningContext & column_pruning_context)
{
    const auto * step = node.getStep().get();
    NameSet & require = column_pruning_context.name_set;

    if (!require.contains(step->getUniqueId()))
    {
        return VisitorUtil::accept(node.getChildren()[0], *this, column_pruning_context);
    }

    return visitDefault<false>(node, column_pruning_context);
}

PlanNodePtr ColumnPruningVisitor::visitExchangeNode(ExchangeNode & node, ColumnPruningContext & column_pruning_context)
{
    const auto * step = node.getStep().get();
    NameSet & require = column_pruning_context.name_set;

    if (require.empty())
    {
        require.insert(step->getOutputStream().header.getByPosition(0).name);
    }

    PlanNodes children;
    DataStreams input_streams;
    for (auto & item : node.getChildren())
    {
        auto child = addProjection(VisitorUtil::accept(item, *this, column_pruning_context), require);
        children.emplace_back(child);
        input_streams.emplace_back(child->getStep()->getOutputStream());
    }

    auto exchange_step = std::make_shared<ExchangeStep>(std::move(input_streams), step->getExchangeMode(), step->getSchema(), step->needKeepOrder());
    return ExchangeNode::createPlanNode(context->nextNodeId(), std::move(exchange_step), children, node.getStatistics());
}

PlanNodePtr ColumnPruningVisitor::visitCTERefNode(CTERefNode & node, ColumnPruningContext & column_pruning_context)
{
    const auto * with_step = node.getStep().get();
    NameSet & require = column_pruning_context.name_set;

    NameSet required;
    for (const auto & item : with_step->getOutputColumns())
        if (require.contains(item.first))
            required.emplace(item.first);

    if (required.empty())
        required.emplace(ExpressionActions::getSmallestColumn(with_step->getOutputStream().header.getNamesAndTypesList()));

    auto & cte_require = cte_require_columns[with_step->getId()];
    for (const auto & item : required)
        cte_require.name_set.emplace(with_step->getOutputColumns().at(item));
    post_order_cte_helper.acceptAndUpdate(with_step->getId(), node.getId(), *this, cte_require);

    NamesAndTypes result_columns;
    std::unordered_map<String, String> output_columns;
    for (const auto & item : with_step->getOutputStream().header.getNamesAndTypes())
        if (required.contains(item.name))
            result_columns.emplace_back(item);
    for (const auto & item : with_step->getOutputColumns())
        if (required.contains(item.first))
            output_columns.emplace(item);

    auto exchange_step = std::make_shared<CTERefStep>(
        DataStream{std::move(result_columns)}, with_step->getId(), std::move(output_columns), with_step->hasFilter());
    return CTERefNode::createPlanNode(context->nextNodeId(), std::move(exchange_step), {}, node.getStatistics());
}

PlanNodePtr ColumnPruningVisitor::visitExplainAnalyzeNode(ExplainAnalyzeNode & node, ColumnPruningContext &)
{
    NameSet require;
    PlanNodePtr child = node.getChildren()[0];
    for (const auto & item : child->getCurrentDataStream().header)
        require.insert(item.name);
    ColumnPruningContext child_column_pruning_context{.name_set = require};
    PlanNodePtr new_child = VisitorUtil::accept(*child, *this, child_column_pruning_context);
    node.replaceChildren({new_child});
    return node.shared_from_this();
}

PlanNodePtr ColumnPruningVisitor::visitIntermediateResultCacheNode(IntermediateResultCacheNode & node, ColumnPruningContext &)
{
    NameSet require;
    PlanNodePtr child = node.getChildren()[0];
    for (const auto & item : child->getCurrentDataStream().header)
        require.insert(item.name);

    ColumnPruningContext column_pruning_context{.name_set = require};
    PlanNodePtr new_child = VisitorUtil::accept(*child, *this, column_pruning_context);
    node.replaceChildren({new_child});
    return node.shared_from_this();
}

PlanNodePtr ColumnPruningVisitor::visitTopNFilteringNode(TopNFilteringNode & node, ColumnPruningContext & column_pruning_context)
{
    auto & step_ptr = node.getStep();
    NameSet & require = column_pruning_context.name_set;

    const auto * step = dynamic_cast<const TopNFilteringStep *>(step_ptr.get());
    for (const auto & item : step->getSortDescription())
    {
        require.insert(item.column_name);
    }
    auto child = VisitorUtil::accept(*node.getChildren()[0], *this, column_pruning_context);
    auto topn_filter_step = std::make_shared<TopNFilteringStep>(
        child->getStep()->getOutputStream(), step->getSortDescription(), step->getSize(), step->getModel(), step->getAlgorithm());
    return TopNFilteringNode::createPlanNode(context->nextNodeId(), std::move(topn_filter_step), PlanNodes{child}, node.getStatistics());
}

PlanNodePtr ColumnPruningVisitor::visitFillingNode(FillingNode & node, ColumnPruningContext & column_pruning_context)
{
    const auto * step = node.getStep().get();
    NameSet & require = column_pruning_context.name_set;

    for (const auto & item : step->getSortDescription())
    {
        require.insert(item.column_name);
    }
    auto child = VisitorUtil::accept(node.getChildren()[0], *this, column_pruning_context);
    auto fill_step = std::make_shared<FillingStep>(child->getStep()->getOutputStream(), step->getSortDescription());
    return FillingNode::createPlanNode(context->nextNodeId(), std::move(fill_step), PlanNodes{child}, node.getStatistics());
}

PlanNodePtr ColumnPruningVisitor::visitTableWriteNode(TableWriteNode & node, ColumnPruningContext &)
{
    const auto * table_write = dynamic_cast<const TableWriteStep *>(node.getStep().get());

    NameSet require;
    for (const auto & item : table_write->getInputStreams()[0].header)
        require.insert(item.name);
    PlanNodePtr child = node.getChildren()[0];
    ColumnPruningContext child_column_pruning_context{.name_set = require};
    PlanNodePtr new_child = VisitorUtil::accept(*child, *this, child_column_pruning_context);
    node.replaceChildren({new_child});
    return node.shared_from_this();
}

PlanNodePtr ColumnPruningVisitor::visitOutfileWriteNode(OutfileWriteNode & node, ColumnPruningContext &)
{
    const auto * outfile_write = dynamic_cast<const OutfileWriteStep *>(node.getStep().get());

    NameSet require;
    for (const auto & item : outfile_write->getInputStreams()[0].header)
        require.insert(item.name);
    PlanNodePtr child = node.getChildren()[0];
    ColumnPruningContext child_column_pruning_context{.name_set = require};
    PlanNodePtr new_child = VisitorUtil::accept(*child, *this, child_column_pruning_context);
    node.replaceChildren({new_child});
    return node.shared_from_this();
}

PlanNodePtr ColumnPruningVisitor::visitTotalsHavingNode(TotalsHavingNode & node, ColumnPruningContext & column_pruning_context)
{
    const auto * step = node.getStep().get();
    NameSet & require = column_pruning_context.name_set;

    for (const auto & col_with_name_type : step->getInputStreams()[0].header)
        if (typeid_cast<const DataTypeAggregateFunction *>(col_with_name_type.type.get()))
            require.emplace(col_with_name_type.name);

    if (const auto & having_filter = step->getHavingFilter())
    {
        auto symbols = SymbolsExtractor::extract(having_filter);
        require.insert(symbols.begin(), symbols.end());
    }

    auto rewritten_child = VisitorUtil::accept(node.getChildren()[0], *this, column_pruning_context);
    node.replaceChildren({rewritten_child});
    return node.shared_from_this();
}

PlanNodePtr ColumnPruningVisitor::visitMergingAggregatedNode(MergingAggregatedNode & node, ColumnPruningContext & column_pruning_context)
{
    const auto * step = node.getStep().get();
    NameSet & require = column_pruning_context.name_set;

    NameSet child_require{step->getKeys().begin(), step->getKeys().end()};

    AggregateDescriptions aggs;
    for (const auto & agg : step->getAggregates())
    {
        if ((agg.argument_names.size() == 1 && require.contains(agg.argument_names[0])) || require.contains(agg.column_name))
        {
            aggs.push_back(agg);
            child_require.insert(agg.argument_names.begin(), agg.argument_names.end());
            child_require.emplace(agg.column_name);
        }
    }

    if (aggs.empty() && step->getKeys().empty())
    {
        return createDummyPlanNode(context).second;
    }

    ColumnPruningContext child_column_pruning_context{.name_set = child_require};
    auto rewritten_child = VisitorUtil::accept(node.getChildren()[0], *this, child_column_pruning_context);
    const auto & rewritten_child_header = rewritten_child->getCurrentDataStream().header;
    ColumnNumbers key_positions;
    for (const auto & key : step->getKeys())
        key_positions.emplace_back(rewritten_child_header.getPositionByName(key));
    const auto & transform_params = step->getAggregatingTransformParams();
    const auto & aggregator_params = transform_params->params;
    Aggregator::Params new_aggregator_params{
        rewritten_child_header, key_positions, std::move(aggs), aggregator_params.overflow_row, aggregator_params.max_threads};
    auto new_transform_params = std::make_shared<AggregatingTransformParams>(new_aggregator_params, transform_params->final);

    auto rewritten_merge_step = std::make_shared<MergingAggregatedStep>(
        rewritten_child->getCurrentDataStream(),
        step->getKeys(),
        step->getGroupingSetsParamsList(),
        step->getGroupings(),
        new_transform_params,
        step->isMemoryEfficientAggregation(),
        step->getMaxThreads(),
        step->getMemoryEfficientMergeThreads());

    PlanNodes children{rewritten_child};
    auto rewritten_merge_node
        = MergingAggregatedNode::createPlanNode(context->nextNodeId(), std::move(rewritten_merge_step), children, node.getStatistics());
    return rewritten_merge_node;
}

PlanNodePtr ColumnPruningVisitor::addProjection(PlanNodePtr node, NameSet & require)
{
    if (!add_projection)
        return node;
    bool need_pruning = false;
    for (auto & col : node->getOutputNames())
    {
        if (!require.contains(col))
        {
            need_pruning = true;
        }
    }

    if (need_pruning && !require.empty())
    {
        Assignments assignments;
        NameToType name_to_type;
        for (auto & col : node->getOutputNamesAndTypes())
        {
            if (require.contains(col.name))
            {
                assignments.emplace(col.name, std::make_shared<ASTIdentifier>(col.name));
                name_to_type[col.name] = col.type;
            }
        }
        // if projection is empty, skip it.
        if (!assignments.empty())
        {
            auto expr_step = std::make_shared<ProjectionStep>(node->getStep()->getOutputStream(), assignments, name_to_type);
            PlanNodes children{node};
            auto expr_node = ProjectionNode::createPlanNode(context->nextNodeId(), std::move(expr_step), children, node->getStatistics());
            return expr_node;
        }
        else
        {
            return node;
        }
    }
    return node;
}

static std::vector<String> makeDistinct(const std::vector<String>& src) {  
    std::unordered_set<String> tmp;  
    std::vector<String> result;  
  
    for (const auto& str : src) {  
        if (tmp.insert(str).second) {  
            result.push_back(str);  
        }  
    }  
  
    return result;  
}

PlanNodePtr ColumnPruningVisitor::convertDistinctToGroupBy(PlanNodePtr node)
{
    auto * distinct_node = dynamic_cast<DistinctNode *>(node.get());
    if (!distinct_node)
        return node;

    const auto & step = *distinct_node->getStep();

    if (step.getLimitHint() == 0 && step.canToAgg())
    {
        NameSet name_set{step.getColumns().begin(), step.getColumns().end()};
        NamesAndTypes arbitrary_names;

        // check decimal type, which is not support for group by columns
        // bool has_decimal_type = false;
        // for (const auto & column : node->getStep()->getOutputStream().header)
        // {
        //     TypeIndex index = column.type->getTypeId();
        //     if (index == TypeIndex::Decimal32 || index == TypeIndex::Decimal64 || index == TypeIndex::Decimal128)
        //     {
        //         has_decimal_type = true;
        //         break;
        //     }
        //     if (!name_set.contains(column.name))
        //         arbitrary_names.emplace_back(column.name, column.type);
        // }
        // if (has_decimal_type)
        // {
        //     return {};
        // }

        AggregateDescriptions descriptions;
        for (auto & name_and_type : arbitrary_names)
        {
            // for rare case, distinct columns don't contain all columns outputs.
            AggregateDescription aggregate_desc;
            aggregate_desc.column_name = name_and_type.name;
            aggregate_desc.argument_names = {name_and_type.name};
            AggregateFunctionProperties properties;
            Array parameters;
            aggregate_desc.function = AggregateFunctionFactory::instance().get("any", {name_and_type.type}, parameters, properties);
            descriptions.emplace_back(aggregate_desc);
        }
        // distinct keys
        auto streams = node->getStep()->getOutputStream();
        auto columns = makeDistinct(step.getColumns());

        auto group_agg_step = std::make_shared<AggregatingStep>(
            streams, columns, NameSet{}, descriptions, GroupingSetsParamsList{}, true);
        auto group_agg_node = PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(group_agg_step), node->getChildren());
        return group_agg_node;
    }

    return node;
}

PlanNodePtr ColumnPruningVisitor::convertFilterWindowToSortingLimit(PlanNodePtr node, NameSet & require)
{
    const auto & filter_step = dynamic_cast<FilterStep &>(*node->getStep());
    auto * window_node = dynamic_cast<WindowNode *>(node->getChildren()[0].get());
    if (!window_node)
        return node;

    const auto & window_step = dynamic_cast<WindowStep &>(*window_node->getStep());
    const auto & window_desc = window_step.getWindow();
    if (window_desc.order_by.empty() || !window_desc.partition_by.empty() || window_desc.window_functions.size() != 1)
        return node;

    String column_name = window_desc.window_functions[0].column_name;
    if (require.contains(column_name))
        return node;

    if (window_desc.frame.begin_type != WindowFrame::BoundaryType::Unbounded
        || (window_desc.frame.end_type != WindowFrame::BoundaryType::Current
            && window_desc.frame.end_type != WindowFrame::BoundaryType::Unbounded))
        return node;

    auto window_func_name = window_desc.window_functions[0].aggregate_function->getName();
    if (window_func_name != "row_number")
        return node;

    const auto & filter = filter_step.getFilter();
    auto symbols = SymbolsExtractor::extractVector(filter);
    if (std::count(symbols.begin(), symbols.end(), column_name) != 1)
        return node;

    PlanNodePtr window_child = window_node->getChildren()[0];
    auto interpreter = ExpressionInterpreter::basicInterpreter(filter_step.getInputStreams()[0].header.getNamesToTypes(), context);

    auto conjuncts = PredicateUtils::extractConjuncts(filter);
    std::vector<ConstASTPtr> new_conjuncts;
    SizeOrVariable limit;

    String func_name;

    auto get_limit_field = [&](const ConstASTPtr & conjunct) -> std::optional<SizeOrVariable> {
        func_name = "";

        const auto * func = conjunct->as<ASTFunction>();
        if (!func || func->arguments->children.size() != 2 || !(func->name == "less" || func->name == "lessOrEquals"))
            return std::nullopt;

        func_name = func->name;

        const auto * column = func->arguments->children[0]->as<ASTIdentifier>();
        if (!column || column_name != column->name())
            return std::nullopt;

        if (const auto * prepared_param = func->arguments->children[1]->as<ASTPreparedParameter>())
            return prepared_param->name;

        auto rhs = interpreter.evaluateConstantExpression(func->arguments->children[1]);
        if (!rhs || !isNativeInteger(rhs->first) || !isUnsignedInteger(rhs->first))
            return std::nullopt;

        auto uint_val = convertFieldToType(rhs->second, DataTypeUInt64());
        return uint_val.safeGet<UInt64>();
    };

    for (const ConstASTPtr & conjunct : conjuncts)
    {
        auto field = get_limit_field(conjunct);
        if (!field.has_value() || func_name.empty())
        {
            new_conjuncts.emplace_back(conjunct);
            continue;
        }

        limit = *field;
    }

    if (new_conjuncts.size() == conjuncts.size())
        return node;

    if (func_name == "less")
    {
        if (std::holds_alternative<size_t>(limit))
            limit = std::get<size_t>(limit) - 1;
        else
            return node;
    }

    auto sorting_step = std::make_shared<SortingStep>(
        window_node->getChildren()[0]->getStep()->getOutputStream(),
        window_desc.order_by,
        limit,
        SortingStep::Stage::FULL,
        SortDescription{});
    auto child_node = SortingNode::createPlanNode(
        context->nextNodeId(), std::move(sorting_step), PlanNodes{window_node->getChildren()}, node->getStatistics());

    UInt64 offset = 0;
    auto limit_step = std::make_shared<LimitStep>(child_node->getStep()->getOutputStream(), limit, offset);
    child_node = LimitNode::createPlanNode(context->nextNodeId(), std::move(limit_step), PlanNodes{child_node}, node->getStatistics());

    if (new_conjuncts.empty())
        return child_node;

    auto new_filter = PredicateUtils::combineConjuncts(new_conjuncts);
    auto new_filter_step = std::make_shared<FilterStep>(child_node->getStep()->getOutputStream(), new_filter);
    auto new_filter_node
        = FilterNode::createPlanNode(context->nextNodeId(), std::move(new_filter_step), {child_node}, node->getStatistics());

    return new_filter_node;
}

String ColumnPruningVisitor::selectColumnWithMinSize(NamesAndTypesList source_columns, StoragePtr storage)
{
    /// You need to read at least one column to find the number of rows.
    /// We will find a column with minimum <compressed_size, type_size, uncompressed_size>.
    /// Because it is the column that is cheapest to read.
    struct ColumnSizeTuple
    {
        size_t compressed_size;
        size_t type_size;
        size_t uncompressed_size;
        String name;

        bool operator<(const ColumnSizeTuple & that) const
        {
            return std::tie(compressed_size, type_size, uncompressed_size)
                < std::tie(that.compressed_size, that.type_size, that.uncompressed_size);
        }
    };

    std::vector<ColumnSizeTuple> columns;
    if (storage)
    {
        auto column_sizes = storage->getColumnSizes();
        for (auto & source_column : source_columns)
        {
            auto c = column_sizes.find(source_column.name);
            if (c == column_sizes.end())
                continue;
            size_t type_size = source_column.type->haveMaximumSizeOfValue() ? source_column.type->getMaximumSizeOfValueInMemory() : 100;
            columns.emplace_back(ColumnSizeTuple{c->second.data_compressed, type_size, c->second.data_uncompressed, source_column.name});
        }
    }

    if (!columns.empty())
        return std::min_element(columns.begin(), columns.end())->name;
    else if (!source_columns.empty())
    {
        if (storage)
        {
            // DO NOT choose Virtuals column, when try get smallest column.
            for (const auto & column : storage->getVirtuals())
            {
                source_columns.remove(column);
            }

            // tmp fix for 40113_lowcard_nullable_subcolumn
            auto metadata_snapshot = storage->getInMemoryMetadataPtr();
            const auto & columns_desc = metadata_snapshot->getColumns();
            source_columns.erase(
                std::remove_if(
                    source_columns.begin(),
                    source_columns.end(),
                    [&](const auto & type_and_name) {
                        auto column_opt = columns_desc.tryGetColumnOrSubcolumn(GetColumnsOptions::Ordinary, type_and_name.name);
                        return column_opt && column_opt->isSubcolumn()
                            && !!(dynamic_cast<const DataTypeLowCardinality *>(column_opt->getTypeInStorage().get()));
                    }),
                source_columns.end());
        }
        /// If we have no information about columns sizes, choose a column of minimum size of its data type.
        return ExpressionActions::getSmallestColumn(source_columns);
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "unexpected branch of selectColumnWithMinSize");
        __builtin_unreachable();
    }
}

}
