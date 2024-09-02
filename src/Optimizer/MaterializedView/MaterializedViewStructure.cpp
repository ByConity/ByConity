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

#include <Optimizer/MaterializedView/MaterializedViewStructure.h>

#include <Analyzers/QueryAnalyzer.h>
#include <Analyzers/QueryRewriter.h>
#include <Interpreters/Context.h>
#include <Optimizer/Iterative/IterativeRewriter.h>
#include <Optimizer/MaterializedView/InnerJoinCollector.h>
#include <Optimizer/MaterializedView/MaterializedViewChecker.h>
#include <Optimizer/PlanOptimizer.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/Rewriter/PredicatePushdown.h>
#include <Optimizer/Rewriter/UnifyJoinOutputs.h>
#include <Optimizer/Rewriter/UnifyNullableType.h>
#include <Optimizer/Rule/Rules.h>
#include <Optimizer/Utils.h>
#include <Parsers/ASTIdentifier.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/QueryPlanner.h>
#include <QueryPlan/SymbolMapper.h>
#include <Common/Exception.h>
#include <Core/Field.h>

#include <unordered_map>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW;
}

MaterializedViewStructurePtr MaterializedViewStructure::buildFrom(
    const StorageID & view_storage_id, const StorageID & target_storage_id, ASTPtr query, bool async_materialized_view_, ContextPtr context)
{
    ContextMutablePtr query_context = Context::createCopy(context);
    query_context->createSymbolAllocator();
    query_context->createPlanNodeIdAllocator();
    query_context->setQueryContext(query_context);
    query_context->setSetting("prefer_global_in_and_join", true); // for dialect_type='CLICKHOUSE'
    query_context->setSetting("cte_mode", Field{"INLINED"}); // support with clause

    auto query_ptr = QueryRewriter().rewrite(query, query_context, false);
    AnalysisPtr analysis = QueryAnalyzer::analyze(query_ptr, query_context);

    if (!analysis->non_deterministic_functions.empty())
        throw Exception(ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW, "materialized view query contains non deterministic functions");

    QueryPlanPtr query_plan = QueryPlanner().plan(query_ptr, *analysis, query_context);

    auto wrap_rewriter_name = [&](const String & name) -> String {
        return "MV_" + name + "_" + view_storage_id.getDatabaseName() + "." + view_storage_id.getTableName();
    };

    static Rewriters rewriters
        = {std::make_shared<PredicatePushdown>(),
           std::make_shared<IterativeRewriter>(Rules::simplifyExpressionRules(), wrap_rewriter_name("SimplifyExpression")),
           std::make_shared<IterativeRewriter>(Rules::removeRedundantRules(), wrap_rewriter_name("RemoveRedundant")),
           std::make_shared<IterativeRewriter>(Rules::inlineProjectionRules(), wrap_rewriter_name("InlineProjection")),
           std::make_shared<IterativeRewriter>(Rules::normalizeExpressionRules(), wrap_rewriter_name("NormalizeExpression")),
           std::make_shared<IterativeRewriter>(Rules::swapPredicateRules(), wrap_rewriter_name("SwapPredicate")),
           std::make_shared<UnifyJoinOutputs>(),
           std::make_shared<UnifyNullableType>()};

    for (auto & rewriter : rewriters)
        rewriter->rewritePlan(*query_plan, query_context);

    GraphvizPrinter::printLogicalPlan(*query_plan, query_context, "MaterializedViewPlan_" + std::to_string(query_context->nextNodeId()));
    return buildFrom(view_storage_id, target_storage_id, query_plan->getPlanNode(), async_materialized_view_, query_context);
}

MaterializedViewStructurePtr MaterializedViewStructure::buildFrom(
    const StorageID & view_storage_id, const StorageID & target_storage_id, PlanNodePtr query, bool async_materialized_view_, ContextPtr context)
{
    PlanNodePtr root = query;
    if (root->getType() != IQueryPlanStep::Type::Projection)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "materialized view sql has no output plan node");

    auto & output_step = dynamic_cast<ProjectionStep &>(*root->getStep().get());
    if (!output_step.isFinalProject())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "materialized view sql has no output plan node");

    std::unordered_map<String, String> output_columns_to_query_columns_map;
    for (const auto & item : output_step.getAssignments())
    {
        const auto * identifier = item.second->as<ASTIdentifier>();
        if (!identifier)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "materialized view sql output plan node contains expression");
        output_columns_to_query_columns_map[item.first] = identifier->name();
    }

    query = root->getChildren()[0];

    MaterializedViewPlanChecker checker{true};
    checker.check(*query, context);

    auto top_aggregate_node = checker.getTopAggregateNode();
    bool has_having_filter = checker.hasHavingFilter();

    auto target_table = DatabaseCatalog::instance().tryGetTable(target_storage_id, context);
    if (!target_table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "materialized view target table not found");

    auto symbol_map = SymbolTransformMap::buildFrom(*query);
    if (!symbol_map)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "materialized view query plan node contains duplicate symbol");

    InnerJoinCollector inner_join_collector;
    inner_join_collector.collect(query);

    std::unordered_set<IQueryPlanStep::Type> skip_nodes;
    skip_nodes.emplace(IQueryPlanStep::Type::Aggregating);
    skip_nodes.emplace(IQueryPlanStep::Type::Sorting);
    JoinHyperGraph join_hyper_graph = JoinHyperGraph::build(query, *symbol_map, context, skip_nodes);

    // enable enable_materialized_view_join_rewriting, optimizer rewrite query use mview contains join.
    if (join_hyper_graph.getPlanNodes().size() > 1 && !context->getSettingsRef().enable_materialized_view_join_rewriting)
        throw Exception(
            ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW,
            "set enable_materialized_view_join_rewriting=1 to support materialized view with join");

    std::unordered_set<StorageID> base_tables;
    for (const auto & node : join_hyper_graph.getPlanNodes())
    {
        if (node->getStep()->getType() != IQueryPlanStep::Type::TableScan)
            throw Exception(
                ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW,
                "query is not supported in materialized view: " + node->getStep()->getName());
        auto storage_id = dynamic_cast<TableScanStep *>(node->getStep().get())->getStorageID();
        base_tables.emplace(storage_id);
    }

    ExpressionEquivalences expression_equivalences;
    auto inner_sources_node_set = join_hyper_graph.getNodeSet(inner_join_collector.getInnerSources());
    for (const auto & join_clause : join_hyper_graph.getJoinConditions())
    {
        if ((inner_sources_node_set & join_clause.first) == join_clause.first)
        {
            auto predicates = PredicateUtils::extractEqualPredicates(join_clause.second);
            for (const auto & predicate : predicates.first)
            {
                auto left_symbol_lineage = symbol_map->inlineReferences(predicate.first);
                auto right_symbol_lineage = symbol_map->inlineReferences(predicate.second);
                expression_equivalences.add(left_symbol_lineage, right_symbol_lineage);
            }
        }
    }

    // materialized view output may not be inconsistent with table (if create materialized view with optimizer disabled)
    std::unordered_map<String, String> output_columns_to_table_columns_map;
    std::unordered_set<String> output_columns;

    auto table_columns = target_table->getInMemoryMetadataPtr()->getColumns().getAllPhysical();
    // clickhouse supports materialized view has more columns than query
    if (table_columns.size() < root->getCurrentDataStream().header.columns())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "size of materialized view physical columns is less than than select outputs for " + target_storage_id.getFullTableName());

    size_t index = 0;
    for (auto & table_column : table_columns)
    {
        if (index >= root->getCurrentDataStream().header.columns())
            break;
        const auto & query_column = root->getCurrentDataStream().header.getByPosition(index++);
        if (!removeNullable(removeLowCardinality(query_column.type))->equals(*removeNullable(removeLowCardinality(table_column.type))))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "materialized view query output type is inconsistent with target table columns type: {} (type: {}) in query, "
                "{}(type: {}) in {}",
                query_column.name,
                query_column.type->getName(),
                table_column.name,
                table_column.type->getName(),
                target_storage_id.getFullTableName());

        auto query_column_name = output_columns_to_query_columns_map.at(query_column.name);
        output_columns.emplace(query_column_name);
        output_columns_to_table_columns_map.emplace(query_column_name, table_column.name);
    }

    std::shared_ptr<const AggregatingStep> aggregating_step = top_aggregate_node
        ? dynamic_pointer_cast<const AggregatingStep>(top_aggregate_node->getStep())
        : std::shared_ptr<const AggregatingStep>{};

    return std::make_shared<MaterializedViewStructure>(
        view_storage_id,
        target_storage_id,
        std::move(target_table),
        std::move(base_tables),
        std::move(join_hyper_graph),
        inner_join_collector.getInnerSources(),
        inner_join_collector.getOuterSources(),
        std::move(aggregating_step),
        has_having_filter,
        std::move(*symbol_map),
        std::move(output_columns),
        std::move(output_columns_to_table_columns_map),
        std::move(output_columns_to_query_columns_map),
        std::move(expression_equivalences),
        async_materialized_view_);
}
}
