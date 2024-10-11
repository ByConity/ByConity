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

#include <Optimizer/Rewriter/MaterializedViewRewriter.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Analyzers/TypeAnalyzer.h>
#include <Catalog/Catalog.h>
#include <Core/Field.h>
#include <Core/SettingsEnums.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/StorageID.h>
#include <Optimizer/CardinalityEstimate/TableScanEstimator.h>
#include <Optimizer/DomainTranslator.h>
#include <Optimizer/JoinGraph.h>
#include <Optimizer/MaterializedView/ExpressionSubstitution.h>
#include <Optimizer/MaterializedView/InnerJoinCollector.h>
#include <Optimizer/MaterializedView/MaterializedViewChecker.h>
#include <Optimizer/MaterializedView/MaterializedViewJoinHyperGraph.h>
#include <Optimizer/MaterializedView/MaterializedViewMemoryCache.h>
#include <Optimizer/MaterializedView/MaterializedViewStructure.h>
#include <Optimizer/MaterializedView/PartitionConsistencyChecker.h>
#include <Optimizer/MaterializedView/RelatedMaterializedViewsExtractor.h>
#include <Optimizer/OptimizerMetrics.h>
#include <Optimizer/PredicateConst.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/Rewriter/PredicatePushdown.h>
#include <Optimizer/SelectQueryInfoHelper.h>
#include <Optimizer/SimpleExpressionRewriter.h>
#include <Optimizer/SymbolsExtractor.h>
#include <Optimizer/Utils.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTableColumnReference.h>
#include <Parsers/IAST.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPlan/Assignment.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/CTEVisitHelper.h>
#include <QueryPlan/FilterStep.h>
#include <QueryPlan/GraphvizPrinter.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanSymbolReallocator.h>
#include <QueryPlan/ProjectionStep.h>
#include <QueryPlan/SimplePlanRewriter.h>
#include <QueryPlan/SimplePlanVisitor.h>
#include <QueryPlan/SymbolAllocator.h>
#include <QueryPlan/SymbolMapper.h>
#include <QueryPlan/TableScanStep.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/StorageDistributed.h>
#include <Common/Exception.h>
#include <Common/LinkedHashMap.h>
#include <common/logger_useful.h>

#include <algorithm>
#include <map>
#include <memory>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <fmt/core.h>
#include <fmt/format.h>

namespace DB
{
namespace ErrorCodes
{
extern const int PARAMETER_OUT_OF_BOUND;
extern const int NO_AVAILABLE_MATERIALIZED_VIEW;
}

using namespace MaterializedView;
namespace
{
struct RewriterCandidate
{
    StorageID view_database_and_table_name;
    StorageID target_database_and_table_name;
    ASTPtr prewhere_expr;
    std::optional<PlanNodeStatisticsPtr> target_table_estimated_stats;
    bool contains_ordered_columns;
    NamesWithAliases table_output_columns;
    Assignments assignments;
    NameToType name_to_type;
    ASTPtr compensation_predicate;
    bool need_rollup = false;
    std::vector<ASTPtr> rollup_keys;
    ASTPtr union_predicate;
};
using RewriterCandidates = std::vector<RewriterCandidate>;

struct RewriterCandidateSort
{
    bool operator()(const RewriterCandidate & lhs, const RewriterCandidate & rhs)
    {
        if (!lhs.target_table_estimated_stats && !rhs.target_table_estimated_stats)
            return lhs.contains_ordered_columns && !rhs.contains_ordered_columns;
        else if (!lhs.target_table_estimated_stats)
            return false;
        else if (!rhs.target_table_estimated_stats)
            return true;
        else
        {
            if (lhs.target_table_estimated_stats.value()->getRowCount() != rhs.target_table_estimated_stats.value()->getRowCount())
            {
                return lhs.target_table_estimated_stats.value()->getRowCount()
                       < rhs.target_table_estimated_stats.value()->getRowCount();
            }
            else
            {
                return lhs.contains_ordered_columns && !rhs.contains_ordered_columns;
            }
        }
    }
};

struct RewriterFailureMessage
{
    StorageID storage;
    String message;
};
using RewriterFailureMessages = std::vector<RewriterFailureMessage>;

using TableMapping = std::unordered_map<size_t, size_t>;
using TableInputRefs = std::vector<TableInputRef>;
using TableInputRefMapping = std::unordered_map<TableInputRef, TableInputRef, TableInputRefHash, TableInputRefEqual>;
using TableInputRefMultiMapping = std::unordered_map<TableInputRef, TableInputRefs, TableInputRefHash, TableInputRefEqual>;
struct JoinSetMatchResult
{
    TableInputRefMultiMapping query_to_view_table_multi_mappings;
    // std::vector<TableInputRef> view_missing_tables;
    // std::unordered_map<String, std::shared_ptr<ASTTableColumnReference>> view_missing_columns;
};

struct PredicateInfo
{
    std::vector<std::pair<ConstASTPtr, ConstASTPtr>> equal_predicates;
    std::vector<ConstASTPtr> inner_predicates;

    std::unordered_map<JoinHyperGraph::NodeSet, std::vector<ConstASTPtr>> outer_predicates;
    std::unordered_map<JoinHyperGraph::NodeSet, std::vector<ConstASTPtr>> outer_join_clauses;

    String toString() const
    {
        auto combine_equal_predicates = [&]() {
            std::vector<ASTPtr> res;
            res.reserve(equal_predicates.size());
            for (const auto & item : equal_predicates)
                res.emplace_back(makeASTFunction("equals", item.first->clone(), item.second->clone()));
            return res;
        };

        auto combine_other_predicates = [&](const auto & predicates) {
            std::vector<ConstASTPtr> res;
            for (const auto & item : predicates)
                res.insert(res.end(), item.second.begin(), item.second.end());
            return res;
        };

        std::stringstream ss;
        ss << "equal_predicates: " << queryToString(PredicateUtils::combineConjuncts(combine_equal_predicates()))
           << ", inner_predicates: " << queryToString(PredicateUtils::combineConjuncts(inner_predicates))
           << ", outer_predicates: " << queryToString(PredicateUtils::combineConjuncts(combine_other_predicates(outer_predicates)))
           << ", outer_join_clauses: " << queryToString(PredicateUtils::combineConjuncts(combine_other_predicates(outer_join_clauses)));
        return ss.str();
    }
};

using ExpressionEquivalences = Equivalences<ConstASTPtr, EqualityASTMap>;

/**
 * tables that children contains, empty if there are unsupported plan nodes
 */
struct BaseTables
{
    std::unordered_set<StorageID> base_tables;
    PlanNodePtr inlined;
};

/**
 * Top-down match whether any node in query can be rewrite using materialized view.
 */
class CandidatesExplorer : public PlanNodeVisitor<BaseTables, std::unordered_set<String>>
{
public:
    static std::unordered_map<PlanNodePtr, RewriterCandidates> explore(
        QueryPlan & query,
        ContextMutablePtr context,
        LinkedHashMap<MaterializedViewStructurePtr, PartitionCheckResult> & materialized_views,
        bool verbose)
    {
        CandidatesExplorer explorer{query.getCTEInfo(), context, materialized_views, verbose};
        std::unordered_set<String> required_columns;
        VisitorUtil::accept(query.getPlanNode(), explorer, required_columns);

        if (verbose)
        {
            static auto log = getLogger("CandidatesExplorer");
            for (auto & item : explorer.failure_messages)
                for (auto & message : item.second)
                    LOG_DEBUG(
                        log,
                        "rewrite fail: plan node id: " + std::to_string(item.first->getId()) + ", type: " + item.first->getStep()->getName()
                        + ", mview: " + message.storage.getFullTableName() + ", cause: " + message.message);
            for (auto & item : explorer.candidates)
                for (auto & candidate : item.second)
                    LOG_DEBUG(
                        log,
                        "rewrite success: plan node id: " + std::to_string(item.first->getId())
                        + ", type: " + item.first->getStep()->getName() + ", mview: " +
                        candidate.target_database_and_table_name.getDatabaseName() + "." +
                        candidate.target_database_and_table_name.getTableName());
        }

        return std::move(explorer.candidates);
    }

protected:
    CandidatesExplorer(
        CTEInfo & cte_info,
        ContextMutablePtr context_,
        LinkedHashMap<MaterializedViewStructurePtr, PartitionCheckResult> & materialized_views_,
        bool verbose_)
        : context(context_), materialized_views(materialized_views_), verbose(verbose_), cte_helper(cte_info)
    {
    }

    BaseTables visitPlanNode(PlanNodeBase & node, std::unordered_set<String> & required_columns) override
    {
        return process(node, required_columns);
    }

    BaseTables visitTableScanNode(TableScanNode & node, std::unordered_set<String> &) override
    {
        auto & step = node.getStep();
        std::unordered_set<StorageID> base_tables;
        base_tables.emplace(step->getStorageID());
        return BaseTables{
            .base_tables = base_tables,
            .inlined = node.shared_from_this(),
        };
    }

    BaseTables visitCTERefNode(CTERefNode & node, std::unordered_set<String> & required_columns) override
    {
        auto cte_id = node.getStep()->getId();
        auto res = cte_helper.accept(cte_id, *this, required_columns);

        return BaseTables{.base_tables = res.base_tables, .inlined = node.getStep()->toInlinedPlanNode(cte_helper.getCTEInfo(), context)};
    }

    BaseTables process(PlanNodeBase & node, const std::unordered_set<String> & required_columns)
    {
        bool supported = MaterializedViewStepChecker::isSupported(node.getStep(), context);

        std::unordered_set<String> child_required_columns;
        if (node.getType() == IQueryPlanStep::Type::Projection)
        {
            auto & step = dynamic_cast<ProjectionStep &>(*node.getStep().get());
            for (const auto & assignment : step.getAssignments())
            {
                for (const auto & symbol : SymbolsExtractor::extract(assignment.second))
                    child_required_columns.emplace(symbol);
            }
        }

        std::unordered_set<StorageID> base_tables;
        PlanNodes children;
        for (auto & child : node.getChildren())
        {
            auto res = VisitorUtil::accept(child, *this, child_required_columns);
            if (res.base_tables.empty())
                supported = false;
            else
                base_tables.insert(res.base_tables.begin(), res.base_tables.end());
            children.emplace_back(res.inlined);
        }
        if (!supported)
            return {};

        auto inlined = PlanNodeBase::createPlanNode(node.getId(), node.getStep(), children);
        matches(inlined, node.shared_from_this(), base_tables, required_columns);
        return BaseTables{.base_tables = base_tables, .inlined = inlined};
    }

    /**
     * matches plan node using all related materialized view.
     */
    void matches(PlanNodePtr query, PlanNodePtr origin, const std::unordered_set<StorageID> & tables, const std::unordered_set<String> & query_required_columns)
    {
        if (tables.size() > JoinHyperGraph::MAX_NODE)
            return;

        if (tables.size() > 1 && !context->getSettingsRef().enable_materialized_view_join_rewriting)
            return;

        std::vector<MaterializedViewStructurePtr> related_materialized_views;
        std::shared_ptr<const AggregatingStep> query_aggregate = extractTopAggregate(query);

        for (const auto & materialized_view : materialized_views)
        {
            if (materialized_view.first->top_aggregating_step.get() && !query_aggregate)
                continue;
            if (tables == materialized_view.first->base_storage_ids)
                related_materialized_views.emplace_back(materialized_view.first);
        }

        if (related_materialized_views.empty())
            return;

        std::optional<SymbolTransformMap> query_map = SymbolTransformMap::buildFrom(*query);
        if (!query_map)
        {
            LOG_ERROR(logger, "skip plan node {}: duplicate symbol name.", query->getId(), ErrorCodes::LOGICAL_ERROR);
            return;
        }

        std::unordered_set<IQueryPlanStep::Type> skip_nodes;
        skip_nodes.emplace(IQueryPlanStep::Type::Aggregating);
        skip_nodes.emplace(IQueryPlanStep::Type::Sorting);
        JoinHyperGraph query_join_hyper_graph = JoinHyperGraph::build(query, *query_map, context, skip_nodes);
        InnerJoinCollector inner_join_collector;
        inner_join_collector.collect(query);
        PlanNodes query_inner_sources = inner_join_collector.getInnerSources();
        // get all predicates from join graph
        auto query_predicates = extractPredicates(query_join_hyper_graph, query_join_hyper_graph.getNodeSet(query_inner_sources));

        for (const auto & view : related_materialized_views)
            if (auto result = match(
                    query,
                    query_required_columns,
                    query_join_hyper_graph,
                    query_predicates,
                    query_inner_sources,
                    query_map,
                    query_aggregate,
                    view->join_hyper_graph,
                    view->inner_sources,
                    view->having_predicates,
                    view->symbol_map,
                    view->output_columns,
                    view->output_columns_to_table_columns_map,
                    view->output_columns_to_query_columns_map,
                    view->top_aggregating_step,
                    view->async_materialized_view,
                    materialized_views[view],
                    view->view_storage_id,
                    view->target_storage_id))
                candidates[origin].emplace_back(*result);
    }


    /**
     * main logic for materialized view based query rewriter.
     */
    std::optional<RewriterCandidate> match(
        const PlanNodePtr & query,
        const std::unordered_set<String> & query_required_columns,
        const JoinHyperGraph & query_join_hyper_graph,
        const PredicateInfo & query_predicates,
        const PlanNodes & query_inner_sources,
        std::optional<SymbolTransformMap> & query_map,
        const std::shared_ptr<const AggregatingStep> & query_aggregate,
        const JoinHyperGraph & view_join_hyper_graph,
        const PlanNodes & view_inner_sources,
        const bool & view_has_having_predicates,
        const SymbolTransformMap & view_map,
        const std::unordered_set<String> & view_outputs,
        const std::unordered_map<String, String> & view_outputs_to_table_columns_map,
        std::unordered_map<String, String> output_columns_to_query_columns_map,
        const std::shared_ptr<const AggregatingStep> & view_aggregate,
        const bool & async_materialized_view,
        const PartitionCheckResult & partition_check_result,
        const StorageID view_storage_id,
        const StorageID target_storage_id)
    {
        auto add_failure_message = [&](const String & message) {
            if (verbose)
                failure_messages[query].emplace_back(
                    RewriterFailureMessage{view_storage_id, message});
        };

        // 0. pre-check
        if (view_aggregate.get() && !query_aggregate)
            return {};

        // 1. check join nodes
        auto join_graph_match_result = matchJoinSet(query_join_hyper_graph, view_join_hyper_graph, context);
        if (!join_graph_match_result)
        {
            add_failure_message("join graph rewrite fail.");
            return {};
        }

        // 1-2. generate table mapping from join graph
        /// If a table is used multiple times, we will create multiple mappings,
        /// and we will try to rewrite the query using each of the mappings.
        /// Then, we will try to map every source table (query) to a target table (view).
        for (auto & query_to_view_table_mappings : generateFlatTableMappings(join_graph_match_result->query_to_view_table_multi_mappings))
        {
            // 1-3. check join edges
            TableMapping query_to_view_table_node_set_index_mappings;
            for (const auto & item : query_to_view_table_mappings)
                query_to_view_table_node_set_index_mappings.emplace(
                    query_join_hyper_graph.getNodeSetIndex(item.first.unique_id),
                    view_join_hyper_graph.getNodeSetIndex(item.second.unique_id));

            auto join_relation_match_result = matchJoinRelation(
                    query_join_hyper_graph,
                    view_join_hyper_graph,
                    view_inner_sources,
                    query_to_view_table_node_set_index_mappings,
                    *join_graph_match_result);
            if (!join_relation_match_result)
            {
                add_failure_message(
                    "join relation match fail. query: " + query_join_hyper_graph.toString() + ";view: " + view_join_hyper_graph.toString());
                break; // bail out
            }

            // 2. check where(predicates)
            ASTPtr union_predicate = PredicateConst::FALSE_VALUE;
            std::vector<ConstASTPtr> compensation_predicates;

            // 2-0. prepare predicates
            auto view_predicates = extractPredicates(
                view_join_hyper_graph,
                mapQueryNodeSet(query_join_hyper_graph.getNodeSet(query_inner_sources), query_to_view_table_node_set_index_mappings));
            if (verbose)
            {
                LOG_DEBUG(logger, "query_predicates: {}; view_predicates: {}", query_predicates.toString(), view_predicates.toString());
            }

            // can not get equivalences from materialized view structure as join compensation
            ExpressionEquivalences view_equivalences;
            for (const auto & predicate : view_predicates.equal_predicates)
            {
                auto left_symbol_lineage = normalizeExpression(predicate.first, view_map);
                auto right_symbol_lineage = normalizeExpression(predicate.second, view_map);
                view_equivalences.add(left_symbol_lineage, right_symbol_lineage);
            }

            // 2-1. equal-predicates
            /// Rewrite query equal predicates to view-based and build new equivalences map. Then check whether
            /// these query equal-predicates exists in view equivalences, otherwise, construct missing predicate for view.
            ExpressionEquivalences view_based_query_equivalences_map;
            for (const auto & predicate : query_predicates.equal_predicates)
            {
                auto left_symbol_lineage = normalizeExpression(predicate.first, *query_map, query_to_view_table_mappings);
                auto right_symbol_lineage = normalizeExpression(predicate.second, *query_map, query_to_view_table_mappings);
                view_based_query_equivalences_map.add(left_symbol_lineage, right_symbol_lineage);

                if (!view_equivalences.isEqual(left_symbol_lineage, right_symbol_lineage))
                    compensation_predicates.emplace_back(makeASTFunction("equals", left_symbol_lineage, right_symbol_lineage));
            }

            // check whether view equal-predicates exists in query equivalences, otherwise, give up.
            bool has_missing_predicate = false;
            for (const auto & predicate : view_predicates.equal_predicates)
            {
                auto left_symbol_lineage = normalizeExpression(predicate.first, view_map);
                auto right_symbol_lineage = normalizeExpression(predicate.second, view_map);
                if (!view_based_query_equivalences_map.isEqual(left_symbol_lineage, right_symbol_lineage))
                {
                    add_failure_message("equal-predicates rewrite fail.");
                    has_missing_predicate = true;
                    break; // bail out
                }
            }
            if (has_missing_predicate)
                continue; // bail out

            // 2-2. rewrite outer join predicates
            auto check_outer_predicates = [&](auto & query_outer_predicates, auto & view_outer_predicates) {
                if (query_outer_predicates.size() != view_outer_predicates.size())
                {
                    add_failure_message("rewrite outer predciates rewrite fail: different size.");
                    return false;
                }
                for (const auto & item : query_outer_predicates)
                {
                    auto normalized_query_outer_predicates = normalizeExpression(
                        PredicateUtils::combineConjuncts(item.second),
                        *query_map,
                        query_to_view_table_mappings,
                        view_based_query_equivalences_map);
                    auto normalized_view_outer_predicates = normalizeExpression(
                        PredicateUtils::combineConjuncts(
                            view_outer_predicates[mapQueryNodeSet(item.first, query_to_view_table_node_set_index_mappings)]),
                        view_map,
                        view_based_query_equivalences_map);
                    if (!ASTEquality::ASTEquals()(normalized_view_outer_predicates, normalized_query_outer_predicates))
                    {
                        add_failure_message("rewrite outer predicates rewrite fail.");
                        return false;
                    }
                }
                return true;
            };
            if (!check_outer_predicates(query_predicates.outer_predicates, view_predicates.outer_predicates))
                continue;
            if (!check_outer_predicates(query_predicates.outer_join_clauses, view_predicates.outer_join_clauses))
                continue;

            // 2-3. rewrite partition-predicates
            auto mv_partition_prediate = normalizeExpression(
                SymbolMapper::simpleMapper(output_columns_to_query_columns_map).map(partition_check_result.mview_partition_predicate),
                *query_map,
                query_to_view_table_mappings);
            compensation_predicates.emplace_back(mv_partition_prediate);

            if (!PredicateUtils::isFalsePredicate(partition_check_result.union_query_partition_predicate))
            {
                auto it = std::find_if(query_to_view_table_mappings.begin(), query_to_view_table_mappings.end(), [&](const auto & item) {
                    return item.first.storage == partition_check_result.depend_storage;
                });
                if (it == query_to_view_table_mappings.end() && query_to_view_table_mappings.size() == 1)
                    it = query_to_view_table_mappings.begin();
                if (it == query_to_view_table_mappings.end())
                {
                    add_failure_message("union query partition predicate rewrite fail.");
                    continue; // bail out
                }
                union_predicate = IdentifierToColumnReference::rewrite(
                    partition_check_result.depend_storage.get(),
                    it->first.unique_id,
                    partition_check_result.union_query_partition_predicate);
            }

            // 2-4. range-predicates
            using namespace DB::Predicate;
            DomainTranslator<ASTPtr> domain_translator(context);
            auto query_extraction_result = domain_translator.getExtractionResult(
                normalizeExpression(
                    PredicateUtils::combineConjuncts(query_predicates.inner_predicates),
                    *query_map,
                    query_to_view_table_mappings,
                    view_based_query_equivalences_map),
                NameToType{});
            auto view_extraction_result = domain_translator.getExtractionResult(
                normalizeExpression(PredicateUtils::combineConjuncts(view_predicates.inner_predicates), view_map, view_based_query_equivalences_map),
                NameToType{});

            TupleDomain<ASTPtr> & view_based_query_domain = query_extraction_result.tuple_domain;
            TupleDomain<ASTPtr> & view_domain = view_extraction_result.tuple_domain;

            ASTPtr view_based_query_other_predicate = query_extraction_result.remaining_expression;
            ASTPtr view_other_predicate = view_extraction_result.remaining_expression;

            if (view_based_query_domain.isNone() || view_domain.isNone())
            {
                add_failure_message("tuple domain is none");
                continue; // bail out
            }

            // 2-5. union rewrite for range predicate
            /// view don't contains all datas query need, it still can be used for query rewrite.
            /// we can get some data view and get remaining data from query.
            /// eg:
            /// ┌────────┐
            /// │ query  │
            /// │  ┌─────┼─┐ (intersect)
            /// └──┼─────┘ │
            ///    │ view  │
            ///    └───────┘
            ///
            /// ┌────────┐
            /// │ query  │   (union_predicate)
            /// │ ┌──────┘
            /// └─┘
            ///  union all
            ///   ┌──────┐
            ///   └──────┘   (intersect of view_based_query_domain and view)
            ///
            /// we only check range filter for union rewrite as only range filter can be used for io pruning.
            if (!view_domain.contains(view_based_query_domain))
            {
                if (!context->getSettingsRef().enable_materialized_view_union_rewriting)
                {
                    add_failure_message("union rewrite is disabled");
                    continue; // bail out
                }

                if (!PredicateUtils::isFalsePredicate(partition_check_result.union_query_partition_predicate))
                {
                    add_failure_message("union predicate is not false");
                    continue; // bail out
                }

                auto union_domain = view_based_query_domain.subtract(view_domain);
                if (!union_domain)
                {
                    add_failure_message("union_predicate is dnf");
                    continue; // bail out
                }
                auto view_based_union_predicate = domain_translator.toPredicate(*union_domain);
                TableInputRefMap view_to_query_table_mappings;
                for (const auto & item : query_to_view_table_mappings)
                    view_to_query_table_mappings.emplace(item.second, item.first);
                union_predicate = normalizeExpression(view_based_union_predicate, view_to_query_table_mappings);

                view_based_query_domain = view_based_query_domain.intersect(view_domain);
                if (view_based_query_domain.isNone())
                {
                    add_failure_message("no intersect between view and query");
                    continue; // bail out
                }
            }

            // calculate range predicate need to be apply to view outputs
            if (view_domain != view_based_query_domain)
            {
                ASTPtr compensation_predicate = domain_translator.toPredicate(view_based_query_domain);
                compensation_predicates.emplace_back(compensation_predicate);
            }

            // 2-6. other-predicates
            auto other_compensation_predicate = PredicateUtils::splitPredicates(
                view_based_query_other_predicate, normalizeExpression(view_other_predicate, view_map, view_based_query_equivalences_map));
            if (!other_compensation_predicate)
            {
                add_failure_message("other-predicates rewrite fail.");
                continue; // bail out
            }
            compensation_predicates.emplace_back(other_compensation_predicate);

            // 2-7. join compensation predicates
            compensation_predicates.emplace_back(normalizeExpression(
                PredicateUtils::combineConjuncts(*join_relation_match_result), view_map, view_based_query_equivalences_map));

            // 2-6. finish predicates rewrite
            auto compensation_predicate = PredicateUtils::combineConjuncts(compensation_predicates);
            if (PredicateUtils::isFalsePredicate(compensation_predicate))
            {
                add_failure_message("compensation predicates rewrite fail.");
                continue; // bail out
            }

            // 3. check need rollup
            // Note: aggregate always need rollup for aggregating merge tree in clickhouse,
            bool need_rollup = query_aggregate
                && (!async_materialized_view || context->getSettingsRef().enforce_materialized_view_union_rewriting || !view_aggregate
                    || query_aggregate->getKeys().size() < view_aggregate->getKeys().size()
                    || !PredicateUtils::isFalsePredicate(union_predicate));

            // 3-1. query aggregate has default result if group by has empty set. not supported yet.
            bool empty_groupings = query_aggregate && query_aggregate->getKeys().empty() &&
                                   view_aggregate && !view_aggregate->getKeys().empty();
            if (empty_groupings && !context->getSettingsRef().enable_materialized_view_empty_grouping_rewriting) {
                continue;
            }
            auto default_value_provider = getAggregateDefaultValueProvider(query_aggregate, context);

            // 3-2. having can't rewrite if aggregate need rollup.
            if (need_rollup && view_has_having_predicates)
            {
                add_failure_message("having predicate rollup rewrite fail.");
                continue;
            }

            // 5. check output columns
            // 5-1. build lineage expression to output columns map using view-based query equivalences map.
            EqualityASTMap<ConstASTPtr> view_output_columns_map;
            for (const auto & view_output : view_outputs)
                view_output_columns_map.emplace(
                    normalizeExpression(makeASTIdentifier(view_output), view_map, view_based_query_equivalences_map),
                    makeASTIdentifier(view_output));

            // 5-2. update symbol transform map if there are view missing tables.
            // for (const auto & missing_table_column : join_graph_match_result->view_missing_columns)
            //     view_output_columns_map.emplace(
            //         normalizeExpression(
            //             missing_table_column.second, *query_map, query_to_view_table_mappings, view_based_query_equivalences_map),
            //         makeASTIdentifier(missing_table_column.first));

            // 5-3. check select columns (contains aggregates)
            NameOrderedSet required_columns_set;
            Assignments assignments;
            NameToType name_to_type;
            bool enforce_agg_node = false;
            for (const auto & name_and_type : query->getCurrentDataStream().header)
            {
                if (!query_required_columns.empty() && !query_required_columns.contains(name_and_type.name))
                    continue;

                const auto & output_name = name_and_type.name;

                auto rewrite = rewriteExpressionContainsAggregates(
                    normalizeExpression(
                        makeASTIdentifier(output_name), *query_map, query_to_view_table_mappings, view_based_query_equivalences_map),
                    view_output_columns_map,
                    view_outputs,
                    view_aggregate != nullptr,
                    need_rollup,
                    empty_groupings,
                    default_value_provider);
                if (!rewrite)
                {
                    String failure_message = "output column `" + output_name + "` rewrite fail.";
                    if (verbose)
                    {
                        failure_message += "\nquery_map: " + query_map->toString();
                        failure_message += "\nview_output_columns_map: " + toString(view_output_columns_map);
                    }
                    add_failure_message(failure_message);
                    break; // bail out
                }
                auto columns = SymbolsExtractor::extract(*rewrite);
                required_columns_set.insert(columns.begin(), columns.end());

                assignments.emplace_back(Assignment{output_name, *rewrite});
                name_to_type.emplace(output_name, name_and_type.type);

                enforce_agg_node = enforce_agg_node || Utils::containsAggregateFunction(*rewrite);
            }
            size_t required_column_size
                = query_required_columns.empty() ? query->getCurrentDataStream().header.columns() : query_required_columns.size();
            if (assignments.size() != required_column_size)
                continue; // bail out

            // 5-4. if rollup is needed, check group by keys.
            need_rollup = need_rollup || enforce_agg_node;
            std::vector<ASTPtr> rollup_keys;
            if (query_aggregate && need_rollup)
            {
                std::unordered_set<ASTPtr, ASTEquality::ASTHash, ASTEquality::ASTEquals> view_based_query_keys;
                for (const auto & query_key : query_aggregate->getKeys())
                    view_based_query_keys.emplace(normalizeExpression(
                        makeASTIdentifier(query_key), *query_map, query_to_view_table_mappings, view_based_query_equivalences_map));

                for (const auto & query_key : view_based_query_keys)
                {
                    auto rewrite = rewriteExpression(query_key->clone(), view_output_columns_map, view_outputs);
                    if (!rewrite)
                    {
                        add_failure_message("group by column `" + query_key->getColumnName() + "` rewrite fail.");
                        break; // bail out
                    }
                    rollup_keys.emplace_back(*rewrite);
                    auto columns = SymbolsExtractor::extract(*rewrite);
                    required_columns_set.insert(columns.begin(), columns.end());
                }
                if (rollup_keys.size() != view_based_query_keys.size())
                    continue; // bail out, rollup agg group by column rewrite fail.
            }

            // 5-5. check columns in predicates.
            auto rewrite_compensation_predicate = rewriteExpression(compensation_predicate, view_output_columns_map, view_outputs);
            if (!rewrite_compensation_predicate)
            {
                String failure_message = "compensation predicate rewrite fail.";
                if (verbose)
                    failure_message += "\nview_output_columns_map:" + toString(view_output_columns_map);
                add_failure_message(failure_message);
                continue;
            }
            auto columns = SymbolsExtractor::extract(compensation_predicate);
            required_columns_set.insert(columns.begin(), columns.end());

            // 6. rewrite union predicate
            if (!PredicateUtils::isFalsePredicate(union_predicate) || context->getSettingsRef().enforce_materialized_view_union_rewriting)
            {
                if (query_aggregate && query->getType() != IQueryPlanStep::Type::Aggregating)
                {
                    add_failure_message("union rewrite for aggregating only supports query with aggregates on the top");
                    continue; // bail out
                }
                if (query_aggregate && !view_aggregate)
                {
                    add_failure_message("union rewrite for aggregating only supports view with aggregate");
                    continue; // bail out
                }

                EqualityASTMap<ConstASTPtr> query_output_columns_map;
                NameSet outputs;
                for (const auto & plan_node : query_inner_sources)
                {
                    for (const auto & query_output : plan_node->getOutputNames())
                    {
                        outputs.emplace(query_output);
                        query_output_columns_map.emplace(
                            normalizeExpression(makeASTIdentifier(query_output), *query_map), makeASTIdentifier(query_output));
                    }
                }

                auto rewrite_union_predicate = rewriteExpression(union_predicate, query_output_columns_map, outputs);
                if (!rewrite_union_predicate)
                {
                    String failure_message = "union predicate rewrite fail.";
                    if (verbose)
                        failure_message += "\nquery_output_columns_map: " + toString(query_output_columns_map);
                    add_failure_message(failure_message);
                    continue;
                }
                union_predicate = *rewrite_union_predicate;
            }

            // 7. construct candidate
            auto it_stats = materialized_views_stats.find(target_storage_id.getFullTableName());
            if (it_stats == materialized_views_stats.end())
            {
                auto stats = TableScanEstimator::estimate(context, target_storage_id);
                it_stats = materialized_views_stats.emplace(target_storage_id.getFullTableName(), stats).first;
            }

            NamesWithAliases table_columns_with_aliases;
            if (required_columns_set.empty()) {
                required_columns_set.emplace(*view_outputs.begin());
            }
            for (const auto & column : required_columns_set)
            {
                auto it = view_outputs_to_table_columns_map.find(column);
                if (it == view_outputs_to_table_columns_map.end())
                {
                    add_failure_message("Logical error: column `" + column + "` not found in table output columns.");
                    continue; // bail out
                }
                table_columns_with_aliases.emplace_back(it->second, column);
            }

            auto storage = DatabaseCatalog::instance().getTable(target_storage_id, context);
            auto metadata_snapshot = storage->getInMemoryMetadataPtr();
            const auto & ordered_columns = metadata_snapshot->sorting_key.column_names;
            bool contains_ordered_columns = false;
            if (!ordered_columns.empty())
            {
                for (const auto & column_with_alias : table_columns_with_aliases)
                {
                    if (ordered_columns[0] == column_with_alias.first && columns.count(column_with_alias.second))
                    {
                        contains_ordered_columns = true;
                        break;
                    }
                }
            }

            // 8. other query info
            // 8-1. keep prewhere info for single table rewriting
            ASTPtr prewhere_expr;
            if (query_join_hyper_graph.getPlanNodes().size() == 1)
            {
                const auto * table_scan = dynamic_cast<const TableScanStep *>(query_join_hyper_graph.getPlanNodes().at(0)->getStep().get());
                if (table_scan != nullptr && table_scan->getQueryInfo().query)
                {
                    auto & select_query = table_scan->getQueryInfo().query->as<ASTSelectQuery &>();
                    if (select_query.prewhere())
                    {
                        auto rewritten_prewhere = rewriteExpression(
                            normalizeExpression(
                                select_query.prewhere(), *query_map, query_to_view_table_mappings, view_based_query_equivalences_map),
                            view_output_columns_map,
                            view_outputs);
                        if (rewritten_prewhere)
                        {
                            std::unordered_map<String, String> mappings;
                            for (const auto & item : table_columns_with_aliases)
                                mappings.emplace(item.second, item.first);
                            SymbolMapper symbol_mapper = SymbolMapper::symbolMapper(mappings);
                            prewhere_expr = symbol_mapper.map(*rewritten_prewhere);
                        }
                    }
                }
            }

            return RewriterCandidate{
                view_storage_id,
                target_storage_id,
                prewhere_expr,
                it_stats->second,
                contains_ordered_columns,
                table_columns_with_aliases,
                assignments,
                name_to_type,
                *rewrite_compensation_predicate,
                need_rollup,
                rollup_keys,
                union_predicate};
        }
        // no matches
        return {};
    }

    static std::shared_ptr<const DB::AggregatingStep> extractTopAggregate(PlanNodePtr query)
    {
        if (query->getType() == IQueryPlanStep::Type::Aggregating)
            return dynamic_pointer_cast<const DB::AggregatingStep>(query->getStep());
        if (query->getChildren().size() == 1)
            return extractTopAggregate(query->getChildren()[0]);
        return nullptr;
    }

    static PredicateInfo extractPredicates(const JoinHyperGraph & join_hyper_graph, const JoinHyperGraph::NodeSet & node_set)
    {
        std::vector<ConstASTPtr> inner_predictes;
        std::unordered_map<JoinHyperGraph::NodeSet, std::vector<ConstASTPtr>> outer_predictes;
        std::unordered_map<JoinHyperGraph::NodeSet, std::vector<ConstASTPtr>> outer_join_clauses;

        for (const auto & filter : join_hyper_graph.getFilters())
        {
            if ((node_set & filter.first) == filter.first)
                inner_predictes.insert(inner_predictes.end(), filter.second.begin(), filter.second.end());
            else
                outer_predictes[filter.first].insert(outer_predictes[filter.first].end(), filter.second.begin(), filter.second.end());
        }

        for (const auto & filter : join_hyper_graph.getJoinConditions())
        {
            if ((node_set & filter.first) == filter.first)
                inner_predictes.insert(inner_predictes.end(), filter.second.begin(), filter.second.end());
            else
                outer_join_clauses[filter.first].insert(outer_join_clauses[filter.first].end(), filter.second.begin(), filter.second.end());
        }

        auto equal_predicates = PredicateUtils::extractEqualPredicates(inner_predictes);
        return PredicateInfo{std::move(equal_predicates.first), std::move(equal_predicates.second), std::move(outer_predictes), std::move(outer_join_clauses)};
    }

    static String toString(const EqualityASTMap<ConstASTPtr> & equality_map)
    {
        String res;
        for (const auto & item : equality_map)
            res += queryToString(*item.first) + " -> " + queryToString(*item.second) + "; ";
        return res;
    }

    /**
     * It will flatten a multimap containing table references to table references, producing all possible combinations of mappings.
     */
    static std::vector<TableInputRefMapping> generateFlatTableMappings(const TableInputRefMultiMapping & table_multi_mapping)
    {
        if (table_multi_mapping.empty())
            return {};

        std::vector<TableInputRefMapping> table_mappings;
        table_mappings.emplace_back(); // init with empty map
        for (const auto & item : table_multi_mapping)
        {
            const auto & query_table_ref = item.first;
            const auto & view_table_refs = item.second;
            if (view_table_refs.size() == 1)
            {
                for (auto & mappings : table_mappings)
                    mappings.emplace(query_table_ref, view_table_refs[0]);
            }
            else
            {
                std::vector<TableInputRefMapping> new_table_mappings;
                for (const auto & view_table_ref : view_table_refs)
                {
                    for (auto & mappings : table_mappings)
                    {
                        bool contains = std::any_of(mappings.begin(), mappings.end(), [&](const auto & m) {
                            return TableInputRefEqual{}.operator()(m.second, view_table_ref);
                        });
                        if (contains)
                            continue;

                        auto new_mappings = mappings;
                        new_mappings.emplace(query_table_ref, view_table_ref);
                        new_table_mappings.emplace_back(std::move(new_mappings));
                    }
                }
                table_mappings.swap(new_table_mappings);
            }
        }
        return table_mappings;
    }

    /**
     * We try to extract table reference and create table mapping relation from query to view.
     * We only check table size here, Join-Relation will check later using table mapping.
     * There we consider extended scenarios, like query missing tables, view missing tables, and self-join.
     */
    static std::optional<JoinSetMatchResult>
    matchJoinSet(const JoinHyperGraph & query_graph, const JoinHyperGraph & view_graph, ContextMutablePtr context)
    {
        if (query_graph.isEmpty())
            return {}; // bail out, if there is no tables.
        const auto & query_nodes = query_graph.getPlanNodes();
        const auto & view_nodes = view_graph.getPlanNodes();

        if (!context->getSettingsRef().enable_materialized_view_join_rewriting && query_nodes.size() > 1) {
            return {};
        }

        std::unordered_map<String, std::vector<std::pair<PlanNodePtr, TableInputRef>>> query_table_map;
        for (const auto & node : query_nodes)
        {
            const auto * table_step = dynamic_cast<const TableScanStep *>(node->getStep().get());
            if (!table_step)
                return {};
            TableInputRef table_input_ref{table_step->getStorage(), node->getId()};
            query_table_map[table_input_ref.getDatabaseTableName()].emplace_back(node, table_input_ref);
        }

        std::unordered_map<String, std::vector<TableInputRef>> view_table_map;
        for (const auto & node : view_nodes)
        {
            const auto * table_step = dynamic_cast<const TableScanStep *>(node->getStep().get());
            if (!table_step)
                return {};
            TableInputRef table_input_ref{table_step->getStorage(), node->getId()};
            view_table_map[table_input_ref.getDatabaseTableName()].emplace_back(table_input_ref);
        }

        bool is_query_missing_table = std::any_of(view_table_map.begin(), view_table_map.end(), [&](const auto & item) {
            return item.second.size() > query_table_map[item.first].size();
        });
        if (is_query_missing_table)
            return {}; // bail out, if some tables are missing in the query

        TableInputRefMultiMapping table_multi_mapping;
        // std::vector<TableInputRef> view_missing_tables;
        // std::unordered_map<String, std::shared_ptr<ASTTableColumnReference>> view_missing_columns;
        for (auto & item : query_table_map)
        {
            auto & query_table_refs = item.second;
            auto view_table_refs = view_table_map[item.first];

            // not supported yet
            if (query_table_refs.size() != view_table_refs.size())
                return {};

            for (auto & query_table_ref : query_table_refs)
                table_multi_mapping[query_table_ref.second] = view_table_refs;
        }

        return JoinSetMatchResult{table_multi_mapping /*, view_missing_tables, view_missing_columns*/};
    }

    static JoinHyperGraph::NodeSet mapQueryNodeSet(const JoinHyperGraph::NodeSet & query_node_set, const TableMapping & table_mapping) {
        JoinHyperGraph::NodeSet view_node_set;
        for (size_t i = 0; i < table_mapping.size(); i++)
            if (query_node_set.test(i))
                view_node_set.set(table_mapping.at(i));
        return view_node_set;
    }

    /**
     * Here we check Join-Relation using table mapping, return compensation predicate
     */
    static std::optional<std::vector<ConstASTPtr>> matchJoinRelation(
        const JoinHyperGraph & query_graph,
        const JoinHyperGraph & view_graph,
        const PlanNodes & view_inner_sources,
        const TableMapping & table_mapping,
        const JoinSetMatchResult & /*join_set_match_result*/)
    {
        std::vector<ConstASTPtr> compensation_predicates;
        if (query_graph.getEdges().empty()) // single table scan
            return compensation_predicates;

        bool all_inner = std::all_of(query_graph.getEdges().begin(), query_graph.getEdges().end(), [](const auto & edge) {
            return edge.join_step->getKind() == ASTTableJoin::Kind::Inner;
        }) && std::all_of(view_graph.getEdges().begin(), view_graph.getEdges().end(), [](const auto & edge) {
            return edge.join_step->getKind() == ASTTableJoin::Kind::Inner;
        });
        if (all_inner)
            return compensation_predicates;

        if (query_graph.getEdges().size() != view_graph.getEdges().size())
            return {}; // baild out, maybe remove this check for query compensation rewrite

        auto view_inner_sources_node_set = view_graph.getNodeSet(view_inner_sources);

        auto match_join_edge = [&](const ASTTableJoin::Kind & query_kind,
                                   const ASTTableJoin::Strictness & query_strictness,
                                   const JoinHyperGraph::NodeSet & query_left_conditions_used_nodes,
                                   const JoinHyperGraph::NodeSet & query_right_conditions_used_nodes,
                                   const JoinHyperGraph::NodeSet & query_left_required_nodes,
                                   const JoinHyperGraph::NodeSet & query_right_required_nodes,
                                   const JoinHyperGraph::Edge & view_edge,
                                   const JoinHyperGraph::HyperEdge & view_hyper_edge) -> bool {
            ASTTableJoin::Kind view_kind = view_edge.join_step->getKind();
            ASTTableJoin::Strictness view_strictness = view_edge.join_step->getStrictness();
            if (query_strictness != view_strictness)
                return false;

            if (query_kind == view_kind)
            {
                if (view_edge.left_conditions_used_nodes == query_left_conditions_used_nodes
                    && view_edge.right_conditions_used_nodes == query_right_conditions_used_nodes
                    && view_hyper_edge.left_required_nodes == query_left_required_nodes
                    && view_hyper_edge.right_required_nodes == query_right_required_nodes)
                {
                    return true;
                }
            }

            // left to right
            if ((query_kind == ASTTableJoin::Kind::Inner && view_kind == ASTTableJoin::Kind::Inner)
                || (query_kind == ASTTableJoin::Kind::Left && view_kind == ASTTableJoin::Kind::Right)
                || (query_kind == ASTTableJoin::Kind::Right && view_kind == ASTTableJoin::Kind::Left))
            {
                if (view_edge.left_conditions_used_nodes == query_right_conditions_used_nodes
                    && view_edge.right_conditions_used_nodes == query_left_conditions_used_nodes
                    && view_hyper_edge.left_required_nodes == query_right_required_nodes
                    && view_hyper_edge.right_required_nodes == query_left_required_nodes)
                {
                    return true;
                }
            }

            return false;
        };

        std::unordered_set<size_t> matched_view_edges;
        for (size_t i = 0; i < query_graph.getEdges().size(); i++)
        {
            const auto & query_edge = query_graph.getEdges()[i];
            const auto & query_hyper_edge = query_graph.getHyperEdges()[i];

            ASTTableJoin::Kind query_kind = query_edge.join_step->getKind();
            ASTTableJoin::Strictness query_strictness = query_edge.join_step->getStrictness();

            auto query_left_conditions_used_nodes = mapQueryNodeSet(query_edge.left_conditions_used_nodes, table_mapping);
            auto query_right_conditions_used_nodes = mapQueryNodeSet(query_edge.right_conditions_used_nodes, table_mapping);
            auto query_left_required_nodes = mapQueryNodeSet(query_hyper_edge.left_required_nodes, table_mapping);
            auto query_right_required_nodes = mapQueryNodeSet(query_hyper_edge.right_required_nodes, table_mapping);

            bool matched = false;
            for (size_t j = 0; j < view_graph.getEdges().size(); j++)
            {
                if (matched_view_edges.contains(i))
                    continue;

                matched = match_join_edge(
                    query_kind,
                    query_strictness,
                    query_left_conditions_used_nodes,
                    query_right_conditions_used_nodes,
                    query_left_required_nodes,
                    query_right_required_nodes,
                    view_graph.getEdges()[i],
                    view_graph.getHyperEdges()[i]);
                if (matched)
                {
                    matched_view_edges.emplace(i);
                    break;
                }
            }

            // join derive: try compute inner from left
            // TODO: other join type like anti.
            if (!matched && query_kind == ASTTableJoin::Kind::Inner)
            {
                for (size_t j = 0; j < view_graph.getEdges().size(); j++)
                {
                    if (matched_view_edges.contains(i))
                        continue;

                    const auto & view_edge = view_graph.getEdges()[i];
                    const auto & view_hyper_edge = view_graph.getHyperEdges()[i];

                    if (view_edge.join_step->getLeftKeys().empty())
                        continue;

                    // check join derive tables is query inner sources
                    JoinHyperGraph::NodeSet inner_sources;
                    if (view_edge.join_step->getKind() == ASTTableJoin::Kind::Left)
                        inner_sources = view_edge.left_conditions_used_nodes;
                    else if (view_edge.join_step->getKind() == ASTTableJoin::Kind::Right)
                        inner_sources = view_edge.right_conditions_used_nodes;
                    else
                        continue;

                    if ((view_inner_sources_node_set & inner_sources) != inner_sources)
                        continue;

                    matched = match_join_edge(
                                  ASTTableJoin::Kind::Left,
                                  query_strictness,
                                  query_left_conditions_used_nodes,
                                  query_right_conditions_used_nodes,
                                  query_left_required_nodes,
                                  query_right_required_nodes,
                                  view_edge,
                                  view_hyper_edge)
                        || match_join_edge(
                                  ASTTableJoin::Kind::Right,
                                  query_strictness,
                                  query_left_conditions_used_nodes,
                                  query_right_conditions_used_nodes,
                                  query_left_required_nodes,
                                  query_right_required_nodes,
                                  view_edge,
                                  view_hyper_edge);
                    if (matched)
                    {
                        compensation_predicates.emplace_back(makeASTFunction(
                            "isNotNull",
                            view_edge.join_step->getKind() == ASTTableJoin::Kind::Left
                                ? makeASTIdentifier(view_edge.join_step->getLeftKeys()[0])
                                : makeASTIdentifier(view_edge.join_step->getRightKeys()[0])));
                        matched_view_edges.emplace(i);
                        break;
                    }
                }
            }

            if (!matched)
                return {}; // bail out
        }
        return compensation_predicates;
    }

public:
    ContextMutablePtr context;
    LinkedHashMap<MaterializedViewStructurePtr, PartitionCheckResult> & materialized_views;
    std::unordered_map<PlanNodePtr, RewriterCandidates> candidates;
    std::unordered_map<PlanNodePtr, RewriterFailureMessages> failure_messages;
    std::map<String, std::optional<PlanNodeStatisticsPtr>> materialized_views_stats;
    const bool verbose;
    SimpleCTEVisitHelper<BaseTables> cte_helper;
    LoggerPtr logger = getLogger("CandidatesExplorer");
};

using ASTToStringMap = EqualityASTMap<String>;

class CostBasedMaterializedViewRewriter : public SimplePlanRewriter<Void>
{
public:
    static bool rewrite(QueryPlan & plan, ContextMutablePtr context_, std::unordered_map<PlanNodePtr, RewriterCandidates> & match_results)
    {
        Void c;
        CostBasedMaterializedViewRewriter rewriter(context_, plan.getCTEInfo(), match_results);
        auto rewrite = VisitorUtil::accept(plan.getPlanNode(), rewriter, c);
        plan.update(rewrite);
        return true;
    }

protected:
    CostBasedMaterializedViewRewriter(ContextMutablePtr context_, CTEInfo & cte_info, std::unordered_map<PlanNodePtr, RewriterCandidates> & match_results_)
        : SimplePlanRewriter(context_, cte_info), match_results(match_results_)
    {
    }

    PlanNodePtr visitPlanNode(PlanNodeBase & node, Void & c) override
    {
        if (auto it = match_results.find(node.shared_from_this()); it != match_results.end())
        {
            auto & candidates = it->second;
            auto candidate_it = std::min_element(candidates.begin(), candidates.end(), RewriterCandidateSort());
            LOG_INFO(log, "use materialized view {} for query rewriting", candidate_it->view_database_and_table_name.getFullTableName());
            try
            {
                auto plan = constructEquivalentPlan(node.shared_from_this(), *candidate_it);
                context->getOptimizerMetrics()->addMaterializedView(candidate_it->view_database_and_table_name);
                return plan;
            }
            catch (...)
            {
                tryLogCurrentException(
                    log,
                    "construct equivalent plan use materialized view failed: "
                        + candidate_it->view_database_and_table_name.getFullTableName());
                return SimplePlanRewriter::visitPlanNode(node, c);
            }
        }
        return SimplePlanRewriter::visitPlanNode(node, c);
    }

    PlanNodePtr constructEquivalentPlan(const PlanNodePtr & query, const RewriterCandidate & candidate)
    {
        // table scan
        auto plan = planTableScan(candidate.target_database_and_table_name, candidate.table_output_columns, candidate.prewhere_expr);

        // where
        if (!PredicateUtils::isTruePredicate(candidate.compensation_predicate))
            plan = PlanNodeBase::createPlanNode(
                context->nextNodeId(), std::make_shared<FilterStep>(plan->getCurrentDataStream(), candidate.compensation_predicate), {plan});

        plan = checkOutputTypes(plan);

        if (candidate.need_rollup)
        {
            // aggregate
            auto [aggregate, rewrite_assignments] = planAggregate(plan, candidate.need_rollup, candidate.rollup_keys, candidate.assignments);

            // reallocate symbols from view scope to query scope
            auto [reallocate, mappings] = PlanSymbolReallocator::reallocate(aggregate, context);
            plan = reallocate;

            // prepare plan final output projection
            SymbolMapper symbol_mapper = SymbolMapper::symbolMapper(mappings);
            Assignments assignments;
            for (const auto & assignemnt : rewrite_assignments)
                assignments.emplace(assignemnt.first, symbol_mapper.map(assignemnt.second));

            // plan aggregate + union rewrite before
            if (!PredicateUtils::isFalsePredicate(candidate.union_predicate)
                || context->getSettingsRef().enforce_materialized_view_union_rewriting)
                plan = planUnionBeforeAggragte(plan, query, candidate.union_predicate, assignments);

            plan = PlanNodeBase::createPlanNode(
                context->nextNodeId(),
                std::make_shared<ProjectionStep>(plan->getCurrentDataStream(), assignments, candidate.name_to_type),
                {plan});
            return plan;
        }
        else
        {
            // output project
            plan = PlanNodeBase::createPlanNode(
                context->nextNodeId(),
                std::make_shared<ProjectionStep>(plan->getCurrentDataStream(), candidate.assignments, candidate.name_to_type),
                {plan});

            // simple union rewrite
            if (!PredicateUtils::isFalsePredicate(candidate.union_predicate)
                || context->getSettingsRef().enforce_materialized_view_union_rewriting)
                plan = planUnion(plan, query, candidate.union_predicate);

                        // reallocate symbols
            PlanNodeAndMappings plan_node_and_mappings = PlanSymbolReallocator::reallocate(plan, context);
            plan = plan_node_and_mappings.plan_node;

            // create projection step to guarante output symbols
            Assignments assignments;
            for (const auto & output : candidate.name_to_type)
                assignments.emplace_back(output.first, std::make_shared<ASTIdentifier>(plan_node_and_mappings.mappings.at(output.first)));

            plan = PlanNodeBase::createPlanNode(
                context->nextNodeId(),
                std::make_shared<ProjectionStep>(plan->getStep()->getOutputStream(), assignments, candidate.name_to_type),
                {plan});
            return plan;
        }
    }

    PlanNodePtr checkOutputTypes(PlanNodePtr plan)
    {
        Assignments assignments;
        NameToType name_to_type;
        for(size_t i = 0; i < plan->getStep()->getOutputStream().header.columns(); i++)
        {
            const auto & column = plan->getStep()->getOutputStream().header.getByPosition(i);
            auto type = column.type;
            type = removeNullable(removeLowCardinality(type));

            const auto * single_aggregate_function = dynamic_cast<const DataTypeCustomSimpleAggregateFunction *>( type->getCustomName());
            if (single_aggregate_function)
            {
                assignments.emplace(
                    column.name,
                    makeASTFunction(
                        "cast",
                        makeASTIdentifier(column.name),
                        std::make_shared<ASTLiteral>(single_aggregate_function->getFunction()->getReturnType()->getName())));
                name_to_type.emplace(column.name, single_aggregate_function->getFunction()->getReturnType());
            }
            else
            {
                assignments.emplace(column.name, makeASTIdentifier(column.name));
                name_to_type.emplace(column.name, type);
            }
        }
        if (!Utils::isIdentity(assignments))
        {
            plan = PlanNodeBase::createPlanNode(
                    context->nextNodeId(),
                    std::make_shared<ProjectionStep>(plan->getCurrentDataStream(), assignments, name_to_type),
                    {plan});
        }
        return plan;
    }

    PlanNodePtr planTableScan(
        const StorageID & target_database_and_table_name,
        const NamesWithAliases & columns_with_aliases,
        ASTPtr prewhere_expr)
    {
        const auto select_expression_list = std::make_shared<ASTExpressionList>();
        select_expression_list->children.reserve(columns_with_aliases.size());
        /// manually substitute column names in place of asterisk
        for (const auto & column : columns_with_aliases)
            select_expression_list->children.emplace_back(std::make_shared<ASTIdentifier>(column.first));

        SelectQueryInfo query_info;
        const auto generated_query = std::make_shared<ASTSelectQuery>();
        generated_query->setExpression(ASTSelectQuery::Expression::SELECT, select_expression_list);
        generated_query->replaceDatabaseAndTable(target_database_and_table_name);
        if (prewhere_expr)
            generated_query->setExpression(ASTSelectQuery::Expression::PREWHERE, std::move(prewhere_expr));

        generated_query->replaceDatabaseAndTable(
            target_database_and_table_name.getDatabaseName(), target_database_and_table_name.getTableName());

        query_info.query = generated_query;

        UInt64 max_block_size = context->getSettingsRef().max_block_size;
        if (!max_block_size)
            throw Exception("Setting 'max_block_size' cannot be zero", ErrorCodes::PARAMETER_OUT_OF_BOUND);

        return PlanNodeBase::createPlanNode(
            context->nextNodeId(),
            std::make_shared<TableScanStep>(
                context,
                target_database_and_table_name,
                columns_with_aliases,
                query_info,
                max_block_size));
    }

    // Union + Rollup Rewrite
    //
    // Before:
    //  view:
    //    - agg[v0]
    //      - projection[v1]
    //        - ts[v2]
    //  query:
    //    - agg[q1]
    //      - any [q2]
    //
    // After:
    //  view:
    //    - agg[v0]
    //      - union
    //        - projection[v1]
    //          - ts[v2]
    //        - agg[q1]
    //          - filter
    //             - any[q2]
    PlanNodePtr
    planUnionBeforeAggragte(const PlanNodePtr & view, PlanNodePtr query, ASTPtr union_predicate, const Assignments & assignments)
    {
        if (query->getStep()->getType() != IQueryPlanStep::Type::Aggregating || view->getStep()->getType() != IQueryPlanStep::Type::Aggregating)
            throw Exception("union rewrite failed: view & query root node expectes as aggregate", ErrorCodes::LOGICAL_ERROR);

        const auto & view_step = dynamic_cast<const AggregatingStep &>(*view->getStep().get());
        const auto & query_step = dynamic_cast<const AggregatingStep &>(*query->getStep().get());

        OutputToInputs output_to_inputs;
        for (const auto & key : query_step.getKeys())
        {
            const auto & view_key = assignments.at(key);
            if (view_key->getType() != ASTType::ASTIdentifier)
                throw Exception("union rewrite failed: view_key expected as identifier", ErrorCodes::LOGICAL_ERROR);
            const auto & view_key_name = view_key->as<ASTIdentifier>()->name();

            auto & inputs = output_to_inputs[view_key_name];
            inputs.emplace_back(view_key_name);
            inputs.emplace_back(key);
        }

        std::unordered_map<String, const AggregateDescription *> view_name_to_aggreagte;
        for (const auto & aggregate : view_step.getAggregates())
            view_name_to_aggreagte.emplace(aggregate.column_name, &aggregate);

        AggregateDescriptions rewrite_aggregates;
        Names group_keys{query_step.getKeys().begin(), query_step.getKeys().end()};
        for (const auto & aggregate : query_step.getAggregates())
        {
            const auto & view_aggregate_output = assignments.at(aggregate.column_name);
            if (view_aggregate_output->getType() != ASTType::ASTIdentifier)
                throw Exception("union rewrite failed: view_aggregate_output expected as identifier", ErrorCodes::LOGICAL_ERROR);
            const auto & view_aggregate_output_name = view_aggregate_output->as<ASTIdentifier>()->name();
            const auto * view_aggreagte = view_name_to_aggreagte.at(view_aggregate_output_name);

            auto rewrite = aggregate;
            if (view_aggreagte->function->getName().ends_with("Merge"))
            {
                String function_name = view_aggreagte->function->getName().substr(0, view_aggreagte->function->getName().size() - 5) + "State";
                AggregateFunctionProperties properties;
                rewrite.function = AggregateFunctionFactory::instance().get(
                    function_name, aggregate.function->getArgumentTypes(), aggregate.function->getParameters(), properties);
            }

            // direct rollup
            if (view_aggreagte->function->getArgumentTypes().size() == 1
                && rewrite.function->getReturnType()->equals(*view_aggreagte->function->getArgumentTypes()[0]))
            {
                rewrite_aggregates.emplace_back(rewrite);
                auto & inputs = output_to_inputs[view_aggreagte->argument_names[0]];
                inputs.emplace_back(view_aggreagte->argument_names[0]);
                inputs.emplace_back(aggregate.column_name);
                continue;
            }

            // rollup on group by results
            if (aggregate.function->getName() == view_aggreagte->function->getName()
                && aggregate.function->getArgumentTypes() == view_aggreagte->function->getArgumentTypes()
                && aggregate.function->getParameters() == view_aggreagte->function->getParameters())
            {
                for (size_t i = 0; i < view_aggreagte->argument_names.size(); i++)
                {
                    auto & inputs = output_to_inputs[view_aggreagte->argument_names[i]];
                    inputs.emplace_back(view_aggreagte->argument_names[i]);
                    inputs.emplace_back(aggregate.argument_names[i]);

                    group_keys.emplace_back(aggregate.argument_names[i]);
                }
                continue;
            }
            throw Exception("aggregate not support", ErrorCodes::LOGICAL_ERROR);
        }

        InsertFilterRewriter rewriter{context, union_predicate};
        Void c;
        auto query_with_filter = VisitorUtil::accept(query->getChildren()[0], rewriter, c);

        query_with_filter = PlanNodeBase::createPlanNode(
            context->nextNodeId(),
            std::make_shared<AggregatingStep>(
                query_with_filter->getStep()->getOutputStream(),
                makeDistinct(group_keys),
                query_step.getKeysNotHashed(),
                rewrite_aggregates,
                query_step.getGroupingSetsParams(),
                query_step.isFinal(),
                query_step.getGroupBySortDescription(),
                query_step.getGroupings(),
                query_step.needOverflowRow(),
                query_step.shouldProduceResultsInOrderOfBucketNumber()),
            {query_with_filter});

        auto [reallocate, mappings] = PlanSymbolReallocator::reallocate(query_with_filter, context);
        for (auto & item : output_to_inputs)
            item.second[1] = mappings.at(item.second[1]);

        const auto & view_child = view->getChildren()[0];

        Block output_stream;
        for (const auto & output : view_child->getStep()->getOutputStream().header)
        {
            if (output_to_inputs.contains(output.name))
                output_stream.insert(output);
        }

        auto unionn = PlanNodeBase::createPlanNode(
            context->nextNodeId(),
            std::make_shared<UnionStep>(
                DataStreams{view_child->getStep()->getOutputStream(), reallocate->getStep()->getOutputStream()},
                DataStream{output_stream},
                output_to_inputs),
            {view_child, reallocate});

        return PlanNodeBase::createPlanNode(context->nextNodeId(), view->getStep(), {unionn});
    }

    static std::vector<String> makeDistinct(const std::vector<String> & src)
    {
        std::unordered_set<String> tmp;
        std::vector<String> result;

        for (const auto & str : src)
        {
            if (tmp.insert(str).second)
            {
                result.push_back(str);
            }
        }

        return result;
    }

    // Union + Rollup Rewrite
    PlanNodePtr planUnion(const PlanNodePtr & view, const PlanNodePtr & query, ASTPtr union_predicate)
    {
        InsertFilterRewriter rewriter{context, union_predicate};
        Void c;
        auto query_with_filter = VisitorUtil::accept(query, rewriter, c);

        OutputToInputs output_to_inputs;
        auto view_outputs = view->getOutputNames();
        auto query_outputs = query->getOutputNames();

        auto [reallocate, mappings] = PlanSymbolReallocator::reallocate(query_with_filter, context);
        for (size_t i = 0; i < view_outputs.size(); i++)
        {
            auto & inputs = output_to_inputs[view_outputs[i]];
            inputs.emplace_back(view_outputs[i]);
            inputs.emplace_back(mappings.at(query_outputs[i]));
        }

        auto unionn = PlanNodeBase::createPlanNode(
            context->nextNodeId(),
            std::make_shared<UnionStep>(
                DataStreams{view->getStep()->getOutputStream(), reallocate->getStep()->getOutputStream()},
                view->getStep()->getOutputStream(),
                output_to_inputs),
            {view, reallocate});
        return unionn;
    }

    /**
     * Extracts aggregates from assignments, plan (rollup) aggregate nodes, return final projection assignments.
     */
    std::pair<PlanNodePtr, Assignments>
    planAggregate(PlanNodePtr plan, bool need_rollup, const std::vector<ASTPtr> & rollup_keys, const Assignments & assignments)
    {
        if (!need_rollup)
            return {plan->shared_from_this(), assignments};

        const auto & input_types = plan->getCurrentDataStream().header.getNamesAndTypes();

        Names keys;
        Assignments arguments;
        NameToType arguments_types;
        ASTToStringMap group_keys;
        for (const auto & key : rollup_keys)
        {
            auto new_symbol = context->getSymbolAllocator()->newSymbol(key);
            auto type = TypeAnalyzer::getType(key, context, input_types);
            arguments.emplace_back(new_symbol, key);
            group_keys.emplace(key, new_symbol);
            arguments_types.emplace(new_symbol, type);
            keys.emplace_back(new_symbol);
        }

        AggregateRewriter agg_rewriter{context, input_types, arguments, arguments_types};
        GroupByKeyRewriter key_rewriter{};
        Void c;
        Assignments rewrite_arguments;
        for (const auto & assignment : assignments)
        {
            // extract aggregates into aggregating node
            auto rewritten_expression = ASTVisitorUtil::accept(assignment.second->clone(), agg_rewriter, c);
            // rewrite remaining expression using group by keys
            rewritten_expression = ASTVisitorUtil::accept(rewritten_expression, key_rewriter, group_keys);
            rewrite_arguments.emplace_back(assignment.first, rewritten_expression);
        }

        if (!agg_rewriter.aggregates.empty() || !keys.empty())
        {
            if (!Utils::isIdentity(arguments))
                plan = PlanNodeBase::createPlanNode(
                    context->nextNodeId(),
                    std::make_shared<ProjectionStep>(plan->getCurrentDataStream(), arguments, arguments_types),
                    {plan});

            plan = PlanNodeBase::createPlanNode(
                context->nextNodeId(),
                std::make_shared<AggregatingStep>(plan->getCurrentDataStream(), keys, NameSet{}, agg_rewriter.aggregates, GroupingSetsParamsList{}, true),
                {plan});
        }
        return std::make_pair(plan, rewrite_arguments);
    }

private:
    std::unordered_map<PlanNodePtr, RewriterCandidates> & match_results;
    LoggerPtr log = getLogger("MaterializedViewCostBasedRewriter");

    class AggregateRewriter : public SimpleExpressionRewriter<Void>
    {
    public:
        AggregateRewriter(
            ContextMutablePtr context_, const NamesAndTypes & input_types_, Assignments & arguments_, NameToType & arguments_types_)
            : context(context_), input_types(input_types_), arguments(arguments_), arguments_types(arguments_types_)
        {
        }

        ASTPtr visitASTFunction(ASTPtr & node, Void & c) override
        {
            auto function = node->as<ASTFunction &>();
            if (!AggregateFunctionFactory::instance().isAggregateFunctionName(function.name))
                return visitNode(node, c);

            auto agg_it = aggregates_map.find(node);
            if (agg_it == aggregates_map.end())
            {
                std::vector<String> agg_arguments;
                DataTypes agg_argument_types;
                for (auto & argument : function.arguments->children)
                {
                    auto it = arguments_map.find(argument);
                    if (it == arguments_map.end())
                    {
                        auto new_symbol = context->getSymbolAllocator()->newSymbol(argument);
                        auto type = TypeAnalyzer::getType(argument, context, input_types);
                        it = arguments_map.emplace(argument, new_symbol).first;
                        arguments_types.emplace(new_symbol, type);
                        arguments.emplace_back(new_symbol, argument);
                    }
                    auto & name = it->second;
                    agg_arguments.emplace_back(name);
                    agg_argument_types.emplace_back(arguments_types.at(name));
                }
                Array parameters;
                if (function.parameters)
                    for (auto & argument : function.parameters->children)
                        parameters.emplace_back(argument->as<ASTLiteral &>().value);
                auto output_column = context->getSymbolAllocator()->newSymbol(node);
                AggregateDescription aggregate_description;
                AggregateFunctionProperties properties;
                aggregate_description.function = AggregateFunctionFactory::instance().get(
                    function.name, agg_argument_types, parameters, properties);
                aggregate_description.argument_names = agg_arguments;
                aggregate_description.parameters = parameters;
                aggregate_description.column_name = output_column;
                aggregates.emplace_back(aggregate_description);
                agg_it = aggregates_map.emplace(node, output_column).first;
            }
            return std::make_shared<ASTIdentifier>(agg_it->second);
        }

        ContextMutablePtr context;
        const NamesAndTypes & input_types;
        Assignments & arguments;
        NameToType & arguments_types;
        AggregateDescriptions aggregates;

        EqualityASTMap<String> arguments_map;
        EqualityASTMap<String> aggregates_map;
    };

    class GroupByKeyRewriter : public SimpleExpressionRewriter<ASTToStringMap>
    {
    public:
        ASTPtr visitNode(ASTPtr & node, ASTToStringMap & map) override
        {
            auto it = map.find(node);
            if (it != map.end())
                return std::make_shared<ASTIdentifier>(it->second);
            return SimpleExpressionRewriter::visitNode(node, map);
        }
    };

    class InsertFilterRewriter : public PlanNodeVisitor<PlanNodePtr, Void>
    {
    public:
        InsertFilterRewriter(ContextMutablePtr context_, ASTPtr predicate_)
            : context(std::move(context_)), predicate(std::move(predicate_)), required_symbols(SymbolsExtractor::extract(predicate))
        {
        }

        PlanNodePtr visitPlanNode(PlanNodeBase & node, Void & c) override
        {
            PlanNodes children;
            for (const auto & item : node.getChildren())
            {
                PlanNodePtr child = VisitorUtil::accept(*item, *this, c);
                children.emplace_back(child);
            }
            node.replaceChildren(children);

            if (!predicate_added)
            {
                for (const auto & symbol : node.getOutputNames())
                {
                    if (required_symbols.contains(symbol))
                    {
                        predicate_added = true;
                        return PlanNodeBase::createPlanNode(
                            context->nextNodeId(),
                            std::make_shared<FilterStep>(node.getStep()->getOutputStream(), predicate),
                            {node.shared_from_this()});
                    }
                }
            }

            return node.shared_from_this();
        }

    private:
        ContextMutablePtr context;
        ASTPtr predicate;
        std::set<std::string> required_symbols;
        bool predicate_added = false;
    };
};
}

bool MaterializedViewRewriter::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    try
    {
        return rewriteImpl(plan, context);
    }
    catch (...)
    {
        if (context->getSettingsRef().enforce_materialized_view_rewrite)
            throw;
        tryLogCurrentException(log, "materialized view rewrite failed.");
        return false;
    }
}

bool MaterializedViewRewriter::rewriteImpl(QueryPlan & plan, ContextMutablePtr context) const
{
    bool enforce = context->getSettingsRef().enforce_materialized_view_rewrite;
    bool verbose = context->getSettingsRef().enable_materialized_view_rewrite_verbose_log;

    auto materialized_views = getRelatedMaterializedViews(plan, context);
    if (materialized_views.empty())
    {
        if (enforce)
            throw Exception("no related materialized views", ErrorCodes::NO_AVAILABLE_MATERIALIZED_VIEW);
        return false;
    }

    auto candidates = CandidatesExplorer::explore(plan, context, materialized_views, verbose);
    if (candidates.empty())
    {
        if (enforce)
            throw Exception("no materialized view candidates", ErrorCodes::NO_AVAILABLE_MATERIALIZED_VIEW);
        return false;
    }

    CostBasedMaterializedViewRewriter::rewrite(plan, context, candidates);
    // todo: add monitoring metrics

    PredicatePushdown predicate_push_down;
    predicate_push_down.rewritePlan(plan, context);
    auto res = PlanSymbolReallocator::reallocate(plan.getPlanNode(), context);
    plan.update(res.plan_node);
    return true;
}

LinkedHashMap<MaterializedViewStructurePtr, PartitionCheckResult> MaterializedViewRewriter::getRelatedMaterializedViews(QueryPlan & plan, ContextMutablePtr context) const
{
    std::vector<MaterializedViewStructurePtr> materialized_views;
    auto & cache = MaterializedViewMemoryCache::instance();

    auto result = RelatedMaterializedViewsExtractor::extract(plan, context);
    for (const auto & view : result.materialized_views)
    {
        if (auto structure = cache.getMaterializedViewStructure(view, context, false, result.local_table_to_distributed_table))
            materialized_views.push_back(*structure);
    }

    for (const auto & view : result.local_materialized_views)
    {
        if (auto structure = cache.getMaterializedViewStructure(view, context, true, result.local_table_to_distributed_table))
            materialized_views.push_back(*structure);
    }

    if (log->debug())
    {
        std::stringstream ss;
        for (const auto & materialized_view : materialized_views)
            ss << materialized_view->view_storage_id.getFullTableName() << ", ";
        LOG_DEBUG(log, "related {} materialized views: {}", materialized_views.size(), ss.str());
    }


    LinkedHashMap<MaterializedViewStructurePtr, PartitionCheckResult> res;
    for (const auto & materialized_view : materialized_views)
    {
        if (auto partition_check_result = checkMaterializedViewPartitionConsistency(materialized_view, context))
            res.emplace(materialized_view, *partition_check_result);
        else
            LOG_DEBUG(log, "skip materialized view {}: check consistency failed", materialized_view->view_storage_id.getFullTableName());
    }
    return res;
}
}
