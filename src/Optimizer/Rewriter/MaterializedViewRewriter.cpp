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
#include <Core/SettingsEnums.h>
#include <Optimizer/CardinalityEstimate/TableScanEstimator.h>
#include <Optimizer/DomainTranslator.h>
#include <Optimizer/JoinGraph.h>
#include <Optimizer/MaterializedView/MaterializeViewChecker.h>
#include <Optimizer/MaterializedView/MaterializedViewMemoryCache.h>
#include <Optimizer/MaterializedView/MaterializedViewStructure.h>
#include <Optimizer/OptimizerMetrics.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/SelectQueryInfoHelper.h>
#include <Optimizer/SimpleExpressionRewriter.h>
#include <Optimizer/SymbolsExtractor.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTableColumnReference.h>
#include <QueryPlan/SimplePlanRewriter.h>
#include <QueryPlan/SimplePlanVisitor.h>
#include <QueryPlan/TableScanStep.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/StorageDistributed.h>
#include <common/logger_useful.h>
#include <Optimizer/EqualityASTMap.h>

#include <map>
#include <memory>
#include <optional>
#include <utility>
#include <fmt/core.h>
#include <fmt/format.h>

namespace DB
{
namespace ErrorCodes
{
extern const int PARAMETER_OUT_OF_BOUND;
}

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

struct TableInputRef
{
    StoragePtr storage;
    PlanNodeId unique_id;
    [[nodiscard]] String getDatabaseTableName() const {
        return storage->getStorageID().getFullTableName();
    }
    [[nodiscard]] String toString() const { return getDatabaseTableName() + "#" + std::to_string(unique_id); }
};

struct TableInputRefHash
{
    size_t operator()(const TableInputRef & ref) const { return std::hash<UInt32>()(ref.unique_id); }
};

struct TableInputRefEqual
{
    bool operator()(const TableInputRef & lhs, const TableInputRef & rhs) const
    {
        return lhs.storage == rhs.storage && lhs.unique_id == rhs.unique_id;
    }
};

struct JoinGraphMatchResult
{
    std::unordered_map<TableInputRef, std::vector<TableInputRef>, TableInputRefHash, TableInputRefEqual> query_to_view_table_mappings;
    std::vector<TableInputRef> view_missing_tables;
    std::unordered_map<String, std::shared_ptr<ASTTableColumnReference>> view_missing_columns;
};

using TableInputRefMap = std::unordered_map<TableInputRef, TableInputRef, TableInputRefHash, TableInputRefEqual>;

String toString(const TableInputRefMap & table_mapping)
{
    String str;
    for (const auto & x: table_mapping)
        str += x.first.toString() + "->" + x.second.toString() + ", ";
    return str;
}

class SwapTableInputRefRewriter : public SimpleExpressionRewriter<const TableInputRefMap>
{
public:
    static ASTPtr rewrite(ASTPtr & expression, const TableInputRefMap & table_mapping)
    {
        SwapTableInputRefRewriter rewriter;
        return ASTVisitorUtil::accept(expression, rewriter, table_mapping);
    }

    ASTPtr visitASTTableColumnReference(ASTPtr & node, const TableInputRefMap & context) override
    {
        auto & table_column_ref = node->as<ASTTableColumnReference &>();
        auto storage_ptr = (const_cast<IStorage *>(table_column_ref.storage))->shared_from_this();
        TableInputRef ref{std::move(storage_ptr), table_column_ref.unique_id};
        if (!context.contains(ref)) {
            throw Exception("table ref not exists: " + ref.toString(), ErrorCodes::LOGICAL_ERROR);
        }
        auto & replace_table_ref = context.at(ref);
        return std::make_shared<ASTTableColumnReference>(
            replace_table_ref.storage.get(), replace_table_ref.unique_id, table_column_ref.column_name);
    }
};

using ExpressionEquivalences = Equivalences<ConstASTPtr, EqualityASTMap>;
using ConstASTMap = EqualityASTMap<ConstASTPtr>;
class EquivalencesRewriter : public SimpleExpressionRewriter<const ConstASTMap>
{
public:
    static ASTPtr rewrite(ASTPtr & expression, const ExpressionEquivalences & expression_equivalences)
    {
        EquivalencesRewriter rewriter;
        const auto map = expression_equivalences.representMap();
        return ASTVisitorUtil::accept(expression, rewriter, map);
    }

    ASTPtr visitNode(ASTPtr & node, const ConstASTMap & context) override
    {
        auto it = context.find(node);
        if (it != context.end())
            return it->second->clone();
        return SimpleExpressionRewriter::visitNode(node, context);
    }
};

class ColumnReferenceRewriter : public SimpleExpressionRewriter<const Void>
{
public:
    ASTPtr visitASTTableColumnReference(ASTPtr & node, const Void &) override
    {
        auto & column_ref = node->as<ASTTableColumnReference &>();
        if (!belonging.has_value())
            belonging = column_ref.unique_id;
        else if (*belonging != column_ref.unique_id)
            belonging = std::nullopt;
        return std::make_shared<ASTIdentifier>(column_ref.column_name);
    }

    std::optional<UInt32> belonging;
};

std::tuple<std::optional<UInt32>, ASTPtr>
rewritePredicateForPartitionCheck(const ConstASTPtr & expression, const SymbolTransformMap & query_transform_map)
{
    ColumnReferenceRewriter rewriter;
    auto rewritten = ASTVisitorUtil::accept(query_transform_map.inlineReferences(expression), rewriter, {});
    return {rewriter.belonging, rewritten};
}

/// Return whether the first is a subset of the second
bool subset(const std::unordered_set<String> & first, const std::unordered_set<String> & second)
{
    if (first.empty() && !second.empty())
        return false;

    for (const auto & element : first)
    {
        if (!second.count(element))
            return false;
    }

    return true;
}

bool checkMVConsistencyByPartition(
    const std::vector<PlanNodePtr> query_table_scans,
    const ConstASTs & query_predicates,
    const SymbolTransformMap & query_transform_map,
    const StorageID & mv_target_table,
    ContextPtr context,
    const std::function<void(const String &)> & add_failure_message,
    Poco::Logger * logger)
{
    // inline query predicates & determine belonging base table
    std::unordered_map<UInt32, ASTs> predicates_by_table;
    for (const auto & pred : query_predicates)
    {
        auto [node_id, new_pred] = rewritePredicateForPartitionCheck(pred, query_transform_map);
        LOG_DEBUG(
            logger,
            "mv check partition: inline query predicate {} to origin nodeId {}, belonging table: {}",
            serializeAST(*pred),
            serializeAST(*new_pred),
            node_id.has_value() ? std::to_string(*node_id) : "?");
        if (node_id.has_value())
            predicates_by_table[*node_id].emplace_back(new_pred);
    }

    // check base table partition schema & collect query required partitions
    // TODO: now the partition schema check is by checking key size, it's better to also check key's expression
    std::unordered_set<String> required_part_name_set;
    std::optional<DataTypes> first_partition_key_types;
    String first_partition_table;
    auto check_partition_schema = [&](const auto & key_description, const auto & storage_id) -> bool {
        // skip non-partitioned table
        if (key_description.data_types.empty())
            return true;

        if (!first_partition_key_types)
        {
            first_partition_key_types = key_description.data_types;
            first_partition_table = storage_id.getFullTableName();
            return true;
        }

        const auto & partition_key_types = key_description.data_types;
        return partition_key_types.size() == first_partition_key_types->size();
    };

    for (const auto & table_scan : query_table_scans)
    {
        const auto * table_step = dynamic_cast<const TableScanStep *>(table_scan->getStep().get());
        if (!table_step)
            continue;

        auto storage = DatabaseCatalog::instance().getTable(table_step->getStorageID(), context);
        auto * merge_tree = dynamic_cast<StorageCnchMergeTree *>(storage.get());
        if (!merge_tree || merge_tree->getSettings()->disable_block_output)
            continue;

        auto storage_metadata = storage->getInMemoryMetadataPtr();
        const auto & key_description = storage_metadata->getPartitionKey();
        // ignore non-partitioned tables
        if (key_description.data_types.empty())
            continue;

        if (!check_partition_schema(key_description, storage->getStorageID()))
        {
            add_failure_message(
                "table " + storage->getStorageID().getFullTableName() + " has a inconsistent partition schema with table "
                + first_partition_table);
            return false;
        }

        auto select_query = std::make_shared<ASTSelectQuery>();
        auto select_expr = std::make_shared<ASTExpressionList>();
        Names column_names_to_return;
        for (const auto & col_name : table_step->getColumnNames())
        {
            select_expr->children.push_back(std::make_shared<ASTIdentifier>(col_name));
            column_names_to_return.push_back(col_name);
        }
        select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expr));
        select_query->replaceDatabaseAndTable(storage->getStorageID());
        auto & table_preds = predicates_by_table[table_scan->getId()];
        if (!table_preds.empty())
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, PredicateUtils::combineConjuncts(table_preds));

        SelectQueryInfo query_info = buildSelectQueryInfoForQuery(select_query, context);
        auto required_partitions = merge_tree->getPrunedPartitions(query_info, column_names_to_return, context).partitions;
        std::unordered_set<String> required_part_name_set_for_this_table;
        for (const auto & part : required_partitions)
        {
            required_part_name_set_for_this_table.emplace(part);
        }

        LOG_DEBUG(
            logger,
            "Get table {} required partition set: {} by query {}",
            storage->getStorageID().getFullTableName(),
            fmt::join(required_part_name_set_for_this_table, ", "),
            serializeAST(*select_query));

        required_part_name_set.insert(required_part_name_set_for_this_table.begin(), required_part_name_set_for_this_table.end());
    }

    if (required_part_name_set.empty())
        return true;

    // check mv partition schema
    auto mv_target_storage = DatabaseCatalog::instance().getTable(mv_target_table, context);
    const auto * mv_target_merge_tree = dynamic_cast<StorageCnchMergeTree *>(mv_target_storage.get());
    if (!mv_target_merge_tree)
        return true;

    auto mv_target_storage_metadata = mv_target_storage->getInMemoryMetadataPtr();
    if (!check_partition_schema(mv_target_storage_metadata->getPartitionKey(), mv_target_table))
    {
        add_failure_message(
            "mv target table " + mv_target_table.getFullTableName() + " has a inconsistent partition schema with table "
            + first_partition_table);
        return false;
    }

    // check mv partitions
    std::unordered_set<String> view_part_name_set;
    if (context->getCnchCatalog())
    {
        auto view_partitions = context->getCnchCatalog()->getPartitionList(mv_target_storage, context.get());
        MergeTreeMetaBase * view_tree_base = dynamic_cast<MergeTreeMetaBase *>(mv_target_storage.get());
        for (const auto & partition : view_partitions)
            view_part_name_set.emplace(partition->getID(*view_tree_base));
    }

    LOG_DEBUG(
        logger,
        "Get mv target table {} owned partition set: {}",
        mv_target_storage->getStorageID().getFullTableName(),
        fmt::join(view_part_name_set, ", "));

    if (!subset(required_part_name_set, view_part_name_set))
    {
        String required_partitions_str;
        for (const auto & require_partition : required_part_name_set)
            required_partitions_str.append(require_partition).append(",");
        String owned_partitions_str;
        for (const auto & own_partition : view_part_name_set)
            owned_partitions_str.append(own_partition).append(",");
        add_failure_message(
            "checkPartitions - require partitions-" + required_partitions_str + " owned partitions-" + owned_partitions_str
            + " match fail.");
        return false;
    }

    return true;
}

class ExtractResult
{
public:
    std::map<String, std::vector<StorageID>> table_based_materialized_views;
    std::map<String, std::vector<StorageID>> table_based_local_materialized_views;
    std::map<String, StorageID> local_table_to_distributed_table;

    ExtractResult(
        const std::map<String, std::vector<StorageID>> & tableBasedMaterializedViews,
        const std::map<String, std::vector<StorageID>> & tableBasedLocalMaterializedViews,
        const std::map<String, StorageID> & localTableToDistributedTable)
        : table_based_materialized_views(tableBasedMaterializedViews)
        , table_based_local_materialized_views(tableBasedLocalMaterializedViews)
        , local_table_to_distributed_table(localTableToDistributedTable)
    {
    }
};

/**
 * Extract materialized views related into the query tables.
 */
class RelatedMaterializedViewExtractor : public SimplePlanVisitor<Void>
{
public:
    static ExtractResult
    extract(QueryPlan & plan, ContextMutablePtr context_)
    {
        Void c;
        RelatedMaterializedViewExtractor finder{context_, plan.getCTEInfo()};
        VisitorUtil::accept(plan.getPlanNode(), finder, c);
        return ExtractResult{
            finder.table_based_materialized_views,
            finder.table_based_local_materialized_views,
            finder.local_table_to_distributed_table};
    }

protected:
    RelatedMaterializedViewExtractor(ContextMutablePtr context_, CTEInfo & cte_info_) : SimplePlanVisitor(cte_info_), context(context_) { }

    Void visitTableScanNode(TableScanNode & node, Void &) override
    {
        auto table_scan = dynamic_cast<const TableScanStep *>(node.getStep().get());
        auto catalog_client = context->tryGetCnchCatalog();
        if (catalog_client)
        {
            auto start_time = context->getTimestamp();
            auto views = catalog_client->getAllViewsOn(*context, table_scan->getStorage(), start_time);
            if (!views.empty())
            {
                Dependencies dependencies;
                for (const auto & view : views)
                    dependencies.emplace_back(view->getStorageID());
                table_based_materialized_views.emplace(table_scan->getStorageID().getFullTableName(), std::move(dependencies));
            }
        }
        else
        {
            auto dependencies = DatabaseCatalog::instance().getDependencies(table_scan->getStorageID());
            if (!dependencies.empty())
                table_based_materialized_views.emplace(table_scan->getStorageID().getFullTableName(), std::move(dependencies));
        }
        return Void{};
    }

private:
    ContextMutablePtr context;
    std::map<String, std::vector<StorageID>> table_based_materialized_views;
    std::map<String, std::vector<StorageID>> table_based_local_materialized_views;
    std::map<String, StorageID> local_table_to_distributed_table;
};

/**
 * tables that children contains, empty if there are unsupported plan nodes
 */
struct BaseTables
{
    std::vector<StorageID> base_tables;
};

/**
 * Top-down match whether any node in query can be rewrite using materialized view.
 */
class CandidatesExplorer : public PlanNodeVisitor<BaseTables, Void>
{
public:
    static std::unordered_map<PlanNodePtr, RewriterCandidates> explore(
        QueryPlan & query,
        ContextMutablePtr context,
        const std::map<String, std::vector<MaterializedViewStructurePtr>> & table_based_materialized_views,
        bool verbose)
    {
        CandidatesExplorer explorer{context, table_based_materialized_views, verbose};
        Void c;
        VisitorUtil::accept(query.getPlanNode(), explorer, c);

        if (verbose)
        {
            static auto * log = &Poco::Logger::get("CandidatesExplorer");
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
        ContextMutablePtr context_,
        const std::map<String, std::vector<MaterializedViewStructurePtr>> & table_based_materialized_views_,
        bool verbose_)
        : context(context_), table_based_materialized_views(table_based_materialized_views_), verbose(verbose_)
    {
    }

    BaseTables visitPlanNode(PlanNodeBase & node, Void &) override { return process(node); }

    BaseTables visitTableScanNode(TableScanNode & node, Void &) override
    {
        auto step = dynamic_cast<const TableScanStep *>(node.getStep().get());
        BaseTables res;
        res.base_tables.emplace_back(StorageID{step->getDatabase(), step->getTable()});
        return res;
    }

    BaseTables process(PlanNodeBase & node)
    {
        bool supported = MaterializedViewStepChecker::isSupported(node.getStep(), context);

        std::vector<StorageID> base_tables;
        for (auto & child : node.getChildren())
        {
            Void c;
            auto res = VisitorUtil::accept(child, *this, c);
            if (res.base_tables.empty())
                supported = false;
            else
                base_tables.insert(base_tables.end(), res.base_tables.begin(), res.base_tables.end());
        }
        if (!supported)
            return {};

        matches(node, base_tables);
        return BaseTables{.base_tables = base_tables};
    }

    /**
     * matches plan node using all related materialized view.
     */
    void matches(PlanNodeBase & query, const std::vector<StorageID> & tables)
    {
        std::vector<MaterializedViewStructurePtr> related_materialized_views;
        for (const auto & table : tables)
            if (auto it = table_based_materialized_views.find(table.getFullTableName()); it != table_based_materialized_views.end())
                related_materialized_views.insert(related_materialized_views.end(), it->second.begin(), it->second.end());

        if (related_materialized_views.empty())
            return;

        std::vector<ConstASTPtr> query_other_predicates;
        std::shared_ptr<const AggregatingStep> query_aggregate;

        JoinGraph query_join_graph = JoinGraph::build(query.shared_from_this(), context, true, false, true);
        if (query_join_graph.getNodes().size() == 1 &&
            query_join_graph.getNodes()[0]->getStep()->getType() == IQueryPlanStep::Type::Aggregating) {

            auto having_predicates = query_join_graph.getFilters();
            query_other_predicates.insert(query_other_predicates.end(), having_predicates.begin(), having_predicates.end());
            query_aggregate = dynamic_pointer_cast<const AggregatingStep>(query_join_graph.getNodes()[0]->getStep());
            query_join_graph = JoinGraph::build(query_join_graph.getNodes()[0]->getChildren()[0], context, true, false, true);
        }

        std::optional<SymbolTransformMap> query_map; // lazy initialization later
        std::optional<NameToType> query_symbol_types; // lazy initialization later

        for (const auto & view : related_materialized_views)
            if (auto result = match(
                    query,
                    query_join_graph,
                    query_other_predicates,
                    query_map,
                    query_aggregate,
                    query_symbol_types,
                    view->join_graph,
                    view->other_predicates,
                    view->symbol_map,
                    view->output_columns,
                    view->output_columns_to_table_columns_map,
                    view->expression_equivalences,
                    view->top_aggregating_step,
                    view->symbol_types,
                    view->view_storage_id,
                    view->target_storage_id))
                candidates[query.shared_from_this()].emplace_back(*result);
    }

    /**
     * main logic for materialized view based query rewriter.
     */
    std::optional<RewriterCandidate> match(
        PlanNodeBase & query,
        const JoinGraph & query_join_graph,
        const std::vector<ConstASTPtr> & query_other_predicates,
        std::optional<SymbolTransformMap> & query_map,
        const std::shared_ptr<const AggregatingStep> & query_aggregate,
        std::optional<NameToType> query_symbol_types,
        const JoinGraph & view_join_graph,
        const std::vector<ConstASTPtr> & view_other_predicates,
        const SymbolTransformMap & view_map,
        const std::unordered_set<String> & view_outputs,
        const std::unordered_map<String, String> & view_outputs_to_table_columns_map,
        const ExpressionEquivalences & view_equivalences,
        const std::shared_ptr<const AggregatingStep> & view_aggregate,
        const NameToType view_symbol_types,
        const StorageID view_storage_id,
        const StorageID target_storage_id)
    {
        auto add_failure_message = [&](const String & message) {
            if (verbose)
                failure_messages[query.shared_from_this()].emplace_back(
                    RewriterFailureMessage{view_storage_id, message});
        };

        // 0. pre-check

        // 1. check join
        auto join_graph_match_result = matchJoinGraph(query_join_graph, view_join_graph, context);
        if (!join_graph_match_result)
        {
            add_failure_message("join graph rewrite fail.");
            return {};
        }

        if (!query_symbol_types)
        {
            query_symbol_types = Utils::extractNameToType(query);
            // actually we should skip all subsequent matches for this plan node
            if (!query_symbol_types)
            {
                add_failure_message("query has ambiguous names.");
                return {};
            }
        }

        // get all predicates from join graph
        auto query_predicates = PredicateUtils::extractEqualPredicates(query_join_graph.getFilters());
        for (const auto & predicate : query_other_predicates)
            query_predicates.second.emplace_back(predicate);
        auto view_predicates = PredicateUtils::extractEqualPredicates(view_join_graph.getFilters());
        for (const auto & predicate : view_other_predicates)
            view_predicates.second.emplace_back(predicate);

        if (!query_map)
        {
            query_map = SymbolTransformMap::buildFrom(query); // lazy initialization here.
            if (!query_map)
            {
                add_failure_message("query transform map invalid, maybe has duplicate symbols");
                return {};
            }
        }

        // 1-2. generate table mapping from join graph
        // If a table is used multiple times, we will create multiple mappings,
        // and we will try to rewrite the query using each of the mappings.
        // Then, we will try to map every source table (query) to a target table (view).
        for (auto & query_to_view_table_mappings : generateFlatTableMappings(join_graph_match_result->query_to_view_table_mappings))
        {
            // 2. check where(predicates)
            std::vector<ConstASTPtr> compensation_predicates;
            ExpressionEquivalences view_based_query_equivalences_map;

            // 2-1. equal-predicates
            // Rewrite query equal predicates to view-based and build new equivalences map. Then check whether
            // these query equal-predicates exists in view equivalences, otherwise, construct missing predicate for view.
            for (const auto & predicate : query_predicates.first)
            {
                auto left_symbol_lineage = normalizeExpression(predicate.first, *query_map, query_to_view_table_mappings);
                auto right_symbol_lineage = normalizeExpression(predicate.second, *query_map, query_to_view_table_mappings);
                view_based_query_equivalences_map.add(left_symbol_lineage, right_symbol_lineage);

                if (!view_equivalences.isEqual(left_symbol_lineage, right_symbol_lineage))
                    compensation_predicates.emplace_back(makeASTFunction("equals", left_symbol_lineage, right_symbol_lineage));
            }

            // check whether view equal-predicates exists in query equivalences, otherwise, give up.
            bool has_missing_predicate = false;
            for (const auto & predicate : view_predicates.first)
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

            ASTPtr query_other_predicate;
            ASTPtr view_other_predicate;

            // 2-2. range-predicates(optional)
            //// Currently range predicate matching is not perfect, as TupleDomain can not normalize OR filters.
            //// See also MV unit test case: MaterializedViewRewriteTest.testFilterQueryOnFilterView3
            if (context->getSettingsRef().enable_materialized_view_rewrite_match_range_filter)
            {
                using namespace DB::Predicate;
                DomainTranslator<ASTPtr> domain_translator(context);
                auto query_extraction_result
                    = domain_translator.getExtractionResult(PredicateUtils::combineConjuncts(query_predicates.second), *query_symbol_types);
                auto view_extraction_result
                    = domain_translator.getExtractionResult(PredicateUtils::combineConjuncts(view_predicates.second), view_symbol_types);

                TupleDomain<ASTPtr> & query_domain_result = query_extraction_result.tuple_domain;
                TupleDomain<ASTPtr> & view_domain_result = view_extraction_result.tuple_domain;

                TupleDomain<ASTPtr> view_based_query_domain = query_domain_result.mapKey<TupleDomain<ASTPtr>>([&](const ASTPtr & ast) {
                    auto res = normalizeExpression(ast, *query_map, query_to_view_table_mappings);
                    return res;
                });

                TupleDomain<ASTPtr> view_domain = view_domain_result.mapKey<TupleDomain<ASTPtr>>([&](const ASTPtr & ast) {
                    auto res = normalizeExpression(ast, view_map);
                    return res;
                });

                if (!view_domain.contains(view_based_query_domain))
                {
                    add_failure_message("range predicates rewrite fail.");
                    continue; // bail out
                }

                if (view_domain != view_based_query_domain)
                {
                    compensation_predicates.emplace_back(domain_translator.toPredicate(view_based_query_domain));
                }

                query_other_predicate = query_extraction_result.remaining_expression;
                view_other_predicate = view_extraction_result.remaining_expression;
            }
            else
            {
                query_other_predicate = PredicateUtils::combineConjuncts(query_predicates.second);
                view_other_predicate = PredicateUtils::combineConjuncts(view_predicates.second);
            }

            // 2-3. other-predicates
            // compensation_predicates.emplace_back(PredicateUtils.splitPredicates(
            //     query_domain_result.getOtherPredicates(), view_domain_result.getOtherPredicates()));
            auto view_based_query_predicates
                = normalizeExpression(query_other_predicate, *query_map, query_to_view_table_mappings, view_based_query_equivalences_map);
            auto other_compensation_predicate = PredicateUtils::splitPredicates(
                view_based_query_predicates, normalizeExpression(view_other_predicate, view_map, view_based_query_equivalences_map));
            if (!other_compensation_predicate)
            {
                add_failure_message("other-predicates rewrite fail.");
                continue; // bail out
            }
            compensation_predicates.emplace_back(other_compensation_predicate);

            auto compensation_predicate = PredicateUtils::combineConjuncts(compensation_predicates);
            if (PredicateUtils::isFalsePredicate(compensation_predicate))
            {
                add_failure_message("compensation predicates rewrite fail.");
                continue; // bail out
            }

            // 3. check need rollup
            // Note: aggregate always need rollup for aggregating merge tree in clickhouse,
            //  but code here is not removed for future features.
            bool need_rollup = query_aggregate.get();
            if (view_aggregate.get() && !query_aggregate.get()) {
                continue;
            }

            // 3-1. query aggregate has default result if group by has empty set. not supported yet.
            bool empty_groupings = query_aggregate && query_aggregate->getKeys().empty() &&
                                   view_aggregate && !view_aggregate->getKeys().empty();
            if (empty_groupings && !context->getSettingsRef().enable_materialized_view_empty_grouping_rewriting) {
                continue;
            }
            auto default_value_provider = getAggregateDefaultValueProvider(query_aggregate, context);

            // 3-2. having can't rewrite if aggregate need rollup.
            if (need_rollup && !query_other_predicates.empty())
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
            for (const auto & missing_table_column : join_graph_match_result->view_missing_columns)
                view_output_columns_map.emplace(
                    normalizeExpression(
                        missing_table_column.second, *query_map, query_to_view_table_mappings, view_based_query_equivalences_map),
                    makeASTIdentifier(missing_table_column.first));

            // 5-3. check select columns (contains aggregates)
            NameSet required_columns_set;
            Assignments assignments;
            NameToType name_to_type;
            bool enforce_agg_node = false;
            for (const auto & name_and_type : query.getCurrentDataStream().header)
            {
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
                    add_failure_message("output column `" + output_name + "` rewrite fail.");
                    break; // bail out
                }
                auto columns = SymbolsExtractor::extract(*rewrite);
                required_columns_set.insert(columns.begin(), columns.end());

                assignments.emplace_back(Assignment{output_name, *rewrite});
                name_to_type.emplace(output_name, name_and_type.type);

                enforce_agg_node = enforce_agg_node || Utils::containsAggregateFunction(*rewrite);
            }
            if (assignments.size() != query.getCurrentDataStream().header.columns())
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
                add_failure_message("compensation predicate rewrite fail.");
                continue;
            }
            auto columns = SymbolsExtractor::extract(*rewrite_compensation_predicate);
            required_columns_set.insert(columns.begin(), columns.end());

            // 5. check data consistency
            auto consistency_check_method = context->getSettingsRef().materialized_view_consistency_check_method;
            if (consistency_check_method == MaterializedViewConsistencyCheckMethod::PARTITION)
            {
                if (!checkMVConsistencyByPartition(
                    query_join_graph.getNodes(),
                    query_join_graph.getFilters(),
                    *query_map,
                    target_storage_id,
                    context,
                    add_failure_message,
                    &Poco::Logger::get("CandidatesExplorer")))
                {
                    continue; // bail out
                }
            }

            // 6. construct candidate
            auto it_stats = materialized_views_stats.find(target_storage_id.getFullTableName());
            if (it_stats == materialized_views_stats.end())
            {
                auto stats = TableScanEstimator::estimate(context, target_storage_id);
                it_stats = materialized_views_stats.emplace(target_storage_id.getFullTableName(), stats).first;
            }

            auto storage = DatabaseCatalog::instance().getTable(target_storage_id, context);
            auto metadata_snapshot = storage->getInMemoryMetadataPtr();
            const auto & ordered_columns = metadata_snapshot->sorting_key.column_names;
            bool contains_ordered_columns = !ordered_columns.empty() && columns.count(ordered_columns.front());

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

            // 6. other query info
            // 6-1. keep prewhere info for single table rewriting
            ASTPtr prewhere_expr;
            if (query_join_graph.getNodes().size() == 1)
            {
                const auto * table_scan = dynamic_cast<const TableScanStep *>(query_join_graph.getNodes().at(0)->getStep().get());
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
                            prewhere_expr = *rewritten_prewhere;
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
                rollup_keys};
        }
        // no matches
        return {};
    }

    static void logNormalizeExpression(const ConstASTPtr & expression,
                                       const SymbolTransformMap & symbol_transform_map,
                                       const TableInputRefMap & table_mapping)
    {
        static auto log = &Poco::Logger::get("MaterializedViewRewriter");
        LOG_TRACE(log, "normalize expression, input expression: " + serializeAST(*expression)
                       + "\nsymbol transform map: " + symbol_transform_map.toString()
                       + "\ntable mapping: " + toString(table_mapping));
    }

    /**
     * First, it rewrite identifiers in expression recursively, get lineage-form expression.
     * Then, it swaps the all TableInputRefs using table_mapping.
     * Finally, it rewrite equivalent expressions to unified expression.
     *
     * <p> it is used to rewrite expression from query to view-based uniform expression.
     */
    static ASTPtr normalizeExpression(
        const ConstASTPtr & expression,
        const SymbolTransformMap & symbol_transform_map,
        const TableInputRefMap & table_mapping,
        const ExpressionEquivalences & expression_equivalences)
    {
        logNormalizeExpression(expression, symbol_transform_map, table_mapping);
        auto lineage = symbol_transform_map.inlineReferences(expression);
        lineage = SwapTableInputRefRewriter::rewrite(lineage, table_mapping);
        lineage = EquivalencesRewriter::rewrite(lineage, expression_equivalences);
        return lineage;
    }

    /**
     * it is used to rewrite expression from view to uniform expression.
     */
    static ASTPtr normalizeExpression(
        const ConstASTPtr & expression,
        const SymbolTransformMap & symbol_transform_map,
        const ExpressionEquivalences & expression_equivalences)
    {
        auto lineage = symbol_transform_map.inlineReferences(expression);
        lineage = EquivalencesRewriter::rewrite(lineage, expression_equivalences);
        return lineage;
    }

    static ASTPtr normalizeExpression(const ConstASTPtr & expression, const SymbolTransformMap & symbol_transform_map)
    {
        auto lineage = symbol_transform_map.inlineReferences(expression);
        return lineage;
    }

    static ASTPtr normalizeExpression(
        const ConstASTPtr & expression, const SymbolTransformMap & symbol_transform_map, const TableInputRefMap & table_mapping)
    {
        logNormalizeExpression(expression, symbol_transform_map, table_mapping);
        auto lineage = symbol_transform_map.inlineReferences(expression);
        lineage = SwapTableInputRefRewriter::rewrite(lineage, table_mapping);
        return lineage;
    }

    static inline ASTPtr makeASTIdentifier(const String & name) { return std::make_shared<ASTIdentifier>(name); }

    /**
     * It will flatten a multimap containing table references to table references, producing all possible combinations of mappings.
     */
    static std::vector<std::unordered_map<TableInputRef, TableInputRef, TableInputRefHash, TableInputRefEqual>> generateFlatTableMappings(
        const std::unordered_map<TableInputRef, std::vector<TableInputRef>, TableInputRefHash, TableInputRefEqual> & table_multi_mapping)
    {
        if (table_multi_mapping.empty())
            return {};

        std::vector<std::unordered_map<TableInputRef, TableInputRef, TableInputRefHash, TableInputRefEqual>> table_mappings;
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
                std::vector<std::unordered_map<TableInputRef, TableInputRef, TableInputRefHash, TableInputRefEqual>> new_table_mappings;
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
     * We try to extract table reference and check whether query can be computed from view.
     * if not, try to compensate, e.g., for join queries it might be possible to join missing tables
     * with view to compute result.
     *
     * <p> supported:
     * - Self join
     * - view tables are subset of query tables (add additional tables through joins if possible)
     *
     * <p> todo:
     * - query tables are subset of view tables (we need to check whether they are cardinality-preserving joins)
     * - outer join
     * - semi join and anti join
     */
    static std::optional<JoinGraphMatchResult>
    matchJoinGraph(const JoinGraph & query_graph, const JoinGraph & view_graph, ContextMutablePtr context)
    {
        if (query_graph.isEmpty())
            return {}; // bail out, if there is no tables.
        const auto & query_nodes = query_graph.getNodes();
        const auto & view_nodes = view_graph.getNodes();

        if (!context->getSettingsRef().enable_materialized_view_join_rewriting && query_nodes.size() > 1) {
            return {};
        }

        if (query_nodes.size() < view_nodes.size())
            return {}; // bail out, if some tables are missing in the query

        auto extract_table_ref = [](PlanNodeBase & node) -> std::optional<TableInputRef> {
            if (const auto * table_step = dynamic_cast<const TableScanStep *>(node.getStep().get()))
                return TableInputRef{table_step->getStorage(), node.getId()};
            return {};
        };

        std::unordered_map<String, std::vector<std::pair<PlanNodePtr, TableInputRef>>> query_table_map;
        for (const auto & node : query_nodes)
        {
            auto table_input_ref = extract_table_ref(*node);
            if (!table_input_ref)
                return {}; // bail out
            query_table_map[table_input_ref->getDatabaseTableName()].emplace_back(node, *table_input_ref);
        }

        std::unordered_map<String, std::vector<TableInputRef>> view_table_map;
        for (const auto & node : view_nodes)
        {
            auto table_input_ref = extract_table_ref(*node);
            if (!table_input_ref)
                return {}; // bail out
            view_table_map[table_input_ref->getDatabaseTableName()].emplace_back(*table_input_ref);
        }

        bool is_query_missing_table = std::any_of(view_table_map.begin(), view_table_map.end(), [&](const auto & item) {
            return item.second.size() > query_table_map[item.first].size();
        });
        if (is_query_missing_table)
            return {}; // bail out, if some tables are missing in the query

        std::unordered_map<TableInputRef, std::vector<TableInputRef>, TableInputRefHash, TableInputRefEqual> table_mapping;
        std::vector<TableInputRef> view_missing_tables;
        std::unordered_map<String, std::shared_ptr<ASTTableColumnReference>> view_missing_columns;
        for (auto & item : query_table_map)
        {
            auto & query_table_refs = item.second;
            auto view_table_refs = view_table_map[item.first];

            // not supported yet
            if (query_table_refs.size() != view_table_refs.size())
                return {};

            for (auto & query_table_ref : query_table_refs)
                table_mapping[query_table_ref.second] = view_table_refs;
        }

        return JoinGraphMatchResult{table_mapping, view_missing_tables, view_missing_columns};
    }

    using AggregateDefaultValueProvider = std::function<std::optional<Field>(const ASTFunction &)>;
    static AggregateDefaultValueProvider getAggregateDefaultValueProvider(
        const std::shared_ptr<const AggregatingStep> & query_step, ContextMutablePtr context)
    {
        return [&, context](const ASTFunction & aggregate_ast_function) -> std::optional<Field> {
            if (!query_step)
                return {};
            const auto & input_types = query_step->getInputStreams()[0].header.getNamesAndTypes();
            DataTypes agg_argument_types;
            if (aggregate_ast_function.arguments)
            {
                for (auto & argument : aggregate_ast_function.arguments->children)
                {
                    auto type = TypeAnalyzer::getType(argument, context, input_types);
                    agg_argument_types.emplace_back(type);
                }
            }

            Array parameters;
            if (aggregate_ast_function.parameters)
            {
                for (auto & argument : aggregate_ast_function.parameters->children)
                    parameters.emplace_back(argument->as<ASTLiteral &>().value);
            }

            AggregateFunctionProperties properties;
            auto aggregate_function = AggregateFunctionFactory::instance().tryGet(
                aggregate_ast_function.name, agg_argument_types, parameters, properties);
            if (!aggregate_function) {
                return {};
            }

            AlignedBuffer place_buffer(aggregate_function->sizeOfData(), aggregate_function->alignOfData());
            AggregateDataPtr place = place_buffer.data();
            aggregate_function->create(place);

            auto column = aggregate_function->getReturnType()->createColumn();
            auto arena = std::make_unique<Arena>();
            aggregate_function->insertResultInto(place, *column, arena.get());
            Field default_value;
            column->get(0, default_value);
            return default_value;
        };
    }

    static std::string getFunctionName(const ASTPtr & rewrite) {
        return rewrite->as<ASTFunction>()->name;
    }

    /**
     * if query is aggregate with empty groupings,
     * rewrite result is not aggregate or aggregate changed (eg. rollup),
     * we need add coalesce to assign default value from origin aggregate function for empty result.
     */
    static std::optional<ASTPtr> rewriteEmptyGroupings(
        const ASTPtr & rewrite,
        const ASTFunction & original_aggregate_function, AggregateDefaultValueProvider & default_value_provider) {
        if (rewrite->getType() == ASTType::ASTFunction
            && getFunctionName(rewrite) == original_aggregate_function.name) {
            return rewrite;
        }

        ASTPtr any_aggregate_function;
        if (rewrite->getType() == ASTType::ASTFunction
            && AggregateFunctionFactory::instance().isAggregateFunctionName(getFunctionName(rewrite))) {
            any_aggregate_function = rewrite;
        }  else {
            any_aggregate_function = makeASTFunction("any", rewrite);
        }
        auto default_value = default_value_provider(original_aggregate_function);
        if (!default_value)
            return {};
        return std::make_optional(makeASTFunction("coalesce", any_aggregate_function, std::make_shared<ASTLiteral>(*default_value)));
    }

    /**
     * It rewrite input expression using output column.
     * If any of the expressions in the input expression cannot be mapped, it will return null.
     *
     * <p> Compared with rewriteExpression, it supports expression contains aggregates, and do rollup rewrite.
     */
    static std::optional<ASTPtr> rewriteExpressionContainsAggregates(
        ASTPtr expression,
        const EqualityASTMap<ConstASTPtr> & view_output_columns_map,
        const std::unordered_set<String> & output_columns,
        bool view_contains_aggregates,
        bool need_rollup,
        bool empty_groupings,
        AggregateDefaultValueProvider & default_value_provider)
    {
        class ExpressionWithAggregateRewriter : public ASTVisitor<std::optional<ASTPtr>, Void>
        {
        public:
            ExpressionWithAggregateRewriter(
                const EqualityASTMap<ConstASTPtr> & view_output_columns_map_,
                const std::unordered_set<String> & output_columns_,
                bool view_contains_aggregates_,
                bool need_rollup_,
                bool empty_groupings_,
                AggregateDefaultValueProvider & default_value_provider_)
                : view_output_columns_map(view_output_columns_map_)
                , output_columns(output_columns_)
                , view_contains_aggregates(view_contains_aggregates_)
                , need_rollup(need_rollup_)
                , empty_groupings(empty_groupings_)
                , default_value_provider(default_value_provider_)
            {
            }

            std::optional<ASTPtr> visitNode(ASTPtr & node, Void & c) override
            {
                if (!need_rollup || !Utils::containsAggregateFunction(node)) {
                    if (view_output_columns_map.contains(node))
                    {
                        auto & result = view_output_columns_map.at(node);
                        return result->clone();
                    }
                }

                for (auto & child : node->children)
                {
                    auto result = ASTVisitorUtil::accept(child, *this, c);
                    if (!result)
                        return {};
                    if (*result != child)
                        child = std::move(*result);
                }
                return node;
            }

            std::optional<ASTPtr> visitASTFunction(ASTPtr & node, Void & c) override
            {
                auto * function = node->as<ASTFunction>();
                if (!AggregateFunctionFactory::instance().isAggregateFunctionName(function->name))
                    return visitNode(node, c);

                // 0. view is not aggregate
                if (!view_contains_aggregates) {
                    return rewriteExpression(node, view_output_columns_map, output_columns, true);
                }

                // try to rewrite the expression with the following rules:
                // 1. state aggregate support rollup directly.
                if (!function->name.ends_with("State"))
                {
                    auto state_function = function->clone();
                    if (function->name.ends_with("Merge")) {
                        function->name = function->name.substr(0, function->name.size() - 5);
                    }
                    state_function->as<ASTFunction &>().name = function->name + "State";
                    auto rewrite_expression = rewriteExpression(state_function, view_output_columns_map, output_columns);
                    if (rewrite_expression)
                    {
                        auto rewritten_function = std::make_shared<ASTFunction>();
                        rewritten_function->name = need_rollup ? (function->name + "Merge") : ("finalizeAggregation");
                        rewritten_function->arguments = std::make_shared<ASTExpressionList>();
                        rewritten_function->children.emplace_back(rewritten_function->arguments);
                        rewritten_function->arguments->children.push_back(*rewrite_expression);
                        rewritten_function->parameters = function->parameters;
                        return rewritten_function;
                    }
                }

                // 2. rewrite with direct match
                auto rewrite = rewriteExpression(node, view_output_columns_map, output_columns, true);
                if (!rewrite)
                    return {};
                if (isAggregateFunction(*rewrite))
                {
                    // special case, some aggregate functions allow to be calculated from group by keys results:
                    //  MV:      select empid from emps group by empid
                    //  Query:   select max(empid) from emps group by empid
                    //  rewrite: select max(empid) from emps group by empid
                    if (!canRollupOnGroupByResults(rewrite.value()->as<const ASTFunction>()->name))
                        return {};
                    return rewrite;
                } else {
                    if (need_rollup)
                        rewrite = getRollupAggregate(function, *rewrite);
                    if (!rewrite)
                        return {};
                    if (empty_groupings)
                        rewrite = rewriteEmptyGroupings(*rewrite, *function, default_value_provider);
                    return rewrite;
                }
            }

            static std::optional<ASTPtr> getRollupAggregate(const ASTFunction * original_function, const ASTPtr & rewrite) {
                String rollup_aggregate_name = getRollupAggregateName(original_function->name);
                if (rollup_aggregate_name.empty())
                    return {};
                auto rollup_function = std::make_shared<ASTFunction>();
                rollup_function->name = std::move(rollup_aggregate_name);
                rollup_function->arguments = std::make_shared<ASTExpressionList>();
                rollup_function->arguments->children.emplace_back(rewrite);
                rollup_function->parameters = original_function->parameters;
                rollup_function->children.emplace_back(rollup_function->arguments);
                return rollup_function;
            }

            static bool isAggregateFunction(const ASTPtr & ast)
            {
                if (const auto * function = ast->as<const ASTFunction>())
                    if (AggregateFunctionFactory::instance().isAggregateFunctionName(function->name))
                        return true;
                return false;
            }

            static String getRollupAggregateName(const String & name)
            {
                if (name == "count")
                    return "sum";
                else if (name == "min" || name == "max" || name == "sum")
                    return name;
                return {};
            }

            static bool canRollupOnGroupByResults(const String & name)
            {
                if (name.ends_with("distinct") || name.starts_with("uniq") ||name == "min" || name == "max")
                    return true;
                else
                    return false;
            }

            const EqualityASTMap<ConstASTPtr> & view_output_columns_map;
            const std::unordered_set<String> & output_columns;
            bool view_contains_aggregates;
            bool need_rollup;
            bool empty_groupings;
            AggregateDefaultValueProvider & default_value_provider;
        };
        ExpressionWithAggregateRewriter rewriter{
            view_output_columns_map, output_columns, view_contains_aggregates, need_rollup, empty_groupings, default_value_provider};
        Void c;
        auto rewrite = ASTVisitorUtil::accept(expression, rewriter, c);
        if (rewrite && isValidExpression(*rewrite, output_columns, true))
            return rewrite;
        return {};
    }

    /**
     * It rewrite input expression using output column.
     * If any of the expressions in the input expression cannot be mapped, it will return null.
     */
    static std::optional<ASTPtr> rewriteExpression(
        ASTPtr expression,
        const EqualityASTMap<ConstASTPtr> & view_output_columns_map,
        const std::unordered_set<String> & output_columns,
        bool allow_aggregate = false)
    {
        EquivalencesRewriter rewriter;
        auto rewrite_expression = ASTVisitorUtil::accept(expression, rewriter, view_output_columns_map);
        if (isValidExpression(rewrite_expression, output_columns, allow_aggregate))
            return rewrite_expression;
        return {}; // rewrite fail, bail out
    }

    /**
     * Checks whether there are identifiers in input expression are mapped into output columns,
     * or there are unmapped table references,
     * or there are nested aggregate functions.
     */
    static bool
    isValidExpression(const ConstASTPtr & expression, const std::unordered_set<String> & output_columns, bool allow_aggregate = false)
    {
        class ExpressionChecker : public ConstASTVisitor<bool, bool>
        {
        public:
            explicit ExpressionChecker(const std::unordered_set<String> & output_columns_) : output_columns(output_columns_) { }

            bool visitNode(const ConstASTPtr & ast, bool & c) override
            {
                return std::all_of(ast->children.begin(), ast->children.end(), [&](const auto & child) {
                    return ASTVisitorUtil::accept(child, *this, c);
                });
            }

            bool visitASTTableColumnReference(const ConstASTPtr &, bool &) override { return false; }

            bool visitASTIdentifier(const ConstASTPtr & node, bool &) override
            {
                return output_columns.count(node->as<const ASTIdentifier &>().name());
            }

            bool visitASTFunction(const ConstASTPtr & node, bool & allow_aggregate) override
            {
                if (!AggregateFunctionFactory::instance().isAggregateFunctionName(node->as<ASTFunction &>().name))
                    return visitNode(node, allow_aggregate);
                if (!allow_aggregate)
                    return false;
                bool allow_nested_aggregate = false;
                return visitNode(node, allow_nested_aggregate);
            }

        private:
            const std::unordered_set<String> & output_columns;
        };
        ExpressionChecker checker{output_columns};
        return ASTVisitorUtil::accept(expression, checker, allow_aggregate);
    }

public:
    ContextMutablePtr context;
    const std::map<String, std::vector<MaterializedViewStructurePtr>> & table_based_materialized_views;
    std::unordered_map<PlanNodePtr, RewriterCandidates> candidates;
    std::unordered_map<PlanNodePtr, RewriterFailureMessages> failure_messages;
    std::map<String, std::optional<PlanNodeStatisticsPtr>> materialized_views_stats;
    const bool verbose;
    Poco::Logger * logger = &Poco::Logger::get("CandidatesExplorer");
};

using ASTToStringMap = EqualityASTMap<String>;

class CostBasedMaterializedViewRewriter : public SimplePlanRewriter<Void>
{
public:
    static void rewrite(QueryPlan & plan, ContextMutablePtr context_, std::unordered_map<PlanNodePtr, RewriterCandidates> & match_results)
    {
        Void c;
        CostBasedMaterializedViewRewriter rewriter(context_, plan.getCTEInfo(), match_results);
        auto rewrite = VisitorUtil::accept(plan.getPlanNode(), rewriter, c);
        plan.update(rewrite);
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
            context->getOptimizerMetrics()->addMaterializedView(candidate_it->view_database_and_table_name);
            return constructEquivalentPlan(*candidate_it);
        }
        return SimplePlanRewriter::visitPlanNode(node, c);
    }

    PlanNodePtr constructEquivalentPlan(const RewriterCandidate & candidate)
    {
        // table scan
        auto plan = planTableScan(candidate.target_database_and_table_name, candidate.table_output_columns, candidate.prewhere_expr);

        // where
        if (!PredicateUtils::isTruePredicate(candidate.compensation_predicate))
            plan = PlanNodeBase::createPlanNode(
                context->nextNodeId(), std::make_shared<FilterStep>(plan->getCurrentDataStream(), candidate.compensation_predicate), {plan});

        // aggregation
        Assignments rewrite_assignments;
        std::tie(plan, rewrite_assignments) = planAggregate(plan, candidate.need_rollup, candidate.rollup_keys, candidate.assignments);

        // output projection
        plan = PlanNodeBase::createPlanNode(
            context->nextNodeId(),
            std::make_shared<ProjectionStep>(plan->getCurrentDataStream(), rewrite_assignments, candidate.name_to_type),
            {plan});
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

    /**
     * Extracts aggregates from assignments, plan (rollup) aggregate nodes, return final projection assignments.
     */
    std::pair<PlanNodePtr, Assignments> planAggregate(
        PlanNodePtr plan,
        bool need_rollup,
        const std::vector<ASTPtr> & rollup_keys,
        const Assignments & assignments)
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
        GroupByKeyRewrite key_rewriter{};
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

    class GroupByKeyRewrite : public SimpleExpressionRewriter<ASTToStringMap>
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
};
}

void MaterializedViewRewriter::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    bool verbose = context->getSettingsRef().enable_materialized_view_rewrite_verbose_log;

    auto materialized_views = getRelatedMaterializedViews(plan, context);
    if (materialized_views.empty())
        return;

    auto candidates = CandidatesExplorer::explore(plan, context, materialized_views, verbose);
    if (candidates.empty())
        return;

    CostBasedMaterializedViewRewriter::rewrite(plan, context, candidates);
    // todo: add monitoring metrics
}

std::map<String, std::vector<MaterializedViewStructurePtr>>
MaterializedViewRewriter::getRelatedMaterializedViews(QueryPlan & plan, ContextMutablePtr context)
{
    std::map<String, std::vector<MaterializedViewStructurePtr>> table_based_mview_structures;
    auto & cache = MaterializedViewMemoryCache::instance();

    auto result = RelatedMaterializedViewExtractor::extract(plan, context);
    for (const auto & views : result.table_based_materialized_views)
    {
        std::vector<MaterializedViewStructurePtr> structures;
        for (const auto & view : views.second)
            if (auto structure = cache.getMaterializedViewStructure(view, context, false, result.local_table_to_distributed_table))
                structures.push_back(*structure);
        if (structures.empty())
            continue;
        table_based_mview_structures.emplace(views.first, std::move(structures));
    }

    for (const auto & views : result.table_based_local_materialized_views)
    {
        std::vector<MaterializedViewStructurePtr> structures;
        for (const auto & view : views.second)
            if (auto structure = cache.getMaterializedViewStructure(view, context, true, result.local_table_to_distributed_table))
                structures.push_back(*structure);
        if (structures.empty())
            continue;
        table_based_mview_structures.emplace(views.first, std::move(structures));
    }
    return table_based_mview_structures;
}

}
