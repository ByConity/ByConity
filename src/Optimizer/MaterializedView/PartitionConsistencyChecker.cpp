#include <Optimizer/MaterializedView/PartitionConsistencyChecker.h>

#include <Optimizer/PredicateUtils.h>
#include <Optimizer/SelectQueryInfoHelper.h>
#include <Optimizer/SymbolTransformMap.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTTableColumnReference.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/QueryPlan.h>
#include "Interpreters/Context_fwd.h"
#include "Parsers/queryToString.h"
#include "Storages/StorageMaterializedView.h"

#include <memory>

namespace DB
{
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

std::optional<ASTPtr> getQueryRelatedPartitions(
    const std::vector<PlanNodePtr> & query_table_scans,
    const ConstASTs & query_predicates,
    const std::unordered_map<PlanNodePtr, String> & partition_keys,
    const SymbolTransformMap & query_transform_map,
    ContextPtr context,
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

    ASTs partitions;

    for (const auto & query_table_scan : query_table_scans)
    {
        const auto * table_step = dynamic_cast<const TableScanStep *>(query_table_scan->getStep().get());
        if (!table_step)
            continue;

        auto * storage = dynamic_cast<StorageCnchMergeTree *>(table_step->getStorage().get());
        if (!storage || storage->getSettings()->disable_block_output)
            return {};

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
        auto & table_preds = predicates_by_table[query_table_scan->getId()];
        if (!table_preds.empty())
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, PredicateUtils::combineConjuncts(table_preds));

        SelectQueryInfo query_info = buildSelectQueryInfoForQuery(select_query, context);
        auto required_partitions = storage->getPrunedPartitions(query_info, column_names_to_return, context).partitions;

        LOG_DEBUG(
            logger,
            "Get table {} required partition set: {} by query {}",
            storage->getStorageID().getFullTableName(),
            fmt::join(required_partitions, ", "),
            serializeAST(*select_query));

        auto part_set = makeASTFunction("tuple");
        auto & children = part_set->arguments->children;
        for (const auto & partition : required_partitions)
            children.emplace_back(std::make_shared<ASTLiteral>(std::move(partition)));

        if (children.empty())
            return {};

        if (!partition_keys.contains(query_table_scan))
            return {};

        auto partition_key_column = std::make_shared<ASTIdentifier>(partition_keys.at(query_table_scan));
        auto in_predicate = makeASTFunction("in", partition_key_column, part_set);
        partitions.emplace_back(in_predicate);
    }
    return PredicateUtils::combineConjuncts(partitions);
}

std::optional<ASTPtr> getViewRefreshedPartitions(const StoragePtr & view, const String & partition_key, ContextPtr context)
{
    auto view_partitions = context->getCnchCatalog()->getPartitionList(view, context.get());
    if (auto * view_tree_base = dynamic_cast<MergeTreeMetaBase *>(view.get()))
    {
        auto part_set = makeASTFunction("tuple");
        auto & children = part_set->arguments->children;
        for (const auto & partition : view_partitions)
            children.emplace_back(std::make_shared<ASTLiteral>(partition->getID(*view_tree_base)));

        if (children.empty())
            return {};

        auto in_predicate = makeASTFunction("in", std::make_shared<ASTIdentifier>(partition_key), part_set);
        return in_predicate;
    }
    return {};
}

std::optional<PartitionCheckResult>
checkMaterializedViewPartitionConsistency(StorageMaterializedView * mview, ContextMutablePtr context)
{
    PartitionDiffPtr partition_diff = std::make_shared<PartitionDiff>();
    VersionPartContainerPtrs latest_versioned_partitions;
    mview->validateAndSyncBaseTablePartitions(partition_diff, latest_versioned_partitions, context);

    if (!partition_diff->paritition_based_refresh)
        return {};

    if (partition_diff->add_partitions.empty() && partition_diff->drop_partitions.empty())
        return PartitionCheckResult{nullptr, 0, PredicateConst::FALSE_VALUE, PredicateConst::TRUE_VALUE};

    auto partition_transformer = mview->getPartitionTransformer();

    VersionPartPtrs update_parts;
    update_parts.insert(update_parts.end(), partition_diff->add_partitions.begin(), partition_diff->add_partitions.end());
    update_parts.insert(update_parts.end(), partition_diff->drop_partitions.begin(), partition_diff->drop_partitions.end());
    std::unordered_map<String, String> name_to_binary;
    PartMapRelations part_map = partition_transformer->transform(update_parts, name_to_binary, partition_diff->depend_storage_id);

    std::set<String> source_parts;
    std::set<String> target_parts;
    for (const auto & relation : part_map)
    {
        target_parts.insert(relation.first);
        for (const auto & part : relation.second)
            source_parts.insert(part);
    }

    if (source_parts.empty() || target_parts.empty())
        return PartitionCheckResult{nullptr, 0, PredicateConst::FALSE_VALUE, PredicateConst::TRUE_VALUE};

    const auto & depend_base_table = partition_transformer->getDependBaseTables().at(partition_diff->depend_storage_id);

    ParserExpression parser(ParserSettings::CLICKHOUSE);
    const auto & settings = context->getSettingsRef();
    String query_partition_filter = fmt::format(
        "{} in ({})",
        queryToString(depend_base_table->partition_key_ast),
        fmt::join(source_parts, ","));
    ASTPtr query_partition_filter_ast = parseQuery(parser, query_partition_filter, settings.max_query_size, settings.max_parser_depth);

    String mv_partition_filter = fmt::format(
        "{} not in ({})",
        queryToString(partition_transformer->getTargeTable()->getInMemoryMetadataPtr()->getPartitionKeyAST()),
        fmt::join(target_parts, ","));
    ASTPtr mv_partition_filter_ast = parseQuery(parser, mv_partition_filter, settings.max_query_size, settings.max_parser_depth);
    return PartitionCheckResult{
        depend_base_table->storage, depend_base_table->unique_id, query_partition_filter_ast, mv_partition_filter_ast};
}
}
