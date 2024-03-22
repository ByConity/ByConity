#include <Optimizer/MaterializedView/PartitionConsistencyChecker.h>

#include <Interpreters/Context_fwd.h>
#include <Optimizer/MaterializedView/MaterializedViewStructure.h>
#include <Optimizer/PredicateConst.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/SelectQueryInfoHelper.h>
#include <Optimizer/SymbolTransformMap.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTTableColumnReference.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/queryToString.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/QueryPlan.h>
#include <Storages/StorageMaterializedView.h>

#include <memory>
#include <unordered_map>

namespace DB
{

std::optional<PartitionCheckResult>
checkMaterializedViewPartitionConsistency(MaterializedViewStructurePtr structure, ContextMutablePtr context)
{
    static PartitionCheckResult freshness = PartitionCheckResult{nullptr, 0, PredicateConst::FALSE_VALUE, PredicateConst::TRUE_VALUE};

    if (context->getSettingsRef().materialized_view_consistency_check_method == MaterializedViewConsistencyCheckMethod::NONE)
        return freshness;

    auto storage = DatabaseCatalog::instance().tryGetTable(structure->view_storage_id, context);
    if (!storage)
        return {};

    auto * mview = dynamic_cast<StorageMaterializedView *>(storage.get());
    if (!mview)
        return {};
    auto partition_transformer = std::make_shared<PartitionTransformer>(mview->getInnerQuery()->clone(), mview->getTargetTableId(), mview->async());
    try
    {
        partition_transformer->validate(context, structure);
    }
    catch (...)
    {
        return {}; // validate failed, bail out
    }

    PartitionDiffPtr partition_diff = std::make_shared<PartitionDiff>();
    VersionPartContainerPtrs latest_versioned_partitions;
    mview->syncBaseTablePartitions(
        partition_diff,
        latest_versioned_partitions,
        partition_transformer->getBaseTables(),
        partition_transformer->getNonDependBaseTables(),
        context,
        true);

    if (partition_diff->add_partitions.empty() && partition_diff->drop_partitions.empty())
        return freshness; // non partition changed

    if (!partition_diff->paritition_based_refresh)
        return {}; // is not parition based refresh, bail out

    if (!partition_diff->depend_storage_id)
        return {}; // is not parition based refresh, bail out

    VersionPartPtrs update_parts;
    update_parts.insert(update_parts.end(), partition_diff->add_partitions.begin(), partition_diff->add_partitions.end());
    update_parts.insert(update_parts.end(), partition_diff->drop_partitions.begin(), partition_diff->drop_partitions.end());
    std::unordered_map<String, String> name_to_binary;
    PartMapRelations part_map = partition_transformer->transform(update_parts, name_to_binary, partition_diff->depend_storage_id);

    std::unordered_set<String> synced_partitions;
    if (mview->sync())
    {
        // fetch synced partitions
        auto view_partitions = context->getCnchCatalog()->getPartitionList(partition_transformer->getTargeTable(), context.get());
        FormatSettings format_settings;
        auto & target_meta_base = dynamic_cast<MergeTreeMetaBase &>(*partition_transformer->getTargeTable());
        for (const auto & partition : view_partitions)
        {
            WriteBufferFromOwnString write_buffer;
            partition->serializeText(target_meta_base, write_buffer, format_settings);
            synced_partitions.emplace(write_buffer.str());
        }
    }

    std::set<String> source_parts;
    std::set<String> target_parts;
    for (const auto & relation : part_map)
    {
        if (synced_partitions.contains(relation.first))
            continue;
        target_parts.insert(relation.first);
        for (const auto & part : relation.second)
            source_parts.insert(part);
    }

    if (source_parts.empty() || target_parts.empty())
        return freshness;

    const auto & depend_base_table = partition_transformer->getDependBaseTables().at(partition_diff->depend_storage_id);

    ParserExpression parser(ParserSettings::CLICKHOUSE);
    const auto & settings = context->getSettingsRef();
    String query_partition_filter = fmt::format(
        "{} in ({})",
        queryToString(depend_base_table->partition_key_ast),
        fmt::join(source_parts, ","));
    ASTPtr query_partition_filter_ast = parseQuery(parser, query_partition_filter, settings.max_query_size, settings.max_parser_depth);

    ASTPtr mv_partition_filter_ast = PredicateConst::TRUE_VALUE;
    if (mview->async())
    {
        String mv_partition_filter = fmt::format(
            "{} not in ({})",
            queryToString(partition_transformer->getTargeTable()->getInMemoryMetadataPtr()->getPartitionKeyAST()),
            fmt::join(target_parts, ","));
        mv_partition_filter_ast = parseQuery(parser, mv_partition_filter, settings.max_query_size, settings.max_parser_depth);
    }

    return PartitionCheckResult{
        depend_base_table->storage, depend_base_table->unique_id, query_partition_filter_ast, mv_partition_filter_ast};
}
}
