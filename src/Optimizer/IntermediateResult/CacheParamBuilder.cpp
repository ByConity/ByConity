#include <Optimizer/IntermediateResult/CacheParamBuilder.h>

#include <Interpreters/Context.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterManager.h>
#include <Optimizer/IntermediateResult/CacheParam.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/RuntimeFilterUtils.h>
#include <Optimizer/Utils.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPlan/FilterStep.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/JoinStep.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/TableScanStep.h>
#include <google/protobuf/util/json_util.h>
#include <Common/LinkedHashMap.h>
#include <Common/SipHash.h>

#include <memory>
#include <string>
#include <unordered_map>

namespace DB
{

CacheParam CacheParamBuilder::buildCacheParam()
{
    // 1. build digest
    auto digest = std::to_string(getDigest());
    // 2. build output order
    std::unordered_map<size_t, size_t> output_pos_to_cache_pos;
    std::unordered_map<size_t, size_t> cache_pos_to_output_pos;
    auto cache_order = getCacheOrder();
    auto original_order = cache_root->getStep()->getOutputStream().header;
    for (size_t original_idx = 0; original_idx < original_order.columns(); ++original_idx)
    {
        size_t cache_idx = cache_order.getPositionByName(original_order.getByPosition(original_idx).name);
        output_pos_to_cache_pos.emplace(original_idx, cache_idx);
        cache_pos_to_output_pos.emplace(cache_idx, original_idx);
    }
    // 3. build related tables
    std::vector<StorageID> tables;
    // collect tables in cache subtree
    collectAndMarkTablesDFS(cache_root, tables, digest);
    // collect tables in rtf builders
    for (const auto & pair : cacheable_runtime_filters.cacheable)
        collectAndMarkTablesDFS(pair.second.node->getChildren()[1], tables, digest);
    CacheParam res{digest, std::move(output_pos_to_cache_pos), std::move(cache_pos_to_output_pos), tables.front(), {}};
    tables.erase(tables.begin());
    res.dependent_tables = std::move(tables);
    return res;
}


size_t CacheParamBuilder::computeStepHash(PlanNodePtr node)
{
    auto normal_step = normalizer.computeNormalStep(node);
    if (auto join_step = dynamic_pointer_cast<JoinStep>(normal_step);
        join_step && !join_step->getRuntimeFilterBuilders().empty())
        return computeJoinHash(join_step);
    else if (auto filter_step = dynamic_pointer_cast<FilterStep>(normal_step))
        return computeFilterHash(filter_step);
    else if (auto table_step = dynamic_pointer_cast<TableScanStep>(normal_step))
        return computeTableScanHash(table_step);
    else if (auto aggregating_step = dynamic_pointer_cast<AggregatingStep>(normal_step))
        return computeAggregatingHash(aggregating_step);
    return normal_step->hash();
}


size_t CacheParamBuilder::computeJoinHash(std::shared_ptr<JoinStep> join_step)
{
    // all rtf builders can be removed because we associate relevant info to usage instead
    auto new_step = std::make_shared<JoinStep>(
        join_step->getInputStreams(),
        join_step->getOutputStream(),
        join_step->getKind(),
        join_step->getStrictness(),
        join_step->getMaxStreams(),
        join_step->getKeepLeftReadInOrder(),
        join_step->getLeftKeys(),
        join_step->getRightKeys(),
        join_step->getKeyIdsNullSafe(),
        join_step->getFilter(),
        join_step->isHasUsing(),
        join_step->getRequireRightKeys(),
        join_step->getAsofInequality(),
        join_step->getDistributionType(),
        join_step->getJoinAlgorithm(),
        join_step->isMagic(),
        join_step->isOrdered(),
        join_step->isSimpleReordered(),
        LinkedHashMap<std::string, RuntimeFilterBuildInfos>{},
        join_step->getHints());
    return new_step->hash();
}

size_t CacheParamBuilder::computeAggregatingHash(std::shared_ptr<AggregatingStep> aggregating_step)
{
    // compute partial aggregating ingore final mark
    auto new_step = std::make_shared<AggregatingStep>(
        aggregating_step->getInputStreams()[0],
        aggregating_step->getKeys(),
        aggregating_step->getKeysNotHashed(),
        aggregating_step->getAggregates(),
        aggregating_step->getGroupingSetsParams(),
        /* final */ false,
        aggregating_step->getGroupBySortDescription(),
        aggregating_step->getGroupings(),
        aggregating_step->needOverflowRow(),
        aggregating_step->shouldProduceResultsInOrderOfBucketNumber(),
        aggregating_step->isNoShuffle(),
        aggregating_step->isStreamingForCache(),
        aggregating_step->getHints());

    // Protos::QueryPlanStep proto;
    // serializeQueryPlanStepToProto(new_step, proto);
    // String json_msg;
    // google::protobuf::util::JsonPrintOptions pb_options;
    // pb_options.preserve_proto_field_names = true;
    // pb_options.always_print_primitive_fields = true;
    // pb_options.add_whitespace = false;
    // google::protobuf::util::MessageToJsonString(proto, &json_msg, pb_options);
    // LOG_DEBUG(getLogger("AddCache"), " {} hash: {}\n json: {}", new_step->getName(), new_step->hash(), json_msg);
    return new_step->hash();
}

size_t CacheParamBuilder::computeFilterHash(std::shared_ptr<FilterStep> filter_step)
{
    auto [runtime_filters, static_filters] = RuntimeFilterUtils::extractRuntimeFilters(filter_step->getFilter());
    if (runtime_filters.empty())
        return filter_step->hash();

    auto new_step = std::make_shared<FilterStep>(
        filter_step->getInputStreams()[0],
        replaceRuntimeFilterIdByHash(filter_step->getFilter()),
        filter_step->removesFilterColumn());
    return new_step->hash();
}

size_t CacheParamBuilder::computeTableScanHash(std::shared_ptr<TableScanStep> table_step)
{
    std::shared_ptr<FilterStep> new_push_down_filter;
    if (auto push_down_filter = dynamic_pointer_cast<FilterStep>(table_step->getPushdownFilter()))
    {
        new_push_down_filter = std::make_shared<FilterStep>(
            push_down_filter->getInputStreams()[0],
            replaceRuntimeFilterIdByHash(push_down_filter->getFilter()),
            push_down_filter->removesFilterColumn());
    }
    SelectQueryInfo new_info = table_step->getQueryInfo();
    auto storage_id = table_step->getStorageID();
    storage_id.clearUUID();

    if (auto query = dynamic_pointer_cast<ASTSelectQuery>(new_info.query))
    {
        auto new_query = static_pointer_cast<ASTSelectQuery>(query->clone());
        if (auto query_filter = query->where())
            new_query->refWhere() = replaceRuntimeFilterIdByHash(query_filter);
        if (auto query_filter = query->prewhere())
            new_query->refPrewhere() = replaceRuntimeFilterIdByHash(query_filter);
        new_query->replaceDatabaseAndTable(storage_id);
        new_info.query = new_query;
    }

    if (context->getSettingsRef().enable_intermediate_result_cache_ignore_partition_filter)
    {
        new_info.partition_filter.reset();
    }

    auto new_step = std::make_shared<TableScanStep>(
        table_step->getOutputStream(),
        table_step->getStorage(),
        storage_id,
        table_step->getMetadataSnapshot(),
        table_step->getStorageSnapshot(),
        table_step->getOriginalTable(),
        table_step->getColumnNames(),
        table_step->getColumnAlias(),
        new_info,
        table_step->getMaxBlockSize(),
        table_step->getTableAlias(),
        table_step->isBucketScan(),
        table_step->getHints(),
        table_step->getInlineExpressions(),
        table_step->getPushdownAggregation(),
        table_step->getPushdownProjection(),
        new_push_down_filter,
        table_step->getTableOutputStream());
    return new_step->hash();
}

size_t CacheParamBuilder::computeRuntimeFilterHash(RuntimeFilterId id)
{
    if (auto it = runtime_filter_hash.find(id); it != runtime_filter_hash.end())
        return it->second;

    Utils::checkArgument(cacheable_runtime_filters.cacheable.contains(id));
    auto build_info = cacheable_runtime_filters.cacheable.at(id);

    auto right_child = build_info.node->getChildren()[1];
    PlanSignature right_sig = computeSignature(right_child);
    SipHash hash{right_sig};

    Block output_order = computeNormalOutputOrder(right_child);
    auto build_symbol_pos = output_order.getPositionByName(build_info.symbol);
    hash.update(build_symbol_pos);
    hash.update(build_info.info.distribution);

    runtime_filter_hash.emplace(id, hash.get64());
    return runtime_filter_hash.at(id);
}

ASTPtr CacheParamBuilder::replaceRuntimeFilterIdByHash(ConstASTPtr filter)
{
    auto [runtime_filters, static_filters] = RuntimeFilterUtils::extractRuntimeFilters(filter);
    if (runtime_filters.empty())
        return filter->clone();

    std::vector<ConstASTPtr> new_filters = std::move(static_filters);
    for (const auto & runtime_filter : runtime_filters)
    {
        RuntimeFilterId id = RuntimeFilterUtils::extractId(runtime_filter);
        Utils::checkArgument(cacheable_runtime_filters.isChecked(id));
        if (cacheable_runtime_filters.ignorable.contains(id))
            continue;
        else
        {
            auto id_hash = computeRuntimeFilterHash(id);
            auto normalized_rtf = runtime_filter->clone();
            normalized_rtf->as<ASTFunction &>().arguments->children[0]->as<ASTLiteral &>().value = id_hash;
            new_filters.emplace_back(normalized_rtf);
        }
    }
    return PredicateUtils::combineConjuncts(new_filters);
}

void CacheParamBuilder::collectAndMarkTablesDFS(PlanNodePtr root, std::vector<StorageID> & tables, const std::string & digest)
{
    if (auto table_step = dynamic_pointer_cast<TableScanStep>(root->getStep()))
    {
        table_step->getQueryInfo().cache_info.addDependent(digest, tables.size());

        auto storage_id = table_step->getStorage()->getStorageID();
        tables.emplace_back(std::move(storage_id));
    }
    else
        for (const auto & child : root->getChildren())
            collectAndMarkTablesDFS(child, tables, digest);
}

}
