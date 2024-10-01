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

#include <memory>
#include <optional>
#include <unordered_map>
#include <Core/SortDescription.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Interpreters/Context.h>
#include <Interpreters/WindowDescription.h>
#include <Optimizer/Property/Property.h>
#include <Optimizer/SimpleExpressionRewriter.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <QueryPlan/ExpandStep.h>
#include <QueryPlan/PlanSerDerHelper.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/SymbolMapper.h>
#include <Common/Exception.h>

namespace DB
{
static constexpr size_t MAX_LOOKUP_TIMES = 10000;

class SymbolMapper::IdentifierRewriter : public SimpleExpressionRewriter<Void>
{
public:
    explicit IdentifierRewriter(MappingFunction & mapping_function_) : mapping_function(mapping_function_) { }

    ASTPtr visitASTIdentifier(ASTPtr & expr, Void &) override
    {
        return std::make_shared<ASTIdentifier>(mapping_function(expr->as<ASTIdentifier &>().name()));
    }

private:
    MappingFunction & mapping_function;
};

SymbolMapper SymbolMapper::simpleMapper(std::unordered_map<Symbol, Symbol> & mapping)
{
    return SymbolMapper([&mapping](Symbol symbol) {
        auto it = mapping.find(symbol);
        return it != mapping.end() ? it->second : std::move(symbol);
    });
}

SymbolMapper SymbolMapper::symbolMapper(std::unordered_map<Symbol, Symbol> & mapping)
{
    return SymbolMapper([&mapping](Symbol symbol) {
        auto it = mapping.find(symbol);
        size_t lookup = 0;
        while (it != mapping.end() && it->second != symbol)
        {
            if (++lookup > MAX_LOOKUP_TIMES)
                throw Exception("endless loop in SymbolMapper", ErrorCodes::LOGICAL_ERROR);
            symbol = it->second;
            it = mapping.find(symbol);
        }
        return symbol;
    });
}

SymbolMapper SymbolMapper::symbolReallocator(std::unordered_map<Symbol, Symbol> & mapping, SymbolAllocator & symbolAllocator)
{
    return SymbolMapper([&](Symbol symbol) {
        auto it = mapping.find(symbol);
        if (it != mapping.end())
        {
            size_t lookup = 0;
            while (it != mapping.end() && it->second != symbol)
            {
                if (++lookup > MAX_LOOKUP_TIMES)
                    throw Exception("endless loop in SymbolMapper", ErrorCodes::LOGICAL_ERROR);
                symbol = it->second;
                it = mapping.find(symbol);
            }
            // do not remap the symbol further
            mapping[symbol] = symbol;
            return symbol;
        }

        Symbol new_symbol = symbolAllocator.newSymbol(symbol);
        mapping[symbol] = new_symbol;
        // do not remap the symbol further
        mapping[new_symbol] = new_symbol;
        return new_symbol;
    });
}

NameSet SymbolMapper::mapToDistinct(const Names & symbols)
{
    NameSet ret;
    std::transform(symbols.begin(), symbols.end(), std::inserter(ret, ret.end()), mapping_function);
    return ret;
}

NameSet SymbolMapper::map(const NameSet & names)
{
    NameSet ret;
    std::transform(names.begin(), names.end(), std::inserter(ret, ret.end()), mapping_function);
    return ret;
}


NamesAndTypes SymbolMapper::map(const NamesAndTypes & name_and_types)
{
    NamesAndTypes ret;
    std::transform(name_and_types.begin(), name_and_types.end(), std::back_inserter(ret), [&](const auto & name_and_type) {
        return NameAndTypePair{mapping_function(name_and_type.name), name_and_type.type};
    });
    return ret;
}

Assignments SymbolMapper::map(const Assignments & assignments)
{
    Assignments ret;
    for (const auto & assignment : assignments)
    {
        auto output = map(assignment.first);
        // fixme: handle duplicate assignment
        if (!ret.contains(output))
        {
            ret.emplace(output, map(assignment.second));
        }
    }
    return ret;
}

Assignment SymbolMapper::map(const Assignment & assignment)
{
    return {map(assignment.first), map(assignment.second)};
}

NameToType SymbolMapper::map(const NameToType & name_to_type)
{
    NameToType ret;
    for (const auto & [name, type] : name_to_type)
    {
        ret.emplace(map(name), type);
    }
    return ret;
}

NamesWithAliases SymbolMapper::map(const NamesWithAliases & name_with_aliases)
{
    NamesWithAliases ret;
    std::transform(name_with_aliases.begin(), name_with_aliases.end(), std::back_inserter(ret), [&](const auto & name_with_alias) {
        return NameWithAlias{name_with_alias.first, mapping_function(name_with_alias.second)};
    });
    return ret;
}

Block SymbolMapper::map(const Block & name_and_types)
{
    Block ret;
    for (const auto & item : name_and_types)
    {
        auto mapped = mapping_function(item.name);
        if (!ret.has(mapped))
        {
            ret.insert(ColumnWithTypeAndName{item.column, item.type, mapped});
        }
    }
    return ret;
}

DataStream SymbolMapper::map(const DataStream & data_stream)
{
    DataStream output{map(data_stream.header)};
    output.has_single_port = data_stream.has_single_port;
    output.sort_mode = data_stream.sort_mode;
    output.sort_description = SortDescription{map(data_stream.sort_description)};
    return output;
}

ASTPtr SymbolMapper::map(const ASTPtr & expr)
{
    if (expr == nullptr)
    {
        return nullptr;
    }
    IdentifierRewriter visitor(mapping_function);
    Void void_context{};
    return ASTVisitorUtil::accept(expr->clone(), visitor, void_context);
}

ASTPtr SymbolMapper::map(const ConstASTPtr & expr)
{
    if (expr == nullptr)
    {
        return nullptr;
    }
    IdentifierRewriter visitor(mapping_function);
    Void void_context{};
    return ASTVisitorUtil::accept(expr->clone(), visitor, void_context);
}

Partitioning SymbolMapper::map(const Partitioning & partition)
{
    return {
        partition.getHandle(),
        map(partition.getColumns()),
        partition.isRequireHandle(),
        partition.getBuckets(),
        partition.getBucketExpr(),
        partition.isEnforceRoundRobin(),
        partition.getComponent()};
}

std::shared_ptr<JoinStep> SymbolMapper::map(const JoinStep & join)
{
    return std::make_shared<JoinStep>(
        map(join.getInputStreams()),
        map(join.getOutputStream()),
        join.getKind(),
        join.getStrictness(),
        join.getMaxStreams(),
        join.getKeepLeftReadInOrder(),
        map(join.getLeftKeys()),
        map(join.getRightKeys()),
        join.getKeyIdsNullSafe(),
        map(join.getFilter()),
        join.isHasUsing(),
        join.getRequireRightKeys(),
        join.getAsofInequality(),
        join.getDistributionType(),
        join.getJoinAlgorithm(),
        join.isMagic(),
        join.isOrdered(),
        join.isSimpleReordered(),
        map(join.getRuntimeFilterBuilders()),
        join.getHints());
}

LinkedHashMap<String, RuntimeFilterBuildInfos> SymbolMapper::map(const LinkedHashMap<String, RuntimeFilterBuildInfos> & infos)
{
    LinkedHashMap<String, RuntimeFilterBuildInfos> res;
    for (const auto & info : infos)
    {
        auto symbol = map(info.first);
        // fixme: handle duplicate symbol
        if (!res.contains(symbol))
        {
            res.emplace(symbol, info.second);
        }
    }
    return res;
}

PlanNodeStatisticsEstimate SymbolMapper::map(const PlanNodeStatisticsEstimate & estimate)
{
    if (!estimate.has_value())
    {
        return estimate;
    }

    std::unordered_map<String, SymbolStatisticsPtr> symbol_statistics;
    for (const auto & entry : estimate.value()->getSymbolStatistics())
    {
        symbol_statistics.emplace(map(entry.first), entry.second);
    }
    return PlanNodeStatisticsEstimate{
        std::make_optional(std::make_shared<PlanNodeStatistics>(estimate.value()->getRowCount(), std::move(symbol_statistics)))};
}

AggregateDescription SymbolMapper::map(const AggregateDescription & desc)
{
    return AggregateDescription{
        desc.function,
        desc.parameters,
        desc.arguments,
        map(desc.argument_names),
        map(desc.column_name),
        desc.mask_column.empty() ? desc.mask_column : map(desc.mask_column)};
}

WindowFunctionDescription SymbolMapper::map(const WindowFunctionDescription & desc)
{
    return WindowFunctionDescription{
        map(desc.column_name),
        desc.function_node,
        desc.aggregate_function,
        desc.function_parameters,
        desc.argument_types,
        map(desc.argument_names)};
}

WindowDescription SymbolMapper::map(const WindowDescription & desc)
{
    return WindowDescription{
        desc.window_name,
        SortDescription{map(desc.partition_by)},
        SortDescription{map(desc.order_by)},
        SortDescription{map(desc.full_sort_description)},
        // desc.partition_by_actions,
        // desc.order_by_actions,
        desc.frame,
        map(desc.window_functions)};
}

SortColumnDescription SymbolMapper::map(const SortColumnDescription & desc)
{
    auto res = desc;
    res.column_name = map(desc.column_name);
    return res;
}

GroupingDescription SymbolMapper::map(const GroupingDescription & desc)
{
    return GroupingDescription{map(desc.argument_names), map(desc.output_name)};
}

GroupingSetsParams SymbolMapper::map(const GroupingSetsParams & param)
{
    GroupingSetsParams res{map(param.used_key_names)};
    res.used_keys = param.used_keys;
    res.missing_keys = param.missing_keys;
    return res;
}

Aggregator::Params SymbolMapper::map(const Aggregator::Params & params)
{
    auto header = map(params.src_header);
    auto intermediate_header = map(params.intermediate_header);
    ColumnNumbers keys;
    std::unordered_set<String> distinct_keys;
    if (params.src_header.columns() != 0)
    {
        for (const auto & key : params.keys)
        {
            auto name = map(params.src_header.getByPosition(key).name);
            if (distinct_keys.emplace(name).second)
                keys.emplace_back(header.getPositionByName(name));
        }
    }
    else
    {
        for (const auto & key : params.keys)
        {
            auto name = map(params.intermediate_header.getByPosition(key).name);
            if (distinct_keys.emplace(name).second)
                keys.emplace_back(intermediate_header.getPositionByName(name));
        }
    }

    return {
        header,
        keys,
        map(params.aggregates),
        params.overflow_row,
        params.max_rows_to_group_by,
        params.group_by_overflow_mode,
        params.group_by_two_level_threshold,
        params.group_by_two_level_threshold_bytes,
        params.max_bytes_before_external_group_by,
        params.enable_adaptive_spill,
        params.spill_buffer_bytes_before_external_group_by,
        params.empty_result_for_aggregation_by_empty_set,
        params.tmp_volume,
        params.max_threads,
        params.min_free_disk_space,
        params.compile_aggregate_expressions,
        params.min_count_to_compile_aggregate_expression,
        intermediate_header,
        params.enable_lc_group_by_opt};
}

AggregatingTransformParamsPtr SymbolMapper::map(const AggregatingTransformParamsPtr & param)
{
    if (param->aggregator_list_ptr && param->aggregator_list_ptr->size() > 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Symbol mapper is unable to handle parallel aggregate param.");

    return std::make_shared<AggregatingTransformParams>(map(param->params), param->final);
}

ArrayJoinActionPtr SymbolMapper::map(const ArrayJoinActionPtr & array_join_action)
{
    if (array_join_action == nullptr)
        return nullptr;
    return std::make_shared<ArrayJoinAction>(
        map(array_join_action->columns),
        array_join_action->is_left,
        array_join_action->is_unaligned,
        array_join_action->function_length,
        array_join_action->function_greatest,
        array_join_action->function_array_resize,
        array_join_action->function_builder);
}

SortDescription SymbolMapper::map(const SortDescription & sort_desc)
{
    SortDescription res;
    std::transform(sort_desc.begin(), sort_desc.end(), std::back_inserter(res), [this](const auto & param) { return this->map(param); });
    return res;
}

std::map<Int32, Names> SymbolMapper::map(const std::map<Int32, Names> & group_id_non_null_symbol)
{
    std::map<Int32, Names> res;
    for (const auto & entry : group_id_non_null_symbol)
    {
        res[entry.first] = map(entry.second);
    }
    return res;
}

std::shared_ptr<AggregatingStep> SymbolMapper::map(const AggregatingStep & agg)
{
    return std::make_shared<AggregatingStep>(
        map(agg.getInputStreams()[0]),
        distinct(map(agg.getKeys())),
        map(agg.getKeysNotHashed()),
        map(agg.getAggregates()),
        map(agg.getGroupingSetsParams()),
        agg.isFinal(),
        SortDescription{map(agg.getGroupBySortDescription())},
        map(agg.getGroupings()),
        agg.needOverflowRow(),
        agg.shouldProduceResultsInOrderOfBucketNumber(),
        agg.isNoShuffle(),
        agg.isStreamingForCache(),        
        agg.getHints());
}

std::shared_ptr<ApplyStep> SymbolMapper::map(const ApplyStep & apply)
{
    return std::make_shared<ApplyStep>(
        map(apply.getInputStreams()),
        map(apply.getCorrelation()),
        apply.getApplyType(),
        apply.getSubqueryType(),
        map(apply.getAssignment()),
        map(apply.getOuterColumns()),
        apply.supportSemiAnti());
}

std::shared_ptr<ArrayJoinStep> SymbolMapper::map(const ArrayJoinStep & array_join)
{
    return std::make_shared<ArrayJoinStep>(map(array_join.getInputStreams()[0]), map(array_join.getArrayJoinAction()));
}

std::shared_ptr<AssignUniqueIdStep> SymbolMapper::map(const AssignUniqueIdStep & assign)
{
    return std::make_shared<AssignUniqueIdStep>(map(assign.getInputStreams()[0]), map(assign.getUniqueId()));
}

std::shared_ptr<DistinctStep> SymbolMapper::map(const DistinctStep & distinct)
{
    return std::make_shared<DistinctStep>(
        map(distinct.getInputStreams()[0]),
        distinct.getSetSizeLimits(),
        distinct.getLimitHint(),
        map(distinct.getColumns()),
        distinct.preDistinct());
}

std::shared_ptr<EnforceSingleRowStep> SymbolMapper::map(const EnforceSingleRowStep & row)
{
    return std::make_shared<EnforceSingleRowStep>(map(row.getInputStreams()[0]));
}

std::shared_ptr<ExtremesStep> SymbolMapper::map(const ExtremesStep & extremes)
{
    return std::make_shared<ExtremesStep>(extremes.getInputStreams()[0]);
}

std::shared_ptr<ExceptStep> SymbolMapper::map(const ExceptStep & except)
{
    std::unordered_map<String, std::vector<String>> outputs_to_inputs;
    for (const auto & [output, inputs] : except.getOutToInputs())
    {
        auto mapped_inputs = map(inputs);
        outputs_to_inputs.emplace(map(output), mapped_inputs);
    }
    return std::make_shared<ExceptStep>(
        map(except.getInputStreams()), map(except.getOutputStream()), outputs_to_inputs, except.isDistinct());
}

std::shared_ptr<ExchangeStep> SymbolMapper::map(const ExchangeStep & exchange)
{
    return std::make_shared<ExchangeStep>(
        map(exchange.getInputStreams()), exchange.getExchangeMode(), map(exchange.getSchema()), exchange.needKeepOrder());
}

std::shared_ptr<FillingStep> SymbolMapper::map(const FillingStep & filling)
{
    return std::make_shared<FillingStep>(map(filling.getInputStreams()[0]), SortDescription{map(filling.getSortDescription())});
}

std::shared_ptr<FinalSampleStep> SymbolMapper::map(const FinalSampleStep & final_sample)
{
    return std::make_shared<FinalSampleStep>(
        map(final_sample.getInputStreams()[0]), final_sample.getSampleSize(), final_sample.getMaxChunkSize());
}

std::shared_ptr<FinishSortingStep> SymbolMapper::map(const FinishSortingStep & finish_sorting)
{
    return std::make_shared<FinishSortingStep>(
        map(finish_sorting.getInputStreams()[0]),
        SortDescription{map(finish_sorting.getPrefixDescription())},
        SortDescription{map(finish_sorting.getResultDescription())},
        finish_sorting.getMaxBlockSize(),
        finish_sorting.getLimit());
}

std::shared_ptr<IntersectStep> SymbolMapper::map(const IntersectStep & intersect)
{
    std::unordered_map<String, std::vector<String>> outputs_to_inputs;
    for (const auto & [output, inputs] : intersect.getOutToInputs())
    {
        auto mapped_inputs = map(inputs);
        outputs_to_inputs.emplace(map(output), mapped_inputs);
    }
    return std::make_shared<IntersectStep>(
        map(intersect.getInputStreams()), map(intersect.getOutputStream()), outputs_to_inputs, intersect.isDistinct());
}

std::shared_ptr<IntersectOrExceptStep> SymbolMapper::map(const IntersectOrExceptStep & intersect_or_except)
{
    return std::make_shared<IntersectOrExceptStep>(
        map(intersect_or_except.getInputStreams()), intersect_or_except.getOperator(), intersect_or_except.getMaxThreads());
}

std::shared_ptr<TableScanStep> SymbolMapper::map(const TableScanStep & scan)
{
    DataStream mapped_output_stream = map(scan.getOutputStream());
    NamesWithAliases mapped_column_alias = map(scan.getColumnAlias());
    DataStream mapped_table_output_stream = map(scan.getTableOutputStream());

    Assignments mapped_inline_expressions;

    for (const auto & inline_expr : scan.getInlineExpressions())
        mapped_inline_expressions.emplace_back(mapping_function(inline_expr.first), inline_expr.second);

    // order matters as symbol mapper should traverse plan nodes bottom-up
    std::shared_ptr<FilterStep> mapped_filter = scan.getPushdownFilterCast() ? map(*scan.getPushdownFilterCast()) : nullptr;
    std::shared_ptr<ProjectionStep> mapped_projection = scan.getPushdownProjectionCast() ? map(*scan.getPushdownProjectionCast()) : nullptr;
    std::shared_ptr<AggregatingStep> mapped_aggregation
        = scan.getPushdownAggregationCast() ? map(*scan.getPushdownAggregationCast()) : nullptr;

    auto mapped_scan = std::make_shared<TableScanStep>(
        std::move(mapped_output_stream),
        scan.getStorage(),
        scan.getStorageID(),
        scan.getMetadataSnapshot(),
        scan.getStorageSnapshot(),
        scan.getOriginalTable(),
        scan.getColumnNames(),
        std::move(mapped_column_alias),
        scan.getQueryInfo(),
        scan.getMaxBlockSize(),
        scan.getTableAlias(),
        scan.isBucketScan(),
        scan.getHints(),
        std::move(mapped_inline_expressions),
        std::move(mapped_aggregation),
        std::move(mapped_projection),
        std::move(mapped_filter),
        std::move(mapped_table_output_stream));

    return mapped_scan;
}


std::shared_ptr<FilterStep> SymbolMapper::map(const FilterStep & filter)
{
    return std::make_shared<FilterStep>(map(filter.getInputStreams()[0]), map(filter.getFilter()), filter.removesFilterColumn());
}

std::shared_ptr<LimitStep> SymbolMapper::map(const LimitStep & limit)
{
    return std::make_shared<LimitStep>(
        map(limit.getInputStreams()[0]),
        limit.getLimit(),
        limit.getOffset(),
        limit.isAlwaysReadTillEnd(),
        limit.isWithTies(),
        SortDescription{map(limit.getSortDescription())},
        limit.isPartial());
}

std::shared_ptr<LimitByStep> SymbolMapper::map(const LimitByStep & limit)
{
    Names names = {map(limit.getColumns())};
    return std::make_shared<LimitByStep>(map(limit.getInputStreams()[0]), limit.getGroupLength(), limit.getGroupOffset(), map(names));
}

std::shared_ptr<MergingSortedStep> SymbolMapper::map(const MergingSortedStep & sorted)
{
    return std::make_shared<MergingSortedStep>(
        map(sorted.getInputStreams()[0]), SortDescription{map(sorted.getSortDescription())}, sorted.getMaxBlockSize(), sorted.getLimit());
}

std::shared_ptr<MergingAggregatedStep> SymbolMapper::map(const MergingAggregatedStep & merging_agg)
{
    return std::make_shared<MergingAggregatedStep>(
        map(merging_agg.getInputStreams()[0]),
        distinct(map(merging_agg.getKeys())),
        map(merging_agg.getGroupingSetsParamsList()),
        map(merging_agg.getGroupings()),
        map(merging_agg.getAggregatingTransformParams()),
        merging_agg.isMemoryEfficientAggregation(),
        merging_agg.getMaxThreads(),
        merging_agg.getMemoryEfficientMergeThreads());
}


std::shared_ptr<MergeSortingStep> SymbolMapper::map(const MergeSortingStep & sorting)
{
    return std::make_shared<MergeSortingStep>(
        map(sorting.getInputStreams()[0]),
        SortDescription{map(sorting.getSortDescription())},
        sorting.getMaxMergedBlockSize(),
        sorting.getLimit(),
        sorting.getMaxBytesBeforeRemerge(),
        sorting.getRemergeLoweredMemoryBytesRatio(),
        sorting.getMaxBytesBeforeExternalSort(),
        sorting.getVolumPtr(),
        sorting.getMinFreeDiskSpace(),
        sorting.isAdaptiveSpillEnabled());
}

std::shared_ptr<MarkDistinctStep> SymbolMapper::map(const MarkDistinctStep & mark_distinct)
{
    return std::make_shared<MarkDistinctStep>(
        map(mark_distinct.getInputStreams()[0]), map(mark_distinct.getMarkerSymbol()), map(mark_distinct.getDistinctSymbols()));
}


std::shared_ptr<OffsetStep> SymbolMapper::map(const OffsetStep & offset)
{
    return std::make_shared<OffsetStep>(map(offset.getInputStreams()[0]), offset.getOffset());
}

std::shared_ptr<PartitionTopNStep> SymbolMapper::map(const PartitionTopNStep & partition_topn)
{
    return std::make_shared<PartitionTopNStep>(
        map(partition_topn.getInputStreams()[0]),
        map(partition_topn.getPartition()),
        map(partition_topn.getOrderBy()),
        partition_topn.getLimit(),
        partition_topn.getModel());
}

std::shared_ptr<PartialSortingStep> SymbolMapper::map(const PartialSortingStep & partial_sorting)
{
    return std::make_shared<PartialSortingStep>(
        map(partial_sorting.getInputStreams()[0]),
        SortDescription{partial_sorting.getSortDescription()},
        partial_sorting.getLimit(),
        partial_sorting.getSizeLimits());
}


std::shared_ptr<ProjectionStep> SymbolMapper::map(const ProjectionStep & projection)
{
    if (projection.isFinalProject())
    {
        Assignments assignments;
        for (const auto & item : projection.getAssignments())
            assignments.emplace(item.first, map(item.second));

        return std::make_shared<ProjectionStep>(
            map(projection.getInputStreams()[0]),
            std::move(assignments),
            projection.getNameToType(),
            projection.isFinalProject(),
            projection.isIndexProject(),
            projection.getHints());
    }
    return std::make_shared<ProjectionStep>(
        map(projection.getInputStreams()[0]),
        map(projection.getAssignments()),
        map(projection.getNameToType()),
        projection.isFinalProject(),
        projection.isIndexProject(),
        projection.getHints());
}

std::shared_ptr<ReadNothingStep> SymbolMapper::map(const ReadNothingStep & read_nothing)
{
    return std::make_shared<ReadNothingStep>(map(read_nothing.getOutputStream()).header);
}

std::shared_ptr<RemoteExchangeSourceStep> SymbolMapper::map(const RemoteExchangeSourceStep & remote_exchange)
{
    return std::make_shared<RemoteExchangeSourceStep>(
        remote_exchange.getInput(),
        map(remote_exchange.getInputStreams()[0]),
        remote_exchange.isAddTotals(),
        remote_exchange.isAddExtremes());
}


std::shared_ptr<SortingStep> SymbolMapper::map(const SortingStep & sorting)
{
    return std::make_shared<SortingStep>(
        map(sorting.getInputStreams()[0]),
        SortDescription{map(sorting.getSortDescription())},
        sorting.getLimit(),
        sorting.getStage(),
        SortDescription{map(sorting.getPrefixDescription())});
}

std::shared_ptr<TopNFilteringStep> SymbolMapper::map(const TopNFilteringStep & topn_filter)
{
    return std::make_shared<TopNFilteringStep>(
        map(topn_filter.getInputStreams()[0]),
        SortDescription{topn_filter.getSortDescription()},
        topn_filter.getSize(),
        topn_filter.getModel(),
        topn_filter.getAlgorithm());
}


std::shared_ptr<UnionStep> SymbolMapper::map(const UnionStep & union_)
{
    std::unordered_map<String, std::vector<String>> outputs_to_inputs;
    for (const auto & [output, inputs] : union_.getOutToInputs())
    {
        auto mapped_inputs = map(inputs);
        outputs_to_inputs.emplace(map(output), mapped_inputs);
    }
    return std::make_shared<UnionStep>(
        map(union_.getInputStreams()), map(union_.getOutputStream()), outputs_to_inputs, union_.getMaxThreads(), union_.isLocal());
}

std::shared_ptr<ValuesStep> SymbolMapper::map(const ValuesStep & values)
{
    return std::make_shared<ValuesStep>(map(values.getOutputStream().header), values.getFields(), values.getRows());
}

std::shared_ptr<WindowStep> SymbolMapper::map(const WindowStep & window)
{
    return std::make_shared<WindowStep>(
        map(window.getInputStreams()[0]),
        map(window.getWindowDescription()),
        map(window.getFunctions()),
        window.needSort(),
        SortDescription{map(window.getPrefixDescription())});
}
std::shared_ptr<CTERefStep> SymbolMapper::map(const CTERefStep & cte_ref)
{
    std::unordered_map<String, String> output_columns;
    for (const auto & item : cte_ref.getOutputColumns())
    {
        output_columns.emplace(map(item.first), item.second);
    }
    return std::make_shared<CTERefStep>(map(cte_ref.getOutputStream()), cte_ref.getId(), output_columns, cte_ref.hasFilter());
}

std::shared_ptr<ExplainAnalyzeStep> SymbolMapper::map(const ExplainAnalyzeStep & step)
{
    return std::make_shared<ExplainAnalyzeStep>(
        map(step.getInputStreams()[0]),
        map(step.getOutputName()),
        step.getKind(),
        step.getContext(),
        step.getQueryPlan(),
        step.getSetting());
}

std::shared_ptr<LocalExchangeStep> SymbolMapper::map(const LocalExchangeStep & step)
{
    return std::make_shared<LocalExchangeStep>(map(step.getInputStreams()[0]), step.getExchangeMode(), map(step.getSchema()));
}

std::shared_ptr<TableWriteStep> SymbolMapper::map(const TableWriteStep & step)
{
    return std::make_shared<TableWriteStep>(map(step.getInputStreams()[0]), step.getTarget(), step.isOutputProfiles());
}

std::shared_ptr<OutfileWriteStep> SymbolMapper::map(const OutfileWriteStep & step)
{
    return std::make_shared<OutfileWriteStep>(map(step.getInputStreams()[0]), step.outfile_target);
}

std::shared_ptr<OutfileFinishStep> SymbolMapper::map(const OutfileFinishStep & step)
{
    return std::make_shared<OutfileFinishStep>(map(step.getInputStreams()[0]));
}

std::shared_ptr<ReadStorageRowCountStep> SymbolMapper::map(const ReadStorageRowCountStep & step)
{
    return std::make_shared<ReadStorageRowCountStep>(
        map(step.getOutputStream().header), step.getQuery(), step.getAggregateDescription(), step.getNumRows(), step.isFinal(), step.getDatabaseAndTableName());
}

std::shared_ptr<BufferStep> SymbolMapper::map(const BufferStep & step)
{
    return std::make_shared<BufferStep>(map(step.getInputStreams()[0]));
}

std::shared_ptr<TableFinishStep> SymbolMapper::map(const TableFinishStep & step)
{
    return std::make_shared<TableFinishStep>(
        map(step.getInputStreams()[0]), step.getTarget(), step.getOutputAffectedRowCountSymbol(), step.getQuery(), step.isOutputProfiles());
}

std::shared_ptr<IntermediateResultCacheStep> SymbolMapper::map(const IntermediateResultCacheStep & step)
{
    return std::make_shared<IntermediateResultCacheStep>(map(step.getInputStreams()[0]), step.getCacheParam(), step.getAggregatorParams());
}

std::shared_ptr<MultiJoinStep> SymbolMapper::map(const MultiJoinStep & step)
{
    return std::make_shared<MultiJoinStep>(map(step.getOutputStream()), step.getGraph());
}

std::shared_ptr<TotalsHavingStep> SymbolMapper::map(const TotalsHavingStep & step)
{
    return std::make_shared<TotalsHavingStep>(
        map(step.getInputStreams()[0]),
        step.isOverflowRow(),
        map(step.getHavingFilter()),
        step.getTotalsMode(),
        step.getAutoIncludeThreshols(),
        step.isFinal());
}

std::shared_ptr<ExpandStep> SymbolMapper::map(const ExpandStep & step)
{
    return std::make_shared<ExpandStep>(
        map(step.getOutputStream()),
        map(step.getAssignments()),
        map(step.getNameToType()),
        map(step.getGroupIdSymbol()),
        step.getGroupIdValue(),
        map(step.getGroupIdNonNullSymbol()));
}

class SymbolMapper::SymbolMapperVisitor : public StepVisitor<QueryPlanStepPtr, SymbolMapper>
{
protected:
#define VISITOR_DEF(TYPE) \
    QueryPlanStepPtr visit##TYPE##Step(const TYPE##Step & step, SymbolMapper & mapper) override { return mapper.map(step); }
    APPLY_STEP_TYPES(VISITOR_DEF)
#undef VISITOR_DEF
};

QueryPlanStepPtr SymbolMapper::map(const IQueryPlanStep & step)
{
    SymbolMapperVisitor visitor;
    return VisitorUtil::accept(step, visitor, *this);
}
}
