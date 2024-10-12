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

#include <QueryPlan/AggregatingStep.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/SettingsEnums.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/grouping.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/IntermediateResult/CacheManager.h>
#include <Processors/Merges/AggregatingSortedTransform.h>
#include <Processors/Merges/FinishAggregatingInOrderTransform.h>
#include <Processors/QueryPipeline.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/Transforms/AggregatingInOrderTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/CopyTransform.h>
#include <Processors/Transforms/CubeTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/RollupTransform.h>
#include <Processors/Transforms/RollupWithGroupingTransform.h>
#include <QueryPlan/PlanSerDerHelper.h>
#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <common/logger_useful.h>

#include <algorithm>
#include <unordered_set>

namespace DB
{
bool hasNonParallelAggregateFunctions(const AggregateDescriptions & aggregates)
{
    for (const auto & agg: aggregates)
    {
        auto agg_name = agg.function->getName();
        // if (startsWith(agg_name, "finderFunnel") ||
        //     startsWith(agg_name, "finderGroupFunnel") ||
        //     startsWith(agg_name, "pathSplit") ||
        //     startsWith(agg_name, "pathCount") ||
        //     startsWith(agg_name, "retention") ||
        //     startsWith(agg_name, "attributionAnalysis") ||
        //     startsWith(agg_name, "attributionCorrelation") ||
        //     startsWith(agg_name, "funnelRep") ||
        //     agg_name == "sessionAnalysis" ||
        //     agg_name == "vSessionAnalysis")
        //     return true;

        if (startsWith(agg_name, "pathCount") ||
            startsWith(agg_name, "attributionCorrelation") ||
            startsWith(agg_name, "funnelRep"))
            return true;
    }

    return false;
}

static ITransformingStep::Traits getTraits(bool should_produce_results_in_order_of_bucket_number)
{
    return ITransformingStep::Traits{
        {
            .preserves_distinct_columns = false, /// Actually, we may check that distinct names are in aggregation keys
            .returns_single_stream
            = should_produce_results_in_order_of_bucket_number, /// Actually, may also return single stream if should_produce_results_in_order_of_bucket_number = false
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }};
}

void computeGroupingFunctions(
    QueryPipeline & pipeline,
    const GroupingDescriptions & groupings,
    const Names & keys,
    const GroupingSetsParamsList & grouping_set_params,
    const BuildQueryPipelineSettings & build_settings)
{
    if (groupings.empty())
        return;

    const bool ansi_mode = build_settings.context->getSettingsRef().dialect_type != DialectType::CLICKHOUSE;
    bool force_grouping_standard_compatibility = build_settings.context->getSettingsRef().force_grouping_standard_compatibility;
    if (ansi_mode)
        force_grouping_standard_compatibility = true;

    auto actions = std::make_shared<ActionsDAG>(pipeline.getHeader().getColumnsWithTypeAndName());
    ActionsDAG::NodeRawConstPtrs outputs = actions->getOutputs();
    ActionsDAG::NodeRawConstPtrs children;

    if (!grouping_set_params.empty())
        children.push_back(&actions->findInOutputs("__grouping_set"));

    auto get_key_index = [&](const String & key) { return std::find(keys.begin(), keys.end(), key) - keys.begin(); };

    ColumnNumbersList grouping_sets_indices;
    for (const auto & param : grouping_set_params)
    {
        ColumnNumbers indices;

        if (!param.used_keys.empty() || !param.missing_keys.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unable to calculate grouping functions by prepared grouping set param");

        for (const auto & key_name : param.used_key_names)
            indices.push_back(get_key_index(key_name));

        grouping_sets_indices.emplace_back(std::move(indices));
    }

    for (const auto & grouping : groupings)
    {
        ColumnNumbers arguments_indexes;

        for (const auto & arg : grouping.argument_names)
            arguments_indexes.push_back(get_key_index(arg));

        if (!grouping_set_params.empty()) // GROUPING SETS, ROLLUP, CUBE
            outputs.push_back(&actions->addFunction(
                std::make_shared<FunctionToOverloadResolverAdaptor>(
                    std::make_shared<FunctionGroupingForGroupingSets>(std::move(arguments_indexes), grouping_sets_indices, force_grouping_standard_compatibility)),
                children,
                grouping.output_name));
        else // ORDINARY
            outputs.push_back(&actions->addFunction(
                std::make_shared<FunctionToOverloadResolverAdaptor>(
                    std::make_shared<FunctionGroupingOrdinary>(std::move(arguments_indexes), force_grouping_standard_compatibility)),
                children,
                grouping.output_name));
    }

    actions->getOutputs().swap(outputs);
    auto expression = std::make_shared<ExpressionActions>(actions, build_settings.getActionsSettings());
    pipeline.addSimpleTransform([&](const Block & header) { return std::make_shared<ExpressionTransform>(header, expression); });
}

Block appendGroupingSetColumn(Block header)
{
    Block res;
    res.insert({std::make_shared<DataTypeUInt64>(), "__grouping_set"});

    for (auto & col : header)
        res.insert(std::move(col));

    return res;
}

static Block
appendGroupingColumns(Block block, const GroupingSetsParamsList & grouping_set_params, const GroupingDescriptions & groupings, bool final)
{
    Block res;

    if (!grouping_set_params.empty())
    {
        size_t rows = block.rows();
        auto column = ColumnUInt64::create(rows);

        res.insert({ColumnPtr(std::move(column)), std::make_shared<DataTypeUInt64>(), "__grouping_set"});
    }

    for (auto & col : block)
        res.insert(std::move(col));

    if (final) /* TODO: @jingpeng remove `rollup` check if rollup can be rewritten to grouping sets */
    {
        for (const auto & grouping : groupings)
        {
            DataTypePtr type;

            type = std::make_shared<DataTypeUInt64>();

            res.insert({std::move(type), grouping.output_name});
        }
    }
    return res;
}

Aggregator::Params
AggregatingStep::createParams(Block header_before_aggregation, AggregateDescriptions aggregates, Names group_by_keys, bool overflow_row)
{
    ColumnNumbers keys;
    for (const auto & key : group_by_keys)
        keys.push_back(header_before_aggregation.getPositionByName(key));

    for (auto & descr : aggregates)
    {
        descr.arguments.clear();
        DataTypes argument_types;
        for (const auto & name : descr.argument_names)
        {
            descr.arguments.push_back(header_before_aggregation.getPositionByName(name));
            if (descr.mask_column == name)
            {
                argument_types.emplace_back(std::make_shared<DataTypeUInt8>());
            }
            else
            {
                argument_types.emplace_back(header_before_aggregation.getDataTypes()[header_before_aggregation.getPositionByName(name)]);
            }
        }

        // For AggregateFunctionNothing, the argument types in `header_before_aggregation` may diff with
        // the ones in `descr.function->argument_types`. In this case, reconstructing aggregate description will lead
        // to a different result.
        //
        // example: SELECT count(in(NULL, []));
        // After the first call(see AggregateFunctionNull::transformAggregateFunction) to AggregateFunctionFactory::instance().get, count(Nullable(Nothing)) --> Nothing(UInt64).
        // However, the new argument types below will be Nullable(Nothing).
        // Nothing(Nullable(Nothing)) --after second AggregateFunctionFactory::instance().get--> Nothing(Nullable(Nothing)).
        // As a result, the output will be Null instead of 0, but 0 was the expected result.
        if (descr.function->getName() == "nothing")
        {
            bool is_before_argument_uint64 = descr.function->getArgumentTypes().size() == 1 && descr.function->getArgumentTypes()[0]->getTypeId() == TypeIndex::UInt64;
            bool is_after_argument_only_null = argument_types.size() == 1 && argument_types[0]->onlyNull();
            if (is_before_argument_uint64 && is_after_argument_only_null)
            {
                argument_types[0] = std::make_shared<DataTypeUInt64>();
            }
        }

        AggregateFunctionProperties properties;
        descr.function = AggregateFunctionFactory::instance().get(descr.function->getName(), argument_types, descr.parameters, properties);
    }


    return Aggregator::Params(
        header_before_aggregation, keys, aggregates, overflow_row, 0, OverflowMode::THROW,
        0,
        0,
        0,
        false,
        10485760,
        false, nullptr, 0, 0, false, 0);
}

GroupingSetsParamsList AggregatingStep::prepareGroupingSetsParams() const
{
    bool is_prepared = std::any_of(grouping_sets_params.cbegin(), grouping_sets_params.cend(), [](const auto & p) {
        return !p.used_keys.empty() || !p.missing_keys.empty();
    });

    if (is_prepared)
        return grouping_sets_params;

    ColumnNumbers all_keys_index;
    const auto & input_header = input_streams.front().header;

    for (const auto & key : keys)
        all_keys_index.push_back(input_header.getPositionByName(key));

    GroupingSetsParamsList result;

    for (const auto & param : grouping_sets_params)
    {
        ColumnNumbers used_indexes;
        std::unordered_set<size_t> used_indexes_set;
        for (const auto & key_name : param.used_key_names)
        {
            used_indexes.push_back(input_header.getPositionByName(key_name));
            used_indexes_set.insert(used_indexes.back());
        }

        ColumnNumbers missing_indexes;
        for (size_t i = 0; i < all_keys_index.size(); ++i)
        {
            if (!used_indexes_set.count(all_keys_index[i]))
                missing_indexes.push_back(i);
        }
        result.emplace_back(std::move(used_indexes), std::move(missing_indexes));
    }

    return result;
}

AggregatingStep::AggregatingStep(
    const DataStream & input_stream_,
    Names keys_,
    const NameSet & keys_not_hashed_,
    Aggregator::Params params_,
    GroupingSetsParamsList grouping_sets_params_,
    bool final_,
    AggregateStagePolicy stage_policy_,
    size_t max_block_size_,
    size_t merge_threads_,
    size_t temporary_data_merge_threads_,
    bool storage_has_evenly_distributed_read_,
    InputOrderInfoPtr group_by_info_,
    SortDescription group_by_sort_description_,
    GroupingDescriptions groupings_,
    bool,
    bool should_produce_results_in_order_of_bucket_number_,
    bool no_shuffle_,
    bool streaming_for_cache_,
    PlanHints hints_)
    : ITransformingStep(
        input_stream_,
        appendGroupingColumns(params_.getHeader(final_), grouping_sets_params_, groupings_, final_),
        getTraits(should_produce_results_in_order_of_bucket_number_),
        false,
        hints_)
    , keys(std::move(keys_))
    , keys_not_hashed(keys_not_hashed_)
    , params(std::move(params_))
    , grouping_sets_params(std::move(grouping_sets_params_))
    , final(final_)
    , stage_policy(stage_policy_)
    , max_block_size(max_block_size_)
    , merge_threads(merge_threads_)
    , temporary_data_merge_threads(temporary_data_merge_threads_)
    , storage_has_evenly_distributed_read(storage_has_evenly_distributed_read_)
    , group_by_info(std::move(group_by_info_))
    , group_by_sort_description(std::move(group_by_sort_description_))
    , groupings(groupings_)
    , should_produce_results_in_order_of_bucket_number(should_produce_results_in_order_of_bucket_number_)
    , streaming_for_cache(streaming_for_cache_)
    , no_shuffle(no_shuffle_)

{
    NameSet output_names;
    for (const auto & key : keys)
        if (!output_names.emplace(key).second)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "duplicate group by key: {}", key);

    for (const auto & aggregate : params.aggregates)
        if (!output_names.emplace(aggregate.column_name).second)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "duplicate aggreagte function output name: {}", aggregate.column_name);

    //    final = final && !totals && !cube & !rollup;
    setInputStreams(input_streams);
}

void AggregatingStep::setInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
    // TODO: what if input_streams and params->getHeader() are inconsistent?
    output_stream->header = appendGroupingColumns(params.getHeader(final), grouping_sets_params, groupings, final);
}

void AggregatingStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & build_settings)
{
    auto cache_holder = pipeline.getCacheHolder();
    if (!cache_holder || cache_holder->all_part_in_storage)
        streaming_for_cache = false;

    QueryPipelineProcessorsCollector collector(pipeline, this);
    const auto & settings = build_settings.context->getSettingsRef();
    this->max_block_size = settings.max_block_size;
    this->temporary_data_merge_threads = settings.aggregation_memory_efficient_merge_threads
        ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads)
        : static_cast<size_t>(settings.max_threads);

    if (isFinal() && hasNonParallelAggregateFunctions(params.aggregates))
    {
        pipeline.resize(1);
    }

    auto merge_max_threads = merge_threads == 0 ? settings.max_threads : merge_threads;
    max_block_size = max_block_size == 0 ? settings.max_block_size : max_block_size;

    NameSet mask_columns;
    for (const auto & descr : params.aggregates)
    {
        if (!descr.mask_column.empty())
        {
            mask_columns.insert(descr.mask_column);
        }
    }

    if (!mask_columns.empty())
    {
        ASTPtr expr_list = std::make_shared<ASTExpressionList>();
        NamesWithAliases output;
        for (const auto & column : getInputStreams()[0].header)
        {
            if (mask_columns.contains(column.name))
            {
                ASTPtr true_predicate
                    = makeASTFunction("equals", std::make_shared<ASTIdentifier>(column.name), std::make_shared<ASTLiteral>(1));
                ASTPtr true_value = std::make_shared<ASTLiteral>(1);
                ASTPtr false_predicate
                    = makeASTFunction("equals", std::make_shared<ASTIdentifier>(column.name), std::make_shared<ASTLiteral>(0));
                ASTPtr false_value = std::make_shared<ASTLiteral>(0);
                ASTPtr else_value = std::make_shared<ASTLiteral>(0);
                auto multi_if = makeASTFunction("multiIf", true_predicate, true_value, false_predicate, false_value, else_value);
                auto cast = makeASTFunction("cast", multi_if, std::make_shared<ASTLiteral>("UInt8"));
                expr_list->children.emplace_back(cast);
                output.emplace_back(NameWithAlias{cast->getColumnName(), column.name});
            }
            else
            {
                expr_list->children.emplace_back(std::make_shared<ASTIdentifier>(column.name));
                output.emplace_back(NameWithAlias{column.name, column.name});
            }
        }
        auto action = createExpressionActions(
            build_settings.context, NamesAndTypesList{input_streams[0].header.getNamesAndTypesList()}, output, expr_list);
        auto expression = std::make_shared<ExpressionActions>(action, build_settings.getActionsSettings());
        pipeline.addSimpleTransform([&](const Block & header) { return std::make_shared<ExpressionTransform>(header, expression); });
    }

    ColumnNumbers key_index;
    if (keys.empty())
        key_index = params.keys;
    auto before_agg_header = pipeline.getHeader();
    for (const auto & name : keys)
        key_index.push_back(before_agg_header.getPositionByName(name));

    AggregateDescriptions new_aggregates = params.aggregates;
    for (auto & descr : new_aggregates)
    {
        descr.arguments.clear();
        for (const auto & name : descr.argument_names)
            descr.arguments.push_back(before_agg_header.getPositionByName(name));
    }

    auto new_params = Aggregator::Params(
        before_agg_header,
        key_index,
        new_aggregates,
        params.overflow_row,
        settings.max_rows_to_group_by,
        settings.group_by_overflow_mode,
        settings.group_by_two_level_threshold,
        settings.group_by_two_level_threshold_bytes,
        settings.max_bytes_before_external_group_by,
        settings.spill_mode == SpillMode::AUTO,
        settings.spill_buffer_bytes_before_external_group_by,
        params.empty_result_for_aggregation_by_empty_set || settings.empty_result_for_aggregation_by_empty_set,
        build_settings.context->getTemporaryVolume(),
        settings.max_threads,
        settings.min_free_disk_space_for_temporary_data,
        settings.compile_aggregate_expressions,
        settings.min_count_to_compile_aggregate_expression,
        {},
        settings.enable_lc_group_by_opt);

    /// Forget about current totals and extremes. They will be calculated again after aggregation if needed.
    pipeline.dropTotalsAndExtremes();
    bool agg_final = final;

    bool allow_to_use_two_level_group_by = pipeline.getNumStreams() > 1 || new_params.max_bytes_before_external_group_by != 0;
    if (!allow_to_use_two_level_group_by)
    {
        new_params.group_by_two_level_threshold = 0;
        new_params.group_by_two_level_threshold_bytes = 0;
    }

    /** Two-level aggregation is useful in two cases:
      * 1. Parallel aggregation is done, and the results should be merged in parallel.
      * 2. An aggregation is done with store of temporary data on the disk, and they need to be merged in a memory efficient way.
      */
    auto transform_params = std::make_shared<AggregatingTransformParams>(std::move(new_params), agg_final);

    if (!grouping_sets_params.empty())
    {
        const auto prepared_sets_params = prepareGroupingSetsParams();
        const size_t grouping_sets_size = prepared_sets_params.size();

        const size_t streams = pipeline.getNumStreams();

        auto input_header = pipeline.getHeader();

        if (grouping_sets_size > 1)
        {
            pipeline.transform([&](OutputPortRawPtrs ports) {
                Processors copiers;
                copiers.reserve(ports.size());

                for (auto * port : ports)
                {
                    auto copier = std::make_shared<CopyTransform>(input_header, grouping_sets_size);
                    connect(*port, copier->getInputPort());
                    copiers.push_back(copier);
                }

                return copiers;
            });
        }

        pipeline.transform([&](OutputPortRawPtrs ports) {
            assert(streams * grouping_sets_size == ports.size());
            Processors processors;
            for (size_t i = 0; i < grouping_sets_size; ++i)
            {
                Aggregator::Params params_for_set{
                    transform_params->params.src_header,
                    prepared_sets_params[i].used_keys,
                    transform_params->params.aggregates,
                    transform_params->params.overflow_row,
                    transform_params->params.max_rows_to_group_by,
                    transform_params->params.group_by_overflow_mode,
                    transform_params->params.group_by_two_level_threshold,
                    transform_params->params.group_by_two_level_threshold_bytes,
                    transform_params->params.max_bytes_before_external_group_by,
                    transform_params->params.enable_adaptive_spill,
                    transform_params->params.spill_buffer_bytes_before_external_group_by,
                    /// Return empty result when aggregating without keys on empty set, if ansi
                    settings.dialect_type != DialectType::CLICKHOUSE ? true : transform_params->params.empty_result_for_aggregation_by_empty_set,
                    transform_params->params.tmp_volume,
                    transform_params->params.max_threads,
                    transform_params->params.min_free_disk_space,
                    transform_params->params.compile_aggregate_expressions,
                    transform_params->params.min_count_to_compile_aggregate_expression,
                    {},
                    transform_params->params.enable_lc_group_by_opt};
                using TwoLevelMode = Aggregator::Params::TwoLevelMode;
                params_for_set.two_level_mode
                    = settings.group_by_two_level_for_grouping_set ? TwoLevelMode::ENFORCE_TWO_LEVEL : TwoLevelMode::ENFORCE_SINGLE_LEVEL;
                auto transform_params_for_set = std::make_shared<AggregatingTransformParams>(std::move(params_for_set), final);

                if (streams > 1)
                {
                    auto many_data = std::make_shared<ManyAggregatedData>(streams);
                    for (size_t j = 0; j < streams; ++j)
                    {
                        auto aggregation_for_set = std::make_shared<AggregatingTransform>(
                            input_header, transform_params_for_set, many_data, j, merge_max_threads, temporary_data_merge_threads);
                        // For each input stream we have `grouping_sets_size` copies, so port index
                        // for transform #j should skip ports of first (j-1) streams.
                        connect(*ports[i + grouping_sets_size * j], aggregation_for_set->getInputs().front());
                        ports[i + grouping_sets_size * j] = &aggregation_for_set->getOutputs().front();
                        processors.push_back(aggregation_for_set);
                    }
                }
                else
                {
                    auto aggregation_for_set = std::make_shared<AggregatingTransform>(input_header, transform_params_for_set);
                    connect(*ports[i], aggregation_for_set->getInputs().front());
                    ports[i] = &aggregation_for_set->getOutputs().front();
                    processors.push_back(aggregation_for_set);
                }
            }

            if (streams > 1)
            {
                OutputPortRawPtrs new_ports;
                new_ports.reserve(grouping_sets_size);

                for (size_t i = 0; i < grouping_sets_size; ++i)
                {
                    size_t output_it = i;
                    auto resize = std::make_shared<ResizeProcessor>(ports[output_it]->getHeader(), streams, 1);
                    auto & inputs = resize->getInputs();

                    for (auto input_it = inputs.begin(); input_it != inputs.end(); output_it += grouping_sets_size, ++input_it)
                        connect(*ports[output_it], *input_it);
                    new_ports.push_back(&resize->getOutputs().front());
                    processors.push_back(resize);
                }

                ports.swap(new_ports);
            }

            assert(ports.size() == grouping_sets_size);
            auto output_header = transform_params->getHeader();

            for (size_t set_counter = 0; set_counter < grouping_sets_size; ++set_counter)
            {
                const auto & header = ports[set_counter]->getHeader();

                /// Here we create a DAG which fills missing keys and adds `__grouping_set` column
                auto dag = std::make_shared<ActionsDAG>(header.getColumnsWithTypeAndName());
                ActionsDAG::NodeRawConstPtrs outputs;
                outputs.reserve(output_header.columns() + 1);

                auto grouping_col = ColumnConst::create(ColumnUInt64::create(1, set_counter), 0);
                const auto * grouping_node
                    = &dag->addColumn({ColumnPtr(std::move(grouping_col)), std::make_shared<DataTypeUInt64>(), "__grouping_set"});

                grouping_node = &dag->materializeNode(*grouping_node);
                outputs.push_back(grouping_node);

                size_t missign_column_index = 0;
                const auto & missing_columns = prepared_sets_params[set_counter].missing_keys;

                for (size_t i = 0; i < output_header.columns(); ++i)
                {
                    auto & col = output_header.getByPosition(i);
                    if (missign_column_index < missing_columns.size() && missing_columns[missign_column_index] == i)
                    {
                        ++missign_column_index;
                        auto column_with_default = col.column->cloneEmpty();
                        col.type->insertDefaultInto(*column_with_default);
                        auto column = ColumnConst::create(std::move(column_with_default), 0);
                        const auto * node = &dag->addColumn({ColumnPtr(std::move(column)), col.type, col.name});
                        node = &dag->materializeNode(*node);
                        outputs.push_back(node);
                    }
                    else
                        outputs.push_back(dag->getOutputs()[header.getPositionByName(col.name)]);
                }

                dag->getOutputs().swap(outputs);
                auto expression = std::make_shared<ExpressionActions>(dag, build_settings.getActionsSettings());
                auto transform = std::make_shared<ExpressionTransform>(header, expression);

                connect(*ports[set_counter], transform->getInputPort());
                processors.emplace_back(std::move(transform));
            }

            return processors;
        });

        if (final)
            computeGroupingFunctions(pipeline, groupings, keys, grouping_sets_params, build_settings);

        aggregating = collector.detachProcessors(0);
        return;
    }


    if (group_by_info)
    {
        bool need_finish_sorting = (group_by_info->order_key_prefix_descr.size() < group_by_sort_description.size());

        if (need_finish_sorting)
        {
            /// TOO SLOW
        }
        else
        {
            if (pipeline.getNumStreams() > 1)
            {
                auto many_data = std::make_shared<ManyAggregatedData>(pipeline.getNumStreams());
                size_t counter = 0;
                pipeline.addSimpleTransform([&](const Block & header) {
                    return std::make_shared<AggregatingInOrderTransform>(
                        header, transform_params, group_by_sort_description, max_block_size, many_data, counter++);
                });

                aggregating_in_order = collector.detachProcessors(0);

                for (auto & column_description : group_by_sort_description)
                {
                    if (!column_description.column_name.empty())
                    {
                        column_description.column_number = pipeline.getHeader().getPositionByName(column_description.column_name);
                        column_description.column_name.clear();
                    }
                }

                auto transform = std::make_shared<FinishAggregatingInOrderTransform>(
                    pipeline.getHeader(), pipeline.getNumStreams(), transform_params, group_by_sort_description, max_block_size);

                pipeline.addTransform(std::move(transform));
                aggregating_sorted = collector.detachProcessors(1);
            }
            else
            {
                pipeline.addSimpleTransform([&](const Block & header) {
                    return std::make_shared<AggregatingInOrderTransform>(
                        header, transform_params, group_by_sort_description, max_block_size);
                });

                aggregating_in_order = collector.detachProcessors(0);
            }

            pipeline.addSimpleTransform(
                [&](const Block & header) { return std::make_shared<FinalizingSimpleTransform>(header, transform_params); });

            finalizing = collector.detachProcessors(2);
            return;
        }
    }
    bool can_streaming_agg = streaming_for_cache && !transform_params->only_merge;

    /// If there are several sources, then we perform parallel aggregation
    if (pipeline.getNumStreams() > 1)
    {
        /// Add resize transform to uniformly distribute data between aggregating streams.
        if (!storage_has_evenly_distributed_read && !can_streaming_agg)
            pipeline.resize(pipeline.getNumStreams(), true, true);
        if (can_streaming_agg)
        {
            pipeline.addSimpleTransform([&](const Block & header) {
                return std::make_shared<AggregatingStreamingTransform>(
                    header,
                    transform_params,
                    settings.streaming_agg_local_ratio,
                    false,
                    settings.enable_intermediate_result_cache_streaming,
                    streaming_for_cache,
                    false,
                    final);
            });
        }
        else
        {
            auto many_data = std::make_shared<ManyAggregatedData>(pipeline.getNumStreams());

            size_t counter = 0;
            pipeline.addSimpleTransform([&](const Block & header) {
                return std::make_shared<AggregatingTransform>(
                    header, transform_params, many_data, counter++, merge_max_threads, temporary_data_merge_threads);
            });
        }
        /// Streaming agg no need resize here
        if (!can_streaming_agg)
        {
            /// We add the explicit resize here, but not in case of aggregating in order, since AIO don't use two-level hash tables and thus returns only buckets with bucket_number = -1.
            pipeline.resize(should_produce_results_in_order_of_bucket_number ? 1 : pipeline.getNumStreams(), true /* force */);
        }
        aggregating = collector.detachProcessors(0);
    }
    else
    {
        pipeline.resize(1);

        if (can_streaming_agg)
            pipeline.addSimpleTransform([&](const Block & header) {
                return std::make_shared<AggregatingStreamingTransform>(
                    header,
                    transform_params,
                    settings.streaming_agg_local_ratio,
                    false,
                    settings.enable_intermediate_result_cache_streaming,
                    streaming_for_cache);
            });
        else
            pipeline.addSimpleTransform([&](const Block & header) { return std::make_shared<AggregatingTransform>(header, transform_params); });

        aggregating = collector.detachProcessors(0);
    }

    if (final)
        computeGroupingFunctions(pipeline, groupings, keys, grouping_sets_params, build_settings);
}

void AggregatingStep::describeActions(FormatSettings & settings) const
{
    params.explain(settings.out, settings.offset);
}

void AggregatingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    params.explain(map);
}

void AggregatingStep::describePipeline(FormatSettings & settings) const
{
    if (!aggregating.empty())
        IQueryPlanStep::describePipeline(aggregating, settings);
    else
    {
        /// Processors are printed in reverse order.
        IQueryPlanStep::describePipeline(finalizing, settings);
        IQueryPlanStep::describePipeline(aggregating_sorted, settings);
        IQueryPlanStep::describePipeline(aggregating_in_order, settings);
    }
}

std::shared_ptr<IQueryPlanStep> AggregatingStep::copy(ContextPtr) const
{
    return std::make_shared<AggregatingStep>(
        input_streams[0],
        keys,
        keys_not_hashed,
        params.aggregates,
        grouping_sets_params,
        final,
        stage_policy,
        group_by_sort_description,
        groupings,
        needOverflowRow(),
        should_produce_results_in_order_of_bucket_number,
        no_shuffle,
        streaming_for_cache,
        hints);
}

void GroupingSetsParams::toProto(Protos::GroupingSetsParams & proto) const
{
    for (const auto & element : used_key_names)
        proto.add_used_key_names(element);
    for (const auto & element : used_keys)
        proto.add_used_keys(element);
    for (const auto & element : missing_keys)
        proto.add_missing_keys(element);
}

void GroupingSetsParams::fillFromProto(const Protos::GroupingSetsParams & proto)
{
    for (const auto & element : proto.used_key_names())
        used_key_names.emplace_back(element);
    for (const auto & element : proto.used_keys())
        used_keys.emplace_back(element);
    for (const auto & element : proto.missing_keys())
        missing_keys.emplace_back(element);
}

void GroupingDescription::toProto(Protos::GroupingDescription & proto) const
{
    for (const auto & element : argument_names)
        proto.add_argument_names(element);
    proto.set_output_name(output_name);
}

void GroupingDescription::fillFromProto(const Protos::GroupingDescription & proto)
{
    for (const auto & element : proto.argument_names())
        argument_names.emplace_back(element);
    output_name = proto.output_name();
}

void AggregatingStep::toProto(Protos::AggregatingStep & proto, bool) const
{
    ITransformingStep::serializeToProtoBase(*proto.mutable_query_plan_base());
    for (const auto & element : keys)
        proto.add_keys(element);

    Names ordered_keys_not_hashed(keys_not_hashed.begin(), keys_not_hashed.end());
    std::sort(ordered_keys_not_hashed.begin(), ordered_keys_not_hashed.end());
    for (const auto & element : ordered_keys_not_hashed)
        proto.add_keys_not_hashed(element);

    params.toProto(*proto.mutable_params());
    for (const auto & element : grouping_sets_params)
        element.toProto(*proto.add_grouping_sets_params());
    proto.set_final(final);
    proto.set_max_block_size(max_block_size);
    proto.set_merge_threads(merge_threads);
    proto.set_temporary_data_merge_threads(temporary_data_merge_threads);
    proto.set_storage_has_evenly_distributed_read(storage_has_evenly_distributed_read);

    if (group_by_info)
        group_by_info->toProto(*proto.mutable_group_by_info());
    for (const auto & element : group_by_sort_description)
        element.toProto(*proto.add_group_by_sort_description());
    for (const auto & element : groupings)
        element.toProto(*proto.add_groupings());
    proto.set_should_produce_results_in_order_of_bucket_number(should_produce_results_in_order_of_bucket_number);
    proto.set_streaming_for_cache(streaming_for_cache);

}

std::shared_ptr<AggregatingStep> AggregatingStep::fromProto(const Protos::AggregatingStep & proto, ContextPtr context)
{
    auto [step_description, base_input_stream] = ITransformingStep::deserializeFromProtoBase(proto.query_plan_base());
    Names keys;
    for (const auto & element : proto.keys())
        keys.emplace_back(element);
    NameSet keys_not_hashed;
    for (const auto & element : proto.keys_not_hashed())
        keys_not_hashed.emplace(element);
    auto params = Aggregator::Params::fromProto(proto.params(), context);
    GroupingSetsParamsList grouping_sets_params;
    for (const auto & proto_element : proto.grouping_sets_params())
    {
        GroupingSetsParams element;
        element.fillFromProto(proto_element);
        grouping_sets_params.emplace_back(std::move(element));
    }
    auto final = proto.final();
    auto max_block_size = proto.max_block_size();
    auto merge_threads = proto.merge_threads();
    auto temporary_data_merge_threads = proto.temporary_data_merge_threads();
    auto storage_has_evenly_distributed_read = proto.storage_has_evenly_distributed_read();

    InputOrderInfoPtr group_by_info = nullptr;
    if (proto.has_group_by_info())
        group_by_info = InputOrderInfo::fromProto(proto.group_by_info());
    SortDescription group_by_sort_description;
    for (const auto & proto_element : proto.group_by_sort_description())
    {
        SortColumnDescription element;
        element.fillFromProto(proto_element);
        group_by_sort_description.emplace_back(std::move(element));
    }
    GroupingDescriptions groupings;
    for (const auto & proto_element : proto.groupings())
    {
        GroupingDescription element;
        element.fillFromProto(proto_element);
        groupings.emplace_back(std::move(element));
    }
    auto should_produce_results_in_order_of_bucket_number = proto.should_produce_results_in_order_of_bucket_number();
    auto streaming_for_cache = proto.streaming_for_cache();

    auto step = std::make_shared<AggregatingStep>(
        base_input_stream,
        keys,
        keys_not_hashed,
        params,
        grouping_sets_params,
        final,
        AggregateStagePolicy::DEFAULT,
        max_block_size,
        merge_threads,
        temporary_data_merge_threads,
        storage_has_evenly_distributed_read,
        group_by_info,
        group_by_sort_description,
        groupings,
        false,
        should_produce_results_in_order_of_bucket_number,
        false,
        streaming_for_cache);
    step->setStepDescription(step_description);
    return step;
}

}
