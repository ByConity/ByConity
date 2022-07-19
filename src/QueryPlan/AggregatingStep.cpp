#include <memory>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Merges/AggregatingSortedTransform.h>
#include <Processors/Merges/FinishAggregatingInOrderTransform.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/AggregatingInOrderTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/CubeTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/RollupTransform.h>
#include <Processors/Transforms/RollupWithGroupingTransform.h>
#include <QueryPlan/AggregatingStep.h>
#include <QueryPlan/PlanSerDerHelper.h>

#include <Core/ColumnWithTypeAndName.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <common/logger_useful.h>
#include <DataStreams/IBlockInputStream.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits{
        {
            .preserves_distinct_columns = false, /// Actually, we may check that distinct names are in aggregation keys
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }};
}

Aggregator::Params AggregatingStep::createParams(Block header_before_aggregation, AggregateDescriptions aggregates, Names group_by_keys)
{
    ColumnNumbers keys;
    for (const auto & key : group_by_keys)
        keys.push_back(header_before_aggregation.getPositionByName(key));

    for (auto & descr : aggregates)
    {
        // tmp fix: For AggregateFunctionNothing, the argument types in `header_before_aggregation` may diff with
        // the ones in `descr.function->argument_types`. In this case, reconstructing aggregate description will lead
        // to a different result.
        //
        // example: SELECT count(in(NULL, []));
        if (descr.function->getName() == "nothing")
            continue;

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
        AggregateFunctionProperties properties;
        descr.function = AggregateFunctionFactory::instance().get(descr.function->getName(), argument_types, descr.parameters, properties);
    }


    return Aggregator::Params(
        header_before_aggregation, keys, aggregates, false, 0, OverflowMode::THROW, 0, 0, 0, false, nullptr, 0, 0, false, 0);
}


AggregatingStep::AggregatingStep(
    const DataStream & input_stream_,
    Names keys_,
    Aggregator::Params params_,
    bool final_,
    size_t max_block_size_,
    size_t merge_threads_,
    size_t temporary_data_merge_threads_,
    bool storage_has_evenly_distributed_read_,
    InputOrderInfoPtr group_by_info_,
    SortDescription group_by_sort_description_,
    bool cube_,
    bool rollup_,
    NameToNameMap groupings_,
    bool)
    : ITransformingStep(input_stream_, params_.getHeader(final_), getTraits(), false)
    , keys(std::move(keys_))
    , params(std::move(params_))
    , final(final_)
    , max_block_size(max_block_size_)
    , merge_threads(merge_threads_)
    , temporary_data_merge_threads(temporary_data_merge_threads_)
    , storage_has_evenly_distributed_read(storage_has_evenly_distributed_read_)
    , group_by_info(std::move(group_by_info_))
    , group_by_sort_description(std::move(group_by_sort_description_))
    , cube(cube_)
    , rollup(rollup_)
    , groupings(groupings_)
{
    //    final = final && !totals && !cube & !rollup;
    setInputStreams(input_streams);
}

void AggregatingStep::setInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
    output_stream->header = params.getHeader(final);

    for (const auto & item : input_streams[0].header)
        if (groupings.contains(item.name))
        {
            output_stream->header.insert(ColumnWithTypeAndName{std::make_shared<DataTypeUInt8>(), groupings[item.name]});
        }
}

void AggregatingStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & build_settings)
{
    QueryPipelineProcessorsCollector collector(pipeline, this);
    const auto & settings = build_settings.context->getSettingsRef();

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
        params.empty_result_for_aggregation_by_empty_set || settings.empty_result_for_aggregation_by_empty_set,
        params.tmp_volume,
        settings.max_threads,
        settings.min_free_disk_space_for_temporary_data,
        settings.compile_aggregate_expressions,
        settings.min_count_to_compile_aggregate_expression);

    /// Forget about current totals and extremes. They will be calculated again after aggregation if needed.
    pipeline.dropTotalsAndExtremes();
    bool agg_final = final && !cube && !rollup;

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

    /// If there are several sources, then we perform parallel aggregation
    if (pipeline.getNumStreams() > 1)
    {
        /// Add resize transform to uniformly distribute data between aggregating streams.
        if (!storage_has_evenly_distributed_read)
            pipeline.resize(pipeline.getNumStreams(), true, true);

        auto many_data = std::make_shared<ManyAggregatedData>(pipeline.getNumStreams());

        size_t counter = 0;
        pipeline.addSimpleTransform([&](const Block & header) {
            return std::make_shared<AggregatingTransform>(
                header, transform_params, many_data, counter++, merge_max_threads, temporary_data_merge_threads);
        });

        pipeline.resize(1);

        aggregating = collector.detachProcessors(0);
    }
    else
    {
        pipeline.resize(1);

        pipeline.addSimpleTransform([&](const Block & header) { return std::make_shared<AggregatingTransform>(header, transform_params); });

        aggregating = collector.detachProcessors(0);
    }


    auto cube_src_header = pipeline.getHeader();
    ColumnNumbers cube_key_index;
    for (const auto & name : keys)
        cube_key_index.push_back(cube_src_header.getPositionByName(name));

    Aggregator::Params cube_params(
        cube_src_header,
        cube_key_index,
        new_aggregates,
        false,
        new_params.max_rows_to_group_by,
        new_params.group_by_overflow_mode,
        0,
        0,
        new_params.max_bytes_before_external_group_by,
        new_params.empty_result_for_aggregation_by_empty_set,
        new_params.tmp_volume,
        new_params.max_threads,
        new_params.min_free_disk_space,
        new_params.compile_aggregate_expressions,
        new_params.min_count_to_compile_aggregate_expression);

    auto cube_transform_params = std::make_shared<AggregatingTransformParams>(cube_params, true);

    if (rollup)
    {
        pipeline.resize(1);
        if (groupings.empty())
        {
            pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr {
                if (stream_type == QueryPipeline::StreamType::Totals)
                    return nullptr;

                return std::make_shared<RollupTransform>(header, cube_transform_params);
            });
        }
        else
        {
            auto agg_header = pipeline.getHeader();
            ColumnNumbers grouping_index;
            Names names;
            for (const auto & item : agg_header.getNames())
                if (groupings.contains(item))
                {
                    grouping_index.emplace_back(agg_header.getPositionByName(item));
                    names.emplace_back(groupings.at(item));
                }

            pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr {
                if (stream_type == QueryPipeline::StreamType::Totals)
                    return nullptr;

                return std::make_shared<RollupWithGroupingTransform>(header, cube_transform_params, grouping_index, names);
            });
        }
    }

    if (cube)
    {
        pipeline.resize(1);
        pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr {
            if (stream_type == QueryPipeline::StreamType::Totals)
                return nullptr;

            return std::make_shared<CubeTransform>(header, cube_transform_params);
        });
    }
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

void AggregatingStep::serialize(WriteBuffer & buf) const
{
    IQueryPlanStep::serializeImpl(buf);

    params.serialize(buf);
    writeBinary(final, buf);
    writeBinary(max_block_size, buf);
    writeBinary(merge_threads, buf);
    writeBinary(temporary_data_merge_threads, buf);
    writeBinary(storage_has_evenly_distributed_read, buf);

    if (group_by_info)
    {
        writeBinary(true, buf);
        group_by_info->serialize(buf);
    }
    else
        writeBinary(false, buf);

    serializeSortDescription(group_by_sort_description, buf);

    serializeStrings(keys, buf);
    writeBinary(cube, buf);
    writeBinary(rollup, buf);
    writeVarUInt(groupings.size(), buf);
    for (const auto & item : groupings)
    {
        writeStringBinary(item.first, buf);
        writeStringBinary(item.second, buf);
    }
}

QueryPlanStepPtr AggregatingStep::deserialize(ReadBuffer & buf, ContextPtr context)
{
    String step_description;
    readBinary(step_description, buf);

    DataStream input_stream = deserializeDataStream(buf);
    Aggregator::Params params = Aggregator::Params::deserialize(buf, context);
    bool final;
    readBinary(final, buf);
    size_t max_block_size;
    readBinary(max_block_size, buf);
    size_t merge_threads;
    readBinary(merge_threads, buf);
    size_t temporary_data_merge_threads;
    readBinary(temporary_data_merge_threads, buf);
    bool storage_has_evenly_distributed_read;
    readBinary(storage_has_evenly_distributed_read, buf);

    bool has_group_by_info = false;
    readBinary(has_group_by_info, buf);
    InputOrderInfoPtr group_by_info = nullptr;
    if (has_group_by_info)
    {
        group_by_info = std::make_shared<InputOrderInfo>();
        const_cast<InputOrderInfo &>(*group_by_info).deserialize(buf);
    }

    SortDescription group_by_sort_description;
    deserializeSortDescription(group_by_sort_description, buf);


    auto keys = deserializeStrings(buf);
    bool cube;
    readBinary(cube, buf);
    bool rollup;
    readBinary(rollup, buf);

    size_t size;
    readVarUInt(size, buf);
    NameToNameMap groupings;
    for (size_t i = 0; i < size; ++i)
    {
        String k;
        readStringBinary(k, buf);
        String v;
        readStringBinary(v, buf);
        groupings[k] = v;
    }
    auto step = std::make_unique<AggregatingStep>(
        input_stream,
        keys,
        params,
        final,
        max_block_size,
        merge_threads,
        temporary_data_merge_threads,
        storage_has_evenly_distributed_read,
        group_by_info,
        group_by_sort_description,
        cube,
        rollup,
        groupings);

    step->setStepDescription(step_description);
    return step;
}
std::shared_ptr<IQueryPlanStep> AggregatingStep::copy(ContextPtr) const
{
    //  todo
    return std::make_shared<AggregatingStep>(input_streams[0], keys, params.aggregates, final, cube, rollup, groupings);
}

}
