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

#include <QueryPlan/MergingAggregatedStep.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/MergingAggregatedTransform.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>

namespace DB
{

static ITransformingStep::Traits getTraits(bool should_produce_results_in_order_of_bucket_number)
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = false,
            .returns_single_stream = should_produce_results_in_order_of_bucket_number,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

static Block appendGroupingColumns(Block header, const GroupingDescriptions & groupings)
{
    for (const auto & grouping: groupings)
        header.insert({std::make_shared<DataTypeUInt64>(), grouping.output_name});

    return header;
}

MergingAggregatedStep::MergingAggregatedStep(
    const DataStream & input_stream_,
    AggregatingTransformParamsPtr params_,
    bool memory_efficient_aggregation_,
    size_t max_threads_,
    size_t memory_efficient_merge_threads_)
    : MergingAggregatedStep(input_stream_,
                            {},
                            {},
                            {},
                            std::move(params_),
                            memory_efficient_aggregation_,
                            max_threads_,
                            memory_efficient_merge_threads_)
{
}

MergingAggregatedStep::MergingAggregatedStep(
    const DataStream & input_stream_,
    Names keys_,
    GroupingSetsParamsList grouping_sets_params_,
    GroupingDescriptions groupings_,
    AggregatingTransformParamsPtr params_,
    bool memory_efficient_aggregation_,
    size_t max_threads_,
    size_t memory_efficient_merge_threads_)
    : ITransformingStep(
        input_stream_,
        appendGroupingColumns(params_->getCustomHeader(params_->final), groupings_),
        getTraits(!(params_->final) && memory_efficient_aggregation_))
    , keys(std::move(keys_))
    , grouping_sets_params(std::move(grouping_sets_params_))
    , groupings(std::move(groupings_))
    , params(params_)
    , memory_efficient_aggregation(memory_efficient_aggregation_)
    , max_threads(max_threads_)
    , memory_efficient_merge_threads(memory_efficient_merge_threads_)
    , should_produce_results_in_order_of_bucket_number(!(params->final) && memory_efficient_aggregation)
{
    NameSet output_names;
    for (const auto & key : keys)
        if (!output_names.emplace(key).second)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "duplicate group by key: {}", key);

    for (const auto & aggregate : params->params.aggregates)
        if (!output_names.emplace(aggregate.column_name).second)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "duplicate aggreagte function output name: {}", aggregate.column_name);

    /// Aggregation keys are distinct
    for (auto key : params->params.keys)
        output_stream->distinct_columns.insert(params->params.intermediate_header.getByPosition(key).name);
}

void MergingAggregatedStep::setInputStreams(const DataStreams & input_streams_)
{
    // TODO: what if input_streams and params->getHeader() are inconsistent
    input_streams = input_streams_;
    output_stream->header = appendGroupingColumns(params->getHeader(), groupings);
}

void MergingAggregatedStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & build_settings)
{
    if (hasNonParallelAggregateFunctions(params->params.aggregates))
    {
        pipeline.resize(1);
    }

    // optimizer use MergingAggregateStep in by-name style, regenerate aggregator params of by-position style
    if (!keys.empty())
    {
        ColumnNumbers key_positions;
        const auto & header = pipeline.getHeader();
        for (const auto & key : keys)
            key_positions.emplace_back(header.getPositionByName(key));

        Aggregator::Params new_params(
            header,
            key_positions,
            params->params.aggregates,
            params->params.overflow_row,
            build_settings.context->getSettingsRef().max_threads);
        params = std::make_shared<AggregatingTransformParams>(new_params, params->final);
    }

    // @FIXME: grouping sets + two-level aggregation is incompatible with memory efficient merge
    // see also: https://meego.feishu.cn/clickhousech/story/detail/14744099
    if (!memory_efficient_aggregation || input_streams.front().header.has("__grouping_set"))
    {
        /// We union several sources into one, paralleling the work.
        pipeline.resize(1);

        /// Now merge the aggregated blocks
        pipeline.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<MergingAggregatedTransform>(header, params, max_threads);
        });
    }
    else
    {
        auto num_merge_threads = memory_efficient_merge_threads
                                 ? static_cast<size_t>(memory_efficient_merge_threads)
                                 : static_cast<size_t>(max_threads);

        pipeline.addMergingAggregatedMemoryEfficientTransform(params, num_merge_threads);
    }

    computeGroupingFunctions(pipeline, groupings, keys, grouping_sets_params, build_settings);

    pipeline.resize(should_produce_results_in_order_of_bucket_number ? 1 : max_threads);
}

void MergingAggregatedStep::describeActions(FormatSettings & settings) const
{
    return params->params.explain(settings.out, settings.offset);
}

void MergingAggregatedStep::describeActions(JSONBuilder::JSONMap & map) const
{
    params->params.explain(map);
}

void MergingAggregatedStep::toProto(Protos::MergingAggregatedStep & proto, bool) const
{
    ITransformingStep::serializeToProtoBase(*proto.mutable_query_plan_base());
    for (const auto & element : keys)
        proto.add_keys(element);
    for (const auto & element : grouping_sets_params)
        element.toProto(*proto.add_grouping_sets_params());
    for (const auto & element : groupings)
        element.toProto(*proto.add_groupings());

    if (!params)
        throw Exception("params cannot be nullptr", ErrorCodes::LOGICAL_ERROR);
    params->toProto(*proto.mutable_params());
    proto.set_memory_efficient_aggregation(memory_efficient_aggregation);
    proto.set_max_threads(max_threads);
    proto.set_memory_efficient_merge_threads(memory_efficient_merge_threads);
}

std::shared_ptr<MergingAggregatedStep> MergingAggregatedStep::fromProto(const Protos::MergingAggregatedStep & proto, ContextPtr context)
{
    auto [step_description, base_input_stream] = ITransformingStep::deserializeFromProtoBase(proto.query_plan_base());
    Names keys;
    for (const auto & element : proto.keys())
        keys.emplace_back(element);
    GroupingSetsParamsList grouping_sets_params;
    for (const auto & proto_element : proto.grouping_sets_params())
    {
        GroupingSetsParams element;
        element.fillFromProto(proto_element);
        grouping_sets_params.emplace_back(std::move(element));
    }
    GroupingDescriptions groupings;
    for (const auto & proto_element : proto.groupings())
    {
        GroupingDescription element;
        element.fillFromProto(proto_element);
        groupings.emplace_back(std::move(element));
    }
    auto params = AggregatingTransformParams::fromProto(proto.params(), context);
    auto memory_efficient_aggregation = proto.memory_efficient_aggregation();
    auto max_threads = proto.max_threads();
    auto memory_efficient_merge_threads = proto.memory_efficient_merge_threads();
    auto step = std::make_shared<MergingAggregatedStep>(
        base_input_stream,
        std::move(keys),
        std::move(grouping_sets_params),
        std::move(groupings),
        std::move(params),
        memory_efficient_aggregation,
        max_threads,
        memory_efficient_merge_threads);
    step->setStepDescription(step_description);
    return step;
}

std::shared_ptr<IQueryPlanStep> MergingAggregatedStep::copy(ContextPtr) const
{
    return std::make_shared<MergingAggregatedStep>(
        input_streams[0], keys, grouping_sets_params, groupings, params, memory_efficient_aggregation, max_threads, memory_efficient_merge_threads);
}

}

