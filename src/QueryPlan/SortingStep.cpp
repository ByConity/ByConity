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

#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/FinishSortingTransform.h>
#include <Processors/Transforms/LimitsCheckingTransform.h>
#include <Processors/Transforms/MergeSortingTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <QueryPlan/SortingStep.h>
#include <Protos/PreparedStatementHelper.h>
#include <Common/JSONBuilder.h>
#include "QueryPlan/PlanSerDerHelper.h"

namespace DB
{

static ITransformingStep::Traits getTraits(const SizeOrVariable & limit, bool is_final_sorting = false)
{
    return ITransformingStep::Traits{
        {
            .preserves_distinct_columns = true,
            .returns_single_stream = is_final_sorting,
            .preserves_number_of_streams = !is_final_sorting,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = std::holds_alternative<UInt64>(limit) && std::get<UInt64>(limit) == 0,
        }};
}

SortingStep::SortingStep(
    const DataStream & input_stream_,
    SortDescription result_description_,
    SizeOrVariable limit_,
    Stage stage_,
    SortDescription prefix_description_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits(limit_, stage_ != Stage::PARTIAL))
    , result_description(result_description_)
    , limit(limit_)
    , stage(stage_)
    , prefix_description(prefix_description_)
{
    /// TODO: check input_stream is partially sorted by the same description.
    output_stream->sort_description = result_description;
    output_stream->sort_mode
        = (input_stream_.has_single_port || stage_ != Stage::PARTIAL) ? DataStream::SortMode::Stream : DataStream::SortMode::Port;
}

void SortingStep::setInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
    output_stream->header = input_streams_[0].header;
}

void SortingStep::updateLimit(size_t limit_)
{
    if (limit_ && !hasPreparedParam() && (getLimitValue() == 0 || limit_ < getLimitValue()))
    {
        limit = limit_;
        transform_traits.preserves_number_of_rows = false;
    }
}

void SortingStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & settings)
{
    auto local_settings = settings.context->getSettingsRef();
    SizeLimits size_limits(local_settings.max_rows_to_sort, local_settings.max_bytes_to_sort, local_settings.sort_overflow_mode);

    auto desc_copy = result_description;

    if (stage == Stage::FULL || stage == Stage::PARTIAL)
    {
        // finish sorting
        if (!prefix_description.empty())
        {
            bool need_finish_sorting = (prefix_description.size() < result_description.size());
            if (pipeline.getNumStreams() > 1)
            {
                UInt64 limit_for_merging = (need_finish_sorting ? 0 : getLimitValue());
                auto transform = std::make_shared<MergingSortedTransform>(
                    pipeline.getHeader(), pipeline.getNumStreams(), prefix_description, local_settings.max_block_size, limit_for_merging);

                pipeline.addTransform(std::move(transform));
            }

            if (need_finish_sorting)
            {
                pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr {
                    if (stream_type != QueryPipeline::StreamType::Main)
                        return nullptr;

                    return std::make_shared<PartialSortingTransform>(header, result_description, getLimitValue());
                });

                /// NOTE limits are not applied to the size of temporary sets in FinishSortingTransform
                pipeline.addSimpleTransform([&](const Block & header) -> ProcessorPtr {
                    return std::make_shared<FinishSortingTransform>(
                        header, prefix_description, result_description, local_settings.max_block_size, getLimitValue());
                });
            }
            return;
        }

        pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr {
            if (stream_type != QueryPipeline::StreamType::Main)
                return nullptr;

            return std::make_shared<PartialSortingTransform>(header, desc_copy, getLimitValue());
        });

        StreamLocalLimits limits;
        limits.mode = LimitsMode::LIMITS_CURRENT; //-V1048
        limits.size_limits = size_limits;

        pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr {
            if (stream_type != QueryPipeline::StreamType::Main)
                return nullptr;

            auto transform = std::make_shared<LimitsCheckingTransform>(header, limits);
            return transform;
        });

        pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr {
            if (stream_type == QueryPipeline::StreamType::Totals)
                return nullptr;

            return std::make_shared<MergeSortingTransform>(
                header,
                result_description,
                local_settings.max_block_size,
                getLimitValue(),
                local_settings.max_bytes_before_remerge_sort / pipeline.getNumStreams(),
                local_settings.remerge_sort_lowered_memory_bytes_ratio,
                local_settings.max_bytes_before_external_sort,
                settings.context->getTemporaryVolume(),
                local_settings.min_free_disk_space_for_temporary_data);
        });
    }

    /// If there are several streams, then we merge them into one
    if (pipeline.getNumStreams() > 1)
    {
        auto transform = std::make_shared<MergingSortedTransform>(
            pipeline.getHeader(), pipeline.getNumStreams(), desc_copy, local_settings.max_block_size, getLimitValue());

        pipeline.addTransform(std::move(transform));
    }
}

void SortingStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << "Sort description: ";
    dumpSortDescription(result_description, input_streams.front().header, settings.out);
    settings.out << '\n';

    std::visit(
        overloaded{
            [&](const UInt64 & x) {
                if (x)
                    settings.out << prefix << "Limit " << x << '\n';
            },
            [&](const String & x) { settings.out << prefix << "Limit " << x << '\n'; }},
        limit);
}

void SortingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Sort Description", explainSortDescription(result_description, input_streams.front().header));

    std::visit(
        overloaded{
            [&](const UInt64 & x) {
                if (x)
                    map.add("Limit", x);
            },
            [&](const String & x) { map.add("Limit", x); }},
        limit);
}

std::shared_ptr<SortingStep> SortingStep::fromProto(const Protos::SortingStep & proto, ContextPtr)
{
    auto [step_description, base_input_stream] = ITransformingStep::deserializeFromProtoBase(proto.query_plan_base());
    SortDescription result_description;
    for (const auto & proto_element : proto.result_description())
    {
        SortColumnDescription element;
        element.fillFromProto(proto_element);
        result_description.emplace_back(std::move(element));
    }

    Stage stage = Stage::FULL;
    if (proto.has_stage())
        stage = StageConverter::fromProto(proto.stage());

    auto limit = proto.limit();
    auto limit_or_var = getSizeOrVariableFromProto(proto.limit_or_var());
    SortDescription prefix_description;
    for (const auto & proto_element : proto.prefix_description())
    {
        SortColumnDescription element;
        element.fillFromProto(proto_element);
        prefix_description.emplace_back(std::move(element));
    }
    auto step = std::make_shared<SortingStep>(base_input_stream, result_description, limit_or_var ? *limit_or_var : limit, stage, prefix_description);
    step->setStepDescription(step_description);
    return step;
}

void SortingStep::toProto(Protos::SortingStep & proto, bool) const
{
    ITransformingStep::serializeToProtoBase(*proto.mutable_query_plan_base());
    for (const auto & element : result_description)
        element.toProto(*proto.add_result_description());
    proto.set_limit(0);
    setSizeOrVariableToProto(limit, *proto.mutable_limit_or_var());
    proto.set_partial(false);
    proto.set_stage(StageConverter::toProto(stage));
    for (const auto & element : prefix_description)
        element.toProto(*proto.add_prefix_description());
}

std::shared_ptr<IQueryPlanStep> SortingStep::copy(ContextPtr) const
{
    return std::make_shared<SortingStep>(input_streams[0], result_description, limit, stage, prefix_description);
}

void SortingStep::prepare(const PreparedStatementContext & prepared_context)
{
    prepared_context.prepare(limit);
}
}
