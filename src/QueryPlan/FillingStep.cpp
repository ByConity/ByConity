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

#include <QueryPlan/FillingStep.h>
#include <Processors/Transforms/FillingTransform.h>
#include <Processors/QueryPipeline.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = false, /// TODO: it seem to actually be true. Check it later.
            .returns_single_stream = true,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

FillingStep::FillingStep(const DataStream & input_stream_, SortDescription sort_description_)
    : ITransformingStep(input_stream_, FillingTransform::transformHeader(input_stream_.header, sort_description_), getTraits())
    , sort_description(std::move(sort_description_))
{
    if (!input_stream_.has_single_port)
        throw Exception("FillingStep expects single input", ErrorCodes::LOGICAL_ERROR);
}

void FillingStep::setInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
    output_stream->header = FillingTransform::transformHeader(input_streams_[0].header, sort_description);
}

void FillingStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr
    {
        bool on_totals = stream_type == QueryPipeline::StreamType::Totals;
        return std::make_shared<FillingTransform>(header, sort_description, on_totals);
    });
}

void FillingStep::describeActions(FormatSettings & settings) const
{
    settings.out << String(settings.offset, ' ');
    dumpSortDescription(sort_description, input_streams.front().header, settings.out);
    settings.out << '\n';
}

void FillingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Sort Description", explainSortDescription(sort_description, input_streams.front().header));
}

std::shared_ptr<FillingStep> FillingStep::fromProto(const Protos::FillingStep & proto, ContextPtr)
{
    auto [step_description, base_input_stream] = ITransformingStep::deserializeFromProtoBase(proto.query_plan_base());
    SortDescription sort_description;
    for (const auto & proto_element : proto.sort_description())
    {
        SortColumnDescription element;
        element.fillFromProto(proto_element);
        sort_description.emplace_back(std::move(element));
    }
    auto step = std::make_shared<FillingStep>(base_input_stream, sort_description);
    step->setStepDescription(step_description);
    return step;
}

void FillingStep::toProto(Protos::FillingStep & proto, bool) const
{
    ITransformingStep::serializeToProtoBase(*proto.mutable_query_plan_base());
    for (const auto & element : sort_description)
        element.toProto(*proto.add_sort_description());
}

std::shared_ptr<IQueryPlanStep> FillingStep::copy(ContextPtr) const
{
    return std::make_shared<FillingStep>(input_streams[0], sort_description);
}

}
