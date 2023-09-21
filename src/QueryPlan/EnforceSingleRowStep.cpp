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

#include <QueryPlan/EnforceSingleRowStep.h>

#include <Processors/Transforms/EnforceSingleRowTransform.h>
#include <Processors/QueryPipeline.h>
#include <Interpreters/join_common.h>

namespace DB
{
EnforceSingleRowStep::EnforceSingleRowStep(const DB::DataStream & input_stream_)
    : ITransformingStep(input_stream_, input_stream_.header, {})
{
    makeOutputNullable();
}

void EnforceSingleRowStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.resize(1);
    pipeline.addSimpleTransform([&](const Block & header) { return std::make_shared<EnforceSingleRowTransform>(header); });
}

void EnforceSingleRowStep::setInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
    makeOutputNullable();
}

void EnforceSingleRowStep::makeOutputNullable()
{
    auto input_header = input_streams[0].header;
    NamesAndTypes nullable_output_header;
    for(auto & input : input_header)
    {
        if (!JoinCommon::canBecomeNullable(input.type))
            nullable_output_header.emplace_back(input.name, input.type);
        else
            nullable_output_header.emplace_back(input.name, JoinCommon::convertTypeToNullable(input.type));
    }
    output_stream = DataStream{.header = {nullable_output_header}};
}

std::shared_ptr<EnforceSingleRowStep> EnforceSingleRowStep::fromProto(const Protos::EnforceSingleRowStep & proto, ContextPtr)
{
    auto [step_description, base_input_stream] = ITransformingStep::deserializeFromProtoBase(proto.query_plan_base());
    auto step = std::make_shared<EnforceSingleRowStep>(base_input_stream);
    step->setStepDescription(step_description);
    return step;
}

void EnforceSingleRowStep::toProto(Protos::EnforceSingleRowStep & proto, bool) const
{
    ITransformingStep::serializeToProtoBase(*proto.mutable_query_plan_base());
}

std::shared_ptr<IQueryPlanStep> EnforceSingleRowStep::copy(ContextPtr) const
{
    return std::make_unique<EnforceSingleRowStep>(input_streams[0]);
}

}
