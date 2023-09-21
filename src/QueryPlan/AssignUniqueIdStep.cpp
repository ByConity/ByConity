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

#include <QueryPlan/AssignUniqueIdStep.h>

#include <DataTypes/DataTypesNumber.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/AssignUniqueIdTransform.h>


namespace DB
{
AssignUniqueIdStep::AssignUniqueIdStep(const DataStream & input_stream_, String unique_id_)
    : ITransformingStep(input_stream_, AssignUniqueIdTransform::transformHeader(input_stream_.header, unique_id_), {})
    , unique_id(std::move(unique_id_))
{
}

void AssignUniqueIdStep::setInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
    output_stream = input_streams[0];
    output_stream->header.insert(ColumnWithTypeAndName{std::make_shared<DataTypeUInt64>(), unique_id});
}

void AssignUniqueIdStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&](const Block & header) { return std::make_shared<AssignUniqueIdTransform>(header, unique_id); });
}

std::shared_ptr<AssignUniqueIdStep> AssignUniqueIdStep::fromProto(const Protos::AssignUniqueIdStep & proto, ContextPtr)
{
    auto [step_description, base_input_stream] = ITransformingStep::deserializeFromProtoBase(proto.query_plan_base());
    auto unique_id = proto.unique_id();
    auto step = std::make_shared<AssignUniqueIdStep>(base_input_stream, unique_id);
    step->setStepDescription(step_description);
    return step;
}

void AssignUniqueIdStep::toProto(Protos::AssignUniqueIdStep & proto, bool) const
{
    ITransformingStep::serializeToProtoBase(*proto.mutable_query_plan_base());
    proto.set_unique_id(unique_id);
}

std::shared_ptr<IQueryPlanStep> AssignUniqueIdStep::copy(ContextPtr) const
{
    return std::make_unique<AssignUniqueIdStep>(input_streams[0], unique_id);
}

}
