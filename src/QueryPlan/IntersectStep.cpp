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

#include <QueryPlan/IntersectStep.h>

#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPipeline.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
// #include <Processors/Transforms/IntersectOrExceptTransform.h>

namespace DB
{
IntersectStep::IntersectStep(
    DataStreams input_streams_,
    DataStream output_stream_,
    std::unordered_map<String, std::vector<String>> output_to_inputs_,
    bool distinct_)
    : SetOperationStep(input_streams_, output_stream_, output_to_inputs_), distinct(distinct_)
{
}

QueryPipelinePtr IntersectStep::updatePipeline(QueryPipelines, const BuildQueryPipelineSettings &)
{
    throw Exception("intersect step is not implemented", ErrorCodes::NOT_IMPLEMENTED);
    // auto pipeline = std::make_unique<QueryPipeline>();
    // QueryPipelineProcessorsCollector collector(*pipeline, this);

    // if (pipelines.empty())
    // {
    //     pipeline->init(Pipe(std::make_shared<NullSource>(output_stream->header)));
    //     processors = collector.detachProcessors();
    //     return pipeline;
    // }

    // for (auto & cur_pipeline : pipelines)
    // {
    //     /// Just in case.
    //     if (!isCompatibleHeader(cur_pipeline->getHeader(), getOutputStream().header))
    //     {
    //         auto converting_dag = ActionsDAG::makeConvertingActions(
    //             cur_pipeline->getHeader().getColumnsWithTypeAndName(),
    //             getOutputStream().header.getColumnsWithTypeAndName(),
    //             ActionsDAG::MatchColumnsMode::Position);

    //         auto converting_actions = std::make_shared<ExpressionActions>(std::move(converting_dag));
    //         cur_pipeline->addSimpleTransform(
    //             [&](const Block & cur_header) { return std::make_shared<ExpressionTransform>(cur_header, converting_actions); });
    //     }

    //     /// For the case of union.
    //     cur_pipeline->addTransform(std::make_shared<ResizeProcessor>(getOutputStream().header, cur_pipeline->getNumStreams(), 1));
    // }

    // *pipeline = QueryPipeline::unitePipelines(std::move(pipelines), context.context->getSettingsRef().max_threads);
    // pipeline->addTransform(std::make_shared<IntersectOrExceptTransform>(
    //     getOutputStream().header,
    //     distinct ? ASTSelectIntersectExceptQuery::Operator::INTERSECT_DISTINCT : ASTSelectIntersectExceptQuery::Operator::INTERSECT_ALL));

    // processors = collector.detachProcessors();
    // return pipeline;
}

std::shared_ptr<IntersectStep> IntersectStep::fromProto(const Protos::IntersectStep & proto, ContextPtr)
{
    auto [base_input_streams, base_output_stream, output_to_inputs] = SetOperationStep::deserializeFromProtoBase(proto.query_plan_base());
    auto distinct = proto.distinct();
    auto step = std::make_shared<IntersectStep>(base_input_streams, base_output_stream, output_to_inputs, distinct);

    return step;
}

void IntersectStep::toProto(Protos::IntersectStep & proto, bool) const
{
    SetOperationStep::serializeToProtoBase(*proto.mutable_query_plan_base());
    proto.set_distinct(distinct);
}

bool IntersectStep::isDistinct() const
{
    return distinct;
}

std::shared_ptr<IQueryPlanStep> IntersectStep::copy(ContextPtr) const
{
    return std::make_unique<IntersectStep>(input_streams, output_stream.value(), distinct);
}

}
