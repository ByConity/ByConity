#include <QueryPlan/IntersectStep.h>

#include <Core/Block.h>
#include <Interpreters/Context.h>

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
    throw Exception("IntersectStep should be rewritten into UnionStep", ErrorCodes::NOT_IMPLEMENTED);
}

void IntersectStep::serialize(WriteBuffer &) const
{
    throw Exception("IntersectStep should be rewritten into UnionStep", ErrorCodes::NOT_IMPLEMENTED);
}

QueryPlanStepPtr IntersectStep::deserialize(ReadBuffer &, ContextPtr)
{
    throw Exception("IntersectStep should be rewritten into UnionStep", ErrorCodes::NOT_IMPLEMENTED);
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
