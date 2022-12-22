#include <QueryPlan/AnyStep.h>

namespace DB
{
QueryPipelinePtr AnyStep::updatePipeline(QueryPipelines, const BuildQueryPipelineSettings &)
{
    throw Exception("AnyStep is a fake step", ErrorCodes::NOT_IMPLEMENTED);
}

void AnyStep::serialize(WriteBuffer &) const
{
    throw Exception("AnyStep is a fake step", ErrorCodes::NOT_IMPLEMENTED);
}

QueryPlanStepPtr AnyStep::deserialize(ReadBuffer &, ContextPtr)
{
    throw Exception("AnyStep is a fake step", ErrorCodes::NOT_IMPLEMENTED);
}

std::shared_ptr<IQueryPlanStep> AnyStep::copy(ContextPtr) const
{
    return std::make_unique<AnyStep>(output_stream.value(), group_id);
}

void AnyStep::setInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
}

}
