#include <QueryPlan/ReadNothingStep.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Sources/NullSource.h>

namespace DB
{

ReadNothingStep::ReadNothingStep(Block output_header)
    : ISourceStep(DataStream{.header = std::move(output_header), .has_single_port = true})
{
}

void ReadNothingStep::initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.init(Pipe(std::make_shared<NullSource>(getOutputStream().header)));
}

void ReadNothingStep::serialize(WriteBuffer & buffer) const
{
    serializeBlock(output_stream->header, buffer);
}

QueryPlanStepPtr ReadNothingStep::deserialize(ReadBuffer & buffer, ContextPtr )
{
    Block output_header = deserializeBlock(buffer);
    return std::make_unique<ReadNothingStep>(output_header);
}

std::shared_ptr<IQueryPlanStep> ReadNothingStep::copy(ContextPtr) const
{
    return std::make_shared<ReadNothingStep>(output_stream->header);
}

}
