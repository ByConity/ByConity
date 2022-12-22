#include <QueryPlan/EnforceSingleRowStep.h>

#include <Processors/Transforms/EnforceSingleRowTransform.h>
#include <Processors/QueryPipeline.h>

namespace DB
{
EnforceSingleRowStep::EnforceSingleRowStep(const DB::DataStream & input_stream_)
    : ITransformingStep(input_stream_, input_stream_.header, {})
{
}

void EnforceSingleRowStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.resize(1);
    pipeline.addSimpleTransform([&](const Block & header) { return std::make_shared<EnforceSingleRowTransform>(header); });
}

void EnforceSingleRowStep::setInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
}

void EnforceSingleRowStep::serialize(WriteBuffer & buf) const
{
    IQueryPlanStep::serializeImpl(buf);
}

QueryPlanStepPtr EnforceSingleRowStep::deserialize(ReadBuffer & buf, ContextPtr)
{
    String step_description;
    readBinary(step_description, buf);

    DataStream input_stream = deserializeDataStream(buf);
    return std::make_unique<EnforceSingleRowStep>(input_stream);
}

std::shared_ptr<IQueryPlanStep> EnforceSingleRowStep::copy(ContextPtr) const
{
    return std::make_unique<EnforceSingleRowStep>(input_streams[0]);
}

}
