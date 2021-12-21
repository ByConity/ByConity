#include <Processors/QueryPlan/RemoteExchangeSourceStep.h>
#include <Interpreters/DistributedStages/PlanSegment.h>


namespace DB
{

RemoteExchangeSourceStep::RemoteExchangeSourceStep(const PlanSegmentInputs & inputs_, DataStream input_stream_)
: inputs(inputs_)
{
    input_streams.emplace_back(std::move(input_stream_));
    output_stream = DataStream{.header = input_streams[0].header};
}

QueryPipelinePtr RemoteExchangeSourceStep::updatePipeline(QueryPipelines, const BuildQueryPipelineSettings &)
{
    return nullptr;
}

void RemoteExchangeSourceStep::serialize(WriteBuffer & buf) const
{
    IQueryPlanStep::serializeImpl(buf);

    writeBinary(inputs.size(), buf);
    for (auto & input : inputs)
        input->serialize(buf);
}

QueryPlanStepPtr RemoteExchangeSourceStep::deserialize(ReadBuffer & buf, ContextPtr)
{
    String step_description;
    readBinary(step_description, buf);

    auto input_stream = deserializeDataStream(buf);

    size_t input_size;
    readBinary(input_size, buf);
    PlanSegmentInputs inputs(input_size);
    for (size_t i = 0; i < input_size; ++i)
    {
        inputs[i] = std::make_shared<PlanSegmentInput>();
        inputs[i]->deserialize(buf);
    }

    auto step = std::make_unique<RemoteExchangeSourceStep>(inputs, input_stream);
    step->setStepDescription(step_description);
    return step;
}

}
