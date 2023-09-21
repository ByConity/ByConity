#include <Processors/QueryPipeline.h>
#include <QueryPlan/ISourceStep.h>
#include <QueryPlan/PlanSerDerHelper.h>

namespace DB
{

ISourceStep::ISourceStep(DataStream output_stream_, PlanHints hints_)
{
    output_stream = std::move(output_stream_);
    hints = std::move(hints_);
}

QueryPipelinePtr ISourceStep::updatePipeline(QueryPipelines, const BuildQueryPipelineSettings & settings)
{
    auto pipeline = std::make_unique<QueryPipeline>();
    QueryPipelineProcessorsCollector collector(*pipeline, this);
    initializePipeline(*pipeline, settings);
    auto added_processors = collector.detachProcessors();
    processors.insert(processors.end(), added_processors.begin(), added_processors.end());
    return pipeline;
}

void ISourceStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

// this won't be override, so use a different name
void ISourceStep::serializeToProtoBase(Protos::ISourceStep & proto) const
{
    serializeBlockToProto(output_stream->header, *proto.mutable_output_header());
}

// return base_output_stream and step_description
Block ISourceStep::deserializeFromProtoBase(const Protos::ISourceStep & proto)
{
    Block output_header = deserializeBlockFromProto(proto.output_header());
    return output_header;
}
}
