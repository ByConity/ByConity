#include <IO/OutfileCommon.h>
#include <QueryPlan/OutfileFinishStep.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits{
        {.preserves_distinct_columns = true,
         .returns_single_stream = false,
         .preserves_number_of_streams = true,
         .preserves_sorting = true},
        {.preserves_number_of_rows = true}};
}

OutfileFinishStep::OutfileFinishStep(const DataStream & input_stream_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
{
}

void OutfileFinishStep::setInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
    output_stream = DataStream{.header = std::move((input_streams_[0].header))};
}

// TODO: calculate progress, do nothing now
void OutfileFinishStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.resize(1);
}

std::shared_ptr<IQueryPlanStep> OutfileFinishStep::copy(ContextPtr) const
{
    return std::make_shared<OutfileFinishStep>(input_streams[0]);
}

}
