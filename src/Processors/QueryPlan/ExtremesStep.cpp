#include <Processors/QueryPlan/ExtremesStep.h>
#include <Processors/QueryPipeline.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = true,
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = true,
        }
    };
}

ExtremesStep::ExtremesStep(const DataStream & input_stream_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
{
}

void ExtremesStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addExtremesTransform();
}

void ExtremesStep::serialize(WriteBuffer & buffer) const
{
    serializeDataStream(input_stream, buffer);
}

QueryPlanStepPtr ExtremesStep::deserialize(ReadBuffer & buffer, ContextPtr )
{
    DataStream input_stream;
    input_stream = deserializeDataStream(buffer);

    return std::make_unique<ExtremesStep>(input_stream);
}

}
