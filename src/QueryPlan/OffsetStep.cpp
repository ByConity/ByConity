#include <QueryPlan/OffsetStep.h>
#include <Processors/OffsetTransform.h>
#include <Processors/QueryPipeline.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>

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
            .preserves_number_of_rows = false,
        }
    };
}

OffsetStep::OffsetStep(const DataStream & input_stream_, size_t offset_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , offset(offset_)
{
}

void OffsetStep::setInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
    output_stream->header = input_streams_[0].header;
}

void OffsetStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &)
{
    auto transform = std::make_shared<OffsetTransform>(
            pipeline.getHeader(), offset, pipeline.getNumStreams());

    pipeline.addTransform(std::move(transform));
}

void OffsetStep::describeActions(FormatSettings & settings) const
{
    settings.out << String(settings.offset, ' ') << "Offset " << offset << '\n';
}

void OffsetStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Offset", offset);
}

void OffsetStep::serialize(WriteBuffer & buffer) const
{
    IQueryPlanStep::serializeImpl(buffer);
    writeBinary(offset, buffer);
}

QueryPlanStepPtr OffsetStep::deserialize(ReadBuffer & buffer, ContextPtr )
{
    String step_description;
    readBinary(step_description, buffer);

    DataStream input_stream;
    input_stream = deserializeDataStream(buffer);

    size_t offset;
    readBinary(offset, buffer);

    auto step = std::make_unique<OffsetStep>(input_stream, offset);

    step->setStepDescription(step_description);
    return step;
}

std::shared_ptr<IQueryPlanStep> OffsetStep::copy(ContextPtr) const
{
    return std::make_shared<OffsetStep>(input_streams[0], offset);
}

}
