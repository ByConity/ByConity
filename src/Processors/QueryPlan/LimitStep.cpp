#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPipeline.h>
#include <Processors/LimitTransform.h>
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

LimitStep::LimitStep(
    const DataStream & input_stream_,
    size_t limit_, size_t offset_,
    bool always_read_till_end_,
    bool with_ties_,
    SortDescription description_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , limit(limit_), offset(offset_)
    , always_read_till_end(always_read_till_end_)
    , with_ties(with_ties_), description(std::move(description_))
{
}

void LimitStep::updateInputStream(DataStream input_stream_)
{
    input_streams.clear();
    input_streams.emplace_back(std::move(input_stream_));
    output_stream = createOutputStream(input_streams.front(), output_stream->header, getDataStreamTraits());
}

void LimitStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &)
{
    auto transform = std::make_shared<LimitTransform>(
        pipeline.getHeader(), limit, offset, pipeline.getNumStreams(), always_read_till_end, with_ties, description);

    pipeline.addTransform(std::move(transform));
}

void LimitStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << "Limit " << limit << '\n';
    settings.out << prefix << "Offset " << offset << '\n';

    if (with_ties || always_read_till_end)
    {
        settings.out << prefix;

        String str;
        if (with_ties)
            settings.out << "WITH TIES";

        if (always_read_till_end)
        {
            if (!with_ties)
                settings.out << ", ";

            settings.out << "Reads all data";
        }

        settings.out << '\n';
    }
}

void LimitStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Limit", limit);
    map.add("Offset", offset);
    map.add("With Ties", with_ties);
    map.add("Reads All Data", always_read_till_end);
}

void LimitStep::serialize(WriteBuffer & buffer) const
{
    serializeDataStreamFromDataStreams(input_streams, buffer);
    writeBinary(limit, buffer);
    writeBinary(offset, buffer);
    writeBinary(always_read_till_end, buffer);
    writeBinary(with_ties, buffer);
    serializeItemVector<SortColumnDescription>(description, buffer);
}

QueryPlanStepPtr LimitStep::deserialize(ReadBuffer & buffer, ContextPtr )
{
    DataStream input_stream;
    input_stream = deserializeDataStream(buffer);

    size_t limit, offset;
    readBinary(limit, buffer);
    readBinary(offset, buffer);

    bool always_read_till_end, with_ties;
    readBinary(always_read_till_end, buffer);
    readBinary(with_ties, buffer);

    SortDescription sort_description;
    sort_description = deserializeItemVector<SortColumnDescription>(buffer);

    return std::make_unique<LimitStep>(input_stream, limit, offset, always_read_till_end, with_ties, sort_description);
}
}
