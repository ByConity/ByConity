#pragma once
#include <DataStreams/SizeLimits.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/Transforms/FinalSampleTransform.h>
#include <Common/JSONBuilder.h>

namespace DB
{
/// Executes Sample. See FinalSampleTransform.
class FinalSampleStep : public ITransformingStep
{
public:
    FinalSampleStep(const DataStream & input_stream_, size_t sample_size_, size_t max_chunk_size_)
        : ITransformingStep(input_stream_, input_stream_.header, getTraits()), sample_size(sample_size_), max_chunk_size(max_chunk_size_)
    {
    }

    String getName() const override { return "FinalSample"; }

    Type getType() const override { return Type::FinalSample; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override
    {
        auto transform = std::make_shared<FinalSampleTransform>(pipeline.getHeader(), sample_size, max_chunk_size, pipeline.getNumStreams());
        pipeline.addTransform(std::move(transform));
    }

    void describeActions(FormatSettings & settings) const override
    {
        String prefix(settings.offset, ' ');
        settings.out << prefix << "sample_size " << sample_size << '\n';
        settings.out << prefix << "max_chunk_size " << max_chunk_size << '\n';
    }

    void describeActions(JSONBuilder::JSONMap & map) const override
    {
        map.add("sample_size", sample_size);
        map.add("max_chunk_size", max_chunk_size);
    }

    void serialize(WriteBuffer & buffer) const override
    {
        IQueryPlanStep::serializeImpl(buffer);
        writeBinary(sample_size, buffer);
        writeBinary(max_chunk_size, buffer);
    }

    static QueryPlanStepPtr deserialize(ReadBuffer & buffer, ContextPtr /*context_*/)
    {
        String step_description;
        readBinary(step_description, buffer);

        DataStream input_stream;
        input_stream = deserializeDataStream(buffer);

        size_t sample_size_;
        size_t max_chunk_size_;
        readBinary(sample_size_, buffer);
        readBinary(max_chunk_size_, buffer);

        auto step = std::make_unique<FinalSampleStep>(input_stream, sample_size_, max_chunk_size_);

        step->setStepDescription(step_description);
        return std::move(step);
    }

    static ITransformingStep::Traits getTraits()
    {
        return ITransformingStep::Traits{
            {
                .preserves_distinct_columns = true,
                .returns_single_stream = false,
                .preserves_number_of_streams = true,
                .preserves_sorting = true,
            },
            {
                .preserves_number_of_rows = false,
            }};
    }

private:
    size_t sample_size;
    size_t max_chunk_size;
};

}
