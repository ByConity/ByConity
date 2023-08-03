#pragma once
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/BufferTransform.h>
#include <QueryPlan/ITransformingStep.h>

namespace DB
{

class BufferStep : public ITransformingStep
{
public:
    explicit BufferStep(const DataStream & input_stream_) : ITransformingStep(input_stream_, input_stream_.header, Traits{})
    {
    }

    String getName() const override
    {
        return "Buffer";
    }

    Type getType() const override
    {
        return Type::Buffer;
    }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override
    {
        pipeline.addSimpleTransform([&](const Block & header) {
            auto transform = std::make_shared<BufferTransform>(header);
            return transform;
        });
    }

    void serialize(WriteBuffer & buffer) const override
    {
        IQueryPlanStep::serializeImpl(buffer);
    }

    static QueryPlanStepPtr deserialize(ReadBuffer & buffer, ContextPtr)
    {
        String step_description;
        readBinary(step_description, buffer);

        DataStream input_stream;
        input_stream = deserializeDataStream(buffer);

        auto step = std::make_unique<BufferStep>(input_stream);

        step->setStepDescription(step_description);
        return step;
    }

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const override
    {
        return std::make_shared<BufferStep>(input_streams[0]);
    }

    void setInputStreams(const DataStreams & input_streams_) override
    {
        input_streams = input_streams_;
        output_stream->header = input_streams_[0].header;
    }
};

}
