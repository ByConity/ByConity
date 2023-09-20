#pragma once
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/BufferTransform.h>
#include <Protos/plan_node.pb.h>
#include <QueryPlan/ITransformingStep.h>

namespace DB
{

class BufferStep : public ITransformingStep
{
public:
    explicit BufferStep(const DataStream & input_stream_) : ITransformingStep(input_stream_, input_stream_.header, Traits{}) { }

    String getName() const override { return "Buffer"; }

    Type getType() const override { return Type::Buffer; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override
    {
        pipeline.addSimpleTransform([&](const Block & header) {
            auto transform = std::make_shared<BufferTransform>(header);
            return transform;
        });
    }

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const override { return std::make_shared<BufferStep>(input_streams[0]); }

    void toProto(Protos::BufferStep & proto, bool for_hash_equals = false) const
    {
        (void)for_hash_equals;
        ITransformingStep::serializeToProtoBase(*proto.mutable_query_plan_base());
    }


    static std::shared_ptr<BufferStep> fromProto(const Protos::BufferStep & proto, ContextPtr)
    {
        auto [step_description, base_input_stream] = ITransformingStep::deserializeFromProtoBase(proto.query_plan_base());
        auto step = std::make_shared<BufferStep>(base_input_stream);
        step->setStepDescription(step_description);
        return step;
    }

    void setInputStreams(const DataStreams & input_streams_) override
    {
        input_streams = input_streams_;
        output_stream->header = input_streams_[0].header;
    }
};

}
