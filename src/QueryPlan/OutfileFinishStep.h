#pragma once

#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/ITransformingStep.h>
#include <QueryPlan/TableWriteStep.h>

namespace DB
{
class OutfileFinishStep : public ITransformingStep
{
public:
    explicit OutfileFinishStep(const DataStream & input_stream_);

    String getName() const override
    {
        return "OutfileFinish";
    }
    Type getType() const override
    {
        return Type::OutfileFinish;
    }

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const override;

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & settings) override;

    void setInputStreams(const DataStreams & input_streams_) override;

    void toProto(Protos::OutfileFinishStep & proto, bool for_hash_equals = false) const
    {
        (void)proto;
        (void)for_hash_equals;
    }

    static std::shared_ptr<OutfileFinishStep> fromProto(const Protos::OutfileFinishStep & proto, ContextPtr)
    {
        (void)proto;
        throw Exception("unimplemented", ErrorCodes::PROTOBUF_BAD_CAST);
    }
};
}
