#pragma once
#include <QueryPlan/ITransformingStep.h>

namespace DB
{
class EnforceSingleRowStep : public ITransformingStep
{
public:
    explicit EnforceSingleRowStep(const DataStream & input_stream_);

    String getName() const override { return "EnforceSingleRow"; }
    Type getType() const override { return Type::EnforceSingleRow; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & settings) override;
    void serialize(WriteBuffer & buffer) const override;
    static QueryPlanStepPtr deserialize(ReadBuffer & buffer, ContextPtr context);
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const override;
    void setInputStreams(const DataStreams & input_streams_) override;
};

}
