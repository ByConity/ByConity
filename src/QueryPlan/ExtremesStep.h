#pragma once
#include <QueryPlan/ITransformingStep.h>
namespace DB
{

/// Calculate extremes. Add special port for extremes.
class ExtremesStep : public ITransformingStep
{
public:
    explicit ExtremesStep(const DataStream & input_stream_);

    String getName() const override { return "Extremes"; }

    Type getType() const override { return Type::Extremes; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    void serialize(WriteBuffer &) const override;
    static QueryPlanStepPtr deserialize(ReadBuffer &, ContextPtr context_ = nullptr);
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const override;
    void setInputStreams(const DataStreams & input_streams_) override;
};

}
