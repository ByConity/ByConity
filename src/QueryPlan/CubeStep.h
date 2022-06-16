#pragma once
#include <QueryPlan/ITransformingStep.h>
#include <DataStreams/SizeLimits.h>
#include <Interpreters/Aggregator.h>

namespace DB
{

struct AggregatingTransformParams;
using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;

/// WITH CUBE. See CubeTransform.
class CubeStep : public ITransformingStep
{
public:
    CubeStep(const DataStream & input_stream_, AggregatingTransformParamsPtr params_);

    String getName() const override { return "Cube"; }

    Type getType() const override { return Type::Cube; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    const Aggregator::Params & getParams() const;

    void serialize(WriteBuffer & buf) const override;
    static QueryPlanStepPtr deserialize(ReadBuffer & buf, ContextPtr);
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const override;
    void setInputStreams(const DataStreams & input_streams_) override;

private:
    AggregatingTransformParamsPtr params;
};

}
