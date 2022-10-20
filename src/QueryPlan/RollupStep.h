#pragma once
#include <QueryPlan/ITransformingStep.h>
#include <DataStreams/SizeLimits.h>

namespace DB
{

struct AggregatingTransformParams;
using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;

/// WITH ROLLUP. See RollupTransform.
class RollupStep : public ITransformingStep
{
public:
    RollupStep(const DataStream & input_stream_, AggregatingTransformParamsPtr params_);

    String getName() const override { return "Rollup"; }

    Type getType() const override { return Type::Rollup; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    void serialize(WriteBuffer & buf) const override;
    static QueryPlanStepPtr deserialize(ReadBuffer & buf, ContextPtr);
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const override;
    void setInputStreams(const DataStreams & input_streams_) override;

private:
    size_t keys_size;
    AggregatingTransformParamsPtr params;
};

}
