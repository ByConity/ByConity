#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <DataStreams/SizeLimits.h>

namespace DB
{

/// Executes OFFSET (without LIMIT). See OffsetTransform.
class OffsetStep : public ITransformingStep
{
public:
    OffsetStep(const DataStream & input_stream_, size_t offset_);

    String getName() const override { return "Offset"; }

    Type getType() const override { return Type::Offset; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    void serialize(WriteBuffer &) const override;
    static QueryPlanStepPtr deserialize(ReadBuffer &, ContextPtr context_ = nullptr);

private:
    size_t offset;
};

}
