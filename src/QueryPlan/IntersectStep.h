#pragma once
#include <QueryPlan/SetOperationStep.h>

namespace DB
{
class IntersectStep : public SetOperationStep
{
public:
    IntersectStep(
        DataStreams input_streams_,
        DataStream output_stream_,
        std::unordered_map<String, std::vector<String>> output_to_inputs_,
        bool distinct_);

    IntersectStep(DataStreams input_streams_, DataStream output_stream_, bool distinct_) : IntersectStep(input_streams_, output_stream_, {}, distinct_) { }

    String getName() const override { return "Intersect"; }
    Type getType() const override { return Type::Intersect; }

    QueryPipelinePtr updatePipeline(QueryPipelines pipelines, const BuildQueryPipelineSettings & context) override;
    void serialize(WriteBuffer & buffer) const override;
    static QueryPlanStepPtr deserialize(ReadBuffer & buffer, ContextPtr context);

    bool isDistinct() const;
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const override;

private:
    bool distinct;
};
}
