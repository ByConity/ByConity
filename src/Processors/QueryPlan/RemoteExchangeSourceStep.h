#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

class PlanSegmentInput;
using PlanSegmentInputPtr = std::shared_ptr<PlanSegmentInput>;
using PlanSegmentInputs = std::vector<PlanSegmentInputPtr>;

class PlanSegment;

class RemoteExchangeSourceStep : public IQueryPlanStep
{
public:
    explicit RemoteExchangeSourceStep(const PlanSegmentInputs & inputs_, DataStream input_stream_);

    String getName() const override { return "RemoteExchangeSource"; }

    Type getType() const override { return Type::RemoteExchangeSource; }

    QueryPipelinePtr updatePipeline(QueryPipelines pipelines, const BuildQueryPipelineSettings & settings) override;

    PlanSegmentInputs getInput() const { return inputs; }

    void setPlanSegment(PlanSegment * plan_segment_) { plan_segment = plan_segment_; }

    PlanSegment * getPlanSegment() const { return plan_segment; }

    void serialize(WriteBuffer & buf) const override;
    static QueryPlanStepPtr deserialize(ReadBuffer & buf, ContextPtr);

private:

    PlanSegmentInputs inputs;

    PlanSegment * plan_segment;
};

}
