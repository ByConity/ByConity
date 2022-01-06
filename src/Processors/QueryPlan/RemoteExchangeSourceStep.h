#pragma once

#include <Processors/QueryPlan/ISourceStep.h>
#include <Poco/Logger.h>
#include "Interpreters/DistributedStages/AddressInfo.h"
#include <Interpreters/Context_fwd.h>
#include <Processors/Exchange/ExchangeOptions.h>

namespace DB
{
class PlanSegmentInput;
using PlanSegmentInputPtr = std::shared_ptr<PlanSegmentInput>;
using PlanSegmentInputs = std::vector<PlanSegmentInputPtr>;

class PlanSegment;
class RemoteExchangeSourceStep : public ISourceStep
{
public:
    explicit RemoteExchangeSourceStep(PlanSegmentInputs inputs_, DataStream input_stream_);

    String getName() const override { return "RemoteExchangeSource"; }
    Type getType() const override { return Type::RemoteExchangeSource; }

    void initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & settings) override;

    PlanSegmentInputs getInput() const { return inputs; }

    void setPlanSegment(PlanSegment * plan_segment_);
    PlanSegment * getPlanSegment() const { return plan_segment; }

    void serialize(WriteBuffer & buf) const override;
    static QueryPlanStepPtr deserialize(ReadBuffer & buf, ContextPtr);

    void describePipeline(FormatSettings & settings) const override;

    void setExchangeOptions(ExchangeOptions options_) { options = options_; }

private:
    PlanSegmentInputs inputs;
    PlanSegment * plan_segment = nullptr;
    Poco::Logger * logger;
    size_t plan_segment_id;
    String query_id;
    String coordinator_address;
    AddressInfo read_address_info;
    ContextPtr context;
    ExchangeOptions options;
};
}
