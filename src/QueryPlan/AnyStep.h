#pragma once
#include <QueryPlan/IQueryPlanStep.h>

namespace DB
{
using GroupId = UInt32;

class AnyStep : public IQueryPlanStep
{
public:
    AnyStep(DataStream output, GroupId group_id_) : group_id(group_id_) { output_stream = output; }

    String getName() const override { return "Leaf"; }
    Type getType() const override { return Type::Any; }

    QueryPipelinePtr updatePipeline(QueryPipelines pipelines, const BuildQueryPipelineSettings & context) override;
    void serialize(WriteBuffer & buf) const override;
    static QueryPlanStepPtr deserialize(ReadBuffer & buf, ContextPtr context);

    GroupId getGroupId() const { return group_id; }

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr context) const override;
    void setInputStreams(const DataStreams & input_streams_) override;

private:
    GroupId group_id;
};
}
