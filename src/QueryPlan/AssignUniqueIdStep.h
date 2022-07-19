#pragma once
#include <QueryPlan/ITransformingStep.h>

namespace DB
{
class AssignUniqueIdStep : public ITransformingStep
{
public:
    explicit AssignUniqueIdStep(const DataStream & input_stream_, String unique_id_);

    String getName() const override { return "AssignUniqueId"; }
    Type getType() const override { return IQueryPlanStep::Type::AssignUniqueId; }

    void setInputStreams(const DataStreams & input_streams_) override;
    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;
    void serialize(WriteBuffer & buffer) const override;
    static QueryPlanStepPtr deserialize(ReadBuffer & buffer, ContextPtr context);

    String getUniqueId() const { return unique_id; }
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const override;

private:
    String unique_id;
};

}
