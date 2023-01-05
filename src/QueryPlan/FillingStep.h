#pragma once
#include <QueryPlan/ITransformingStep.h>
#include <Core/SortDescription.h>

namespace DB
{

/// Implements modifier WITH FILL of ORDER BY clause. See FillingTransform.
class FillingStep : public ITransformingStep
{
public:
    FillingStep(const DataStream & input_stream_, SortDescription sort_description_);

    String getName() const override { return "Filling"; }

    Type getType() const override { return Type::Filling; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const SortDescription & getSortDescription() const { return sort_description; }

    void serialize(WriteBuffer &) const override;
    static QueryPlanStepPtr deserialize(ReadBuffer &, ContextPtr context_ = nullptr);
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const override;
    void setInputStreams(const DataStreams & input_streams_) override;

private:
    SortDescription sort_description;
};

}
