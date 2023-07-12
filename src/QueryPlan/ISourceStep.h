#pragma once
#include <QueryPlan/IQueryPlanStep.h>

namespace DB
{

/// Step which takes empty pipeline and initializes it. Returns single logical DataStream.
class ISourceStep : public IQueryPlanStep
{
public:
    explicit ISourceStep(DataStream output_stream_, PlanHints hints_ = {});

    QueryPipelinePtr updatePipeline(QueryPipelines pipelines, const BuildQueryPipelineSettings & settings) override;

    virtual void initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & settings) = 0;

    void describePipeline(FormatSettings & settings) const override;

    Type getType() const override { return Type::ISource; }

    void setInputStreams(const DataStreams &) override {}

protected:
    /// We collect processors got after pipeline transformation.
    Processors processors;
};

}
