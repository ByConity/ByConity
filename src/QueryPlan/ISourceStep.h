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

    void setInputStreams(const DataStreams &) override {}

    // this won't be override, so use a different name
    void serializeToProtoBase(Protos::ISourceStep & proto) const;
    // return step_description and base_output_stream
    static Block deserializeFromProtoBase(const Protos::ISourceStep & proto);

protected:
    /// We collect processors got after pipeline transformation.
    Processors processors;
};

}
