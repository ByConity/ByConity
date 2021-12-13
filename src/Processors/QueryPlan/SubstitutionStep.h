#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class ActionsDAG;
using ActionsDAGPtr = std::shared_ptr<ActionsDAG>;

/// Implements column substitution for materialized view rewrite
class SubstitutionStep : public ITransformingStep
{
public:
    SubstitutionStep(
        const DataStream & input_stream_,
        const std::unordered_map<String, String> & name_substitution_info_);

    String getName() const override { return "Substitution"; }
    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    std::unordered_map<String, String> name_substitution_info;
};

}
