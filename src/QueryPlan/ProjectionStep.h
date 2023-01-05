#pragma once

#include <QueryPlan/Assignment.h>
#include <QueryPlan/ITransformingStep.h>
#include <Optimizer/DynamicFilters.h>
#include <Core/NameToType.h>

namespace DB
{
class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class ProjectionStep : public ITransformingStep
{
public:
    explicit ProjectionStep(
        const DataStream & input_stream_,
        Assignments assignments_,
        NameToType name_to_type_,
        bool final_project_ = false,
        std::unordered_map<String, DynamicFilterBuildInfo> dynamic_filters_ = {});

    String getName() const override { return "Projection"; }
    Type getType() const override { return Type::Projection; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;
    void serialize(WriteBuffer & buf) const override;
    static QueryPlanStepPtr deserialize(ReadBuffer & buf, ContextPtr context);

    ActionsDAGPtr createActions(ContextPtr context) const;
    const Assignments & getAssignments() const { return assignments; }
    const NameToType & getNameToType() const { return name_to_type; }

    const std::unordered_map<String, DynamicFilterBuildInfo> & getDynamicFilters() const { return dynamic_filters; }

    bool isFinalProject() const { return final_project; }
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const override;
    void setInputStreams(const DataStreams & input_streams_) override;

private:
    Assignments assignments;
    NameToType name_to_type;
    // final output step
    bool final_project;

    std::unordered_map<String, DynamicFilterBuildInfo> dynamic_filters;

    void buildDynamicFilterPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & build_context) const;
};

}
