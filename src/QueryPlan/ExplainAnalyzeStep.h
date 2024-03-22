#pragma once

#include <optional>
#include <Parsers/ASTExplainQuery.h>
#include <QueryPlan/ITransformingStep.h>
#include <QueryPlan/QueryPlan.h>
#include <Interpreters/ExplainSettings.h>

namespace DB
{
struct PlanSegmentDescription;
using PlanSegmentDescriptionPtr = std::shared_ptr<PlanSegmentDescription>;
using PlanSegmentDescriptions = std::vector<PlanSegmentDescriptionPtr>;

class ExplainAnalyzeStep : public ITransformingStep
{
public:
    ExplainAnalyzeStep(
        const DataStream & input_stream_,
        const String & output_name_,
        ASTExplainQuery::ExplainKind explain_kind_,
        ContextMutablePtr context_,
        std::shared_ptr<QueryPlan> query_plan_ptr_,
        QueryPlanSettings settings);

    String getName() const override { return "ExplainAnalyze"; }
    Type getType() const override { return Type::ExplainAnalyze; }
    bool hasPlan() const { return query_plan_ptr != nullptr; }
        const std::shared_ptr<QueryPlan> & getQueryPlan() const { return query_plan_ptr; }
    const ContextMutablePtr & getContext() const { return context; }
    const QueryPlanSettings & getSetting() const { return settings; }
    String getOutputName() const
    {
        return output_stream->header.getByPosition(0).name;
    }
//    void setQueryPlan(QueryPlanPtr query_plan_ptr_) { query_plan = query_plan_ptr_; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const override;
    ASTExplainQuery::ExplainKind getKind() const { return kind; }
    void setInputStreams(const DataStreams & input_streams_) override;

    void setPlanSegmentDescriptions(PlanSegmentDescriptions & descriptions) { segment_descriptions = descriptions; }
    void toProto([[maybe_unused]] Protos::ExplainAnalyzeStep & proto, [[maybe_unused]] bool for_hash_equals = false) const
    {
        // do nothing
    }

    static std::shared_ptr<ExplainAnalyzeStep> fromProto(const Protos::ExplainAnalyzeStep & proto, ContextPtr)
    {
        (void)proto;
        throw Exception("unimplemented", ErrorCodes::PROTOBUF_BAD_CAST);
    }

private:
    ASTExplainQuery::ExplainKind kind;
    ContextMutablePtr context;
    std::shared_ptr<QueryPlan> query_plan_ptr;
    PlanSegmentDescriptions segment_descriptions;
    QueryPlanSettings settings;
};
using ExplainAnalyzeStepPtr = std::shared_ptr<ExplainAnalyzeStep>;

}
