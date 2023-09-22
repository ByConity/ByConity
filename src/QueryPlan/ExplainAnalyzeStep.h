#pragma once

#include <Parsers/ASTExplainQuery.h>
#include <QueryPlan/ITransformingStep.h>
#include <QueryPlan/QueryPlan.h>

namespace DB
{

class ExplainAnalyzeStep : public ITransformingStep
{
public:
    ExplainAnalyzeStep(
        const DataStream & input_stream_,
        ASTExplainQuery::ExplainKind explain_kind_,
        ContextMutablePtr context_,
        std::shared_ptr<QueryPlan> query_plan_ptr_ = nullptr,
        bool print_stats_ = true,
        bool print_profile_ = true
    );

    String getName() const override { return "ExplainAnalyze"; }
    Type getType() const override { return Type::ExplainAnalyze; }
    bool hasPlan() const { return query_plan_ptr != nullptr; }
//    void setQueryPlan(QueryPlanPtr query_plan_ptr_) { query_plan = query_plan_ptr_; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const override;
    ASTExplainQuery::ExplainKind getKind() const { return kind; }
    void setInputStreams(const DataStreams & input_streams_) override;
    bool isPrintStats() const { return print_stats; }
    bool isPrintProfile() const { return print_profile; }

    void setPlanSegmentDescriptions(PlanSegmentDescriptions & descriptions) { segment_descriptions = descriptions; }
    void toProto(Protos::ExplainAnalyzeStep & proto, bool for_hash_equals = false) const
    {
        (void)proto;
        (void)for_hash_equals;
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
    bool print_stats;
    bool print_profile;
};
using ExplainAnalyzeStepPtr = std::shared_ptr<ExplainAnalyzeStep>;

}
