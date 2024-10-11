#pragma once
#include <Processors/ISimpleTransform.h>
#include <QueryPlan/ExplainAnalyzeStep.h>
#include <Interpreters/ProcessorProfile.h>
#include <Interpreters/Context.h>
//#include <Interpreters/profile/ProfileLogHub.h>
#include <Interpreters/ProcessorsProfileLog.h>


namespace DB
{
using ProcessorsSet = std::unordered_set<const IProcessor *>;

class ExplainAnalyzeTransform : public ISimpleTransform
{
public:
    ExplainAnalyzeTransform(
        const Block & input_header_,
        const Block & output_header_,
        ASTExplainQuery::ExplainKind kind_,
        std::shared_ptr<QueryPlan> query_plan_ptr_,
        ContextMutablePtr context_,
        PlanSegmentDescriptions & segment_descriptions_,
        QueryPlanSettings settings = {});

    String getName() const override { return "ExplainAnalyzeTransform"; }
protected:
    void transform(Chunk & chunk) override;
    ISimpleTransform::Status prepare() override;

    void getProcessorProfiles(ProcessorsSet & processors_set, ProcessorProfiles & profiles, const IProcessor * processor);
    void getRemoteProcessorProfiles(std::unordered_map<size_t, std::unordered_map<String, ProcessorProfiles>> & segment_profiles);
private:
    ASTExplainQuery::ExplainKind kind;
    ContextMutablePtr context;
    std::shared_ptr<QueryPlan> query_plan_ptr;
    PlanSegmentDescriptions segment_descriptions;
    bool has_final_transform = true;
    QueryPlanSettings settings;
    String coordinator_address;
};
}
