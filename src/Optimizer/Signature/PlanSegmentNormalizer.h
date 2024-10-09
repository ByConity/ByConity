#pragma once

#include <Interpreters/Context_fwd.h>
#include <Optimizer/Signature/PlanNormalizer.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/QueryPlan.h>

namespace DB
{
class PlanSegmentNormalizer
{
public:
    explicit PlanSegmentNormalizer(ContextPtr context_, CTEInfo * _cte_info = nullptr) : cte_info(_cte_info), context(context_)
    {
    }
    PlanNodePtr buildNormalPlan(QueryPlan::Node * node, PlanNormalizerOptions options = {});
    PlanNodePtr buildNormalPlan(PlanNodePtr node, PlanNormalizerOptions options = {});

private:
    PlanNodePtr buildNormalPlanForNodeImpl(
        QueryPlan::Node * node, std::unordered_map<QueryPlan::Node *, StepAndOutputOrder> & results, PlanNormalizerOptions options);
    PlanNodePtr buildNormalPlanForNodeImpl(
        PlanNodePtr node, std::unordered_map<PlanNodePtr, StepAndOutputOrder> & results, PlanNormalizerOptions options);
    CTEInfo * cte_info;
    ContextPtr context;
};

String generatePlanSegmentPlanHash(PlanSegment * plan_segment, const ContextPtr & context);
}
