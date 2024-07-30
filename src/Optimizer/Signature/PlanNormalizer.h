#pragma once

#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Optimizer/Signature/StepNormalizer.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/QueryPlan.h>

#include <memory>
#include <unordered_map>
#include <utility>

namespace DB
{

class PlanNormalizer
{
public:
    using NormalSteps = std::unordered_map<PlanNodePtr, StepAndOutputOrder>;
    virtual ~PlanNormalizer() = default;
    PlanNormalizer(PlanNormalizer &&) = default;
    PlanNormalizer(const PlanNormalizer &) = default;

    explicit PlanNormalizer(const CTEInfo & _cte_info, ContextPtr _context): cte_info(_cte_info), context(_context)
    {
    }

    static PlanNormalizer from(const QueryPlan & plan, ContextPtr _context) { return PlanNormalizer(plan.getCTEInfo(), _context); }

    QueryPlanStepPtr computeNormalStep(PlanNodePtr node)
    {
        return computeNormalStepImpl(node).normal_step;
    }
    Block computeNormalOutputOrder(PlanNodePtr node)
    {
        return computeNormalStepImpl(node).output_order;
    }
    PlanNodePtr buildNormalPlan(PlanNodePtr root)
    {
        return buildNormalPlanImpl(root);
    }

protected:
    virtual StepAndOutputOrder computeNormalStepImpl(PlanNodePtr node);

private:
    const CTEInfo & cte_info;
    ContextPtr context;
    NormalSteps normal_steps;
    PlanNodePtr buildNormalPlanImpl(PlanNodePtr node);
};

}
