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

class PlanNormalizeResult
{
public:
    explicit PlanNormalizeResult() = default;
    explicit PlanNormalizeResult(std::unordered_map<std::shared_ptr<const PlanNodeBase>, QueryPlanStepPtr> _steps): normal_steps(std::move(_steps)) {}

    void addOrUpdate(std::shared_ptr<const PlanNodeBase> node, QueryPlanStepPtr normal_step);
    // returns the hash for the normalized step if node is found; otherwise returns the hash of node->step()
    size_t getNormalizedHash(std::shared_ptr<const PlanNodeBase> node) const;
    PlanNodePtr buildNormalizedPlanAt(std::shared_ptr<const PlanNodeBase> root, ContextPtr context) const { return buildNormalizedPlanImpl(root, context); }

protected: // protected for test
    std::unordered_map<std::shared_ptr<const PlanNodeBase>, QueryPlanStepPtr> normal_steps;
private:
    PlanNodePtr buildNormalizedPlanImpl(std::shared_ptr<const PlanNodeBase> node, ContextPtr context) const;
};

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
