#pragma once

#include <Interpreters/Context_fwd.h>
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
    static PlanNormalizeResult normalize(const QueryPlan & plan, ContextPtr context);
};

}
