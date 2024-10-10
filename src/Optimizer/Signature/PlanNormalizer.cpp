#include <Optimizer/Signature/PlanNormalizer.h>

#include <Interpreters/Context_fwd.h>
#include <Optimizer/Signature/StepNormalizer.h>
#include <QueryPlan/CTEVisitHelper.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/QueryPlan.h>

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

namespace DB
{
StepAndOutputOrder NormalizeVisitor::visitCTERefNodeImpl(PlanNodeBase & node, PlanNormalizer::NormalSteps & normal_steps)
{
    auto node_ptr = node.shared_from_this();
    if (auto it = normal_steps.find(node_ptr); it != normal_steps.end())
        return it->second;

    std::vector<StepAndOutputOrder> cte_root_result{};
    CTEId cte_id = static_pointer_cast<CTERefStep>(node.getStep())->getId();
    cte_root_result.emplace_back(VisitorUtil::accept(cte_info.getCTEs().at(cte_id), *this, normal_steps));
    StepAndOutputOrder step_result = step_normalizer.normalize(node.getStep(), std::move(cte_root_result));
    normal_steps.emplace(node_ptr, step_result);
    return step_result;
}

StepAndOutputOrder NormalizeVisitor::visitPlanNodeImpl(PlanNodeBase & node, PlanNormalizer::NormalSteps & normal_steps)
{
    auto node_ptr = node.shared_from_this();
    if (auto it = normal_steps.find(node_ptr); it != normal_steps.end())
        return it->second;

    std::vector<StepAndOutputOrder> children_results{};
    for (const auto & child : node.getChildren())
        children_results.emplace_back(VisitorUtil::accept(child, *this, normal_steps));
    StepAndOutputOrder step_result = step_normalizer.normalize(node.getStep(), std::move(children_results));
    normal_steps.emplace(node_ptr, step_result);
    return step_result;
}

StepAndOutputOrder NormalizeNodeVisitor::normalize(
    ContextPtr _context,
    QueryPlan::Node * node,
    std::unordered_map<QueryPlan::Node *, StepAndOutputOrder> & results,
    PlanNormalizerOptions options)
{
    NormalizeNodeVisitor visitor(_context, options);
    return VisitorUtil::accept(node, visitor, results);
}

StepAndOutputOrder
NormalizeNodeVisitor::visitNode(QueryPlan::Node * node, std::unordered_map<QueryPlan::Node *, StepAndOutputOrder> & results)
{
    if (auto iter = results.find(node); iter != results.end())
        return iter->second;
    std::vector<StepAndOutputOrder> children_results{};
    for (auto * child : node->children)
        children_results.emplace_back(VisitorUtil::accept(child, *this, results));
    StepAndOutputOrder step_result = step_normalizer.normalize(node->step, std::move(children_results));
    results.emplace(node, step_result);
    return step_result;
}

StepAndOutputOrder PlanNormalizer::computeNormalStepImpl(PlanNodePtr node, PlanNormalizerOptions options)
{
    if (auto it = normal_steps.find(node); it != normal_steps.end())
        return it->second;

    return NormalizeVisitor::normalize(node, cte_info, context, normal_steps, options);
}

PlanNodePtr PlanNormalizer::buildNormalPlanImpl(PlanNodePtr node, PlanNormalizerOptions options)
{
    if (!node)
        return nullptr;

    PlanNodes new_children;
    for (const auto & child : node->getChildren())
        new_children.emplace_back(buildNormalPlanImpl(child, options));
    QueryPlanStepPtr normal_step = computeNormalStepImpl(node, options).normal_step;
    return PlanNodeBase::createPlanNode(node->getId(), normal_step, std::move(new_children));
}

}
