#include <Optimizer/Signature/PlanSegmentNormalizer.h>
#include <QueryPlan/PlanPrinter.h>

namespace DB
{

PlanNodePtr PlanSegmentNormalizer::buildNormalPlan(QueryPlan::Node * node, PlanNormalizerOptions options)
{
    std::unordered_map<QueryPlan::Node *, StepAndOutputOrder> results;
    return buildNormalPlanForNodeImpl(node, results, options);
}

PlanNodePtr PlanSegmentNormalizer::buildNormalPlanForNodeImpl(
    QueryPlan::Node * node, std::unordered_map<QueryPlan::Node *, StepAndOutputOrder> & results, PlanNormalizerOptions options)
{
    if (!node)
        return nullptr;
    PlanNodes new_children;
    for (const auto & child : node->children)
        new_children.emplace_back(buildNormalPlanForNodeImpl(child, results, options));
    auto result = NormalizeNodeVisitor::normalize(context, node, results, options);
    auto plan_node = PlanNodeBase::createPlanNode(node->id, result.normal_step, std::move(new_children));
    return plan_node;
}

PlanNodePtr PlanSegmentNormalizer::buildNormalPlan(PlanNodePtr node, PlanNormalizerOptions options)
{
    std::unordered_map<PlanNodePtr, StepAndOutputOrder> results;
    return buildNormalPlanForNodeImpl(node, results, options);
}

PlanNodePtr PlanSegmentNormalizer::buildNormalPlanForNodeImpl(
    PlanNodePtr node, std::unordered_map<PlanNodePtr, StepAndOutputOrder> & results, PlanNormalizerOptions options)
{
    if (!node)
        return nullptr;
    PlanNodes new_children;
    for (const auto & child : node->getChildren())
        new_children.emplace_back(buildNormalPlanForNodeImpl(child, results, options));
    auto result = NormalizeVisitor::normalize(node, *cte_info, context, results, options);
    auto plan_node = PlanNodeBase::createPlanNode(node->getId(), result.normal_step, std::move(new_children));
    return plan_node;
}

String generatePlanSegmentPlanHash(PlanSegment * plan_segment, const ContextPtr & context)
{
    auto & plan = plan_segment->getQueryPlan();
    auto normalizer = PlanSegmentNormalizer(context);
    auto logical_plan = normalizer.buildNormalPlan(plan.getRoot(), {.normalize_literals = true, .normalize_storage = true});
    auto logical_plan_str = PlanPrinter::textPlanNode(logical_plan, context);
    return logical_plan_str;
}
}
