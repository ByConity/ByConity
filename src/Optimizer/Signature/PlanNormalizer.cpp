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

namespace
{
// NormalizeVisitor is actually a ConstVisitor, which normalizes each step in the tree in post-order
class NormalizeVisitor : public PlanNodeVisitor<StepAndOutputOrder, PlanNormalizer::NormalSteps>
{
public:
    static StepAndOutputOrder
    normalize(PlanNodePtr node, const CTEInfo & _cte_info, ContextPtr _context, PlanNormalizer::NormalSteps & normal_steps)
    {
        NormalizeVisitor visitor(_cte_info, _context);
        return VisitorUtil::accept(node, visitor, normal_steps);
    }

protected:
    StepAndOutputOrder visitPlanNode(PlanNodeBase & node, PlanNormalizer::NormalSteps & normal_steps) override { return visitPlanNodeImpl(node, normal_steps); }
    StepAndOutputOrder visitCTERefNode(CTERefNode & node, PlanNormalizer::NormalSteps & normal_steps) override { return visitCTERefNodeImpl(node, normal_steps); }
    StepAndOutputOrder visitPlanNodeImpl(PlanNodeBase & node, PlanNormalizer::NormalSteps & normal_steps)
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
    StepAndOutputOrder visitCTERefNodeImpl(PlanNodeBase & node, PlanNormalizer::NormalSteps & normal_steps)
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

private:
    const CTEInfo & cte_info;
    StepNormalizer step_normalizer;
    explicit NormalizeVisitor(const CTEInfo & _cte_info, ContextPtr _context): cte_info(_cte_info), step_normalizer(_context)
    {
    }
};
} // anonymous namespace

StepAndOutputOrder PlanNormalizer::computeNormalStepImpl(PlanNodePtr node)
{
    if (auto it = normal_steps.find(node); it != normal_steps.end())
        return it->second;

    return NormalizeVisitor::normalize(node, cte_info, context, normal_steps);
}

PlanNodePtr PlanNormalizer::buildNormalPlanImpl(PlanNodePtr node)
{
    if (!node)
        return nullptr;

    PlanNodes new_children;
    for (const auto & child : node->getChildren())
        new_children.emplace_back(buildNormalPlanImpl(child));
    QueryPlanStepPtr normal_step = computeNormalStepImpl(node).normal_step;
    return PlanNodeBase::createPlanNode(node->getId(), normal_step, std::move(new_children));
}

}
