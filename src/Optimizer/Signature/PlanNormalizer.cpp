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
class NormalizeVisitor : public PlanNodeVisitor<StepAndOutputOrder, PlanNormalizeResult>
{
public:
    explicit NormalizeVisitor(ContextPtr _context, const CTEInfo & _cte_info) : step_normalizer(_context), cte_info(_cte_info) {}

protected:
    StepAndOutputOrder visitPlanNode(PlanNodeBase & node, PlanNormalizeResult & res) override { return visitPlanNodeImpl(node, res); }
    StepAndOutputOrder visitCTERefNode(CTERefNode & node, PlanNormalizeResult & res) override {
        auto cte_step = node.getStep();
        CTEId cte_id = cte_step->getId();
        std::vector<StepAndOutputOrder> cte_root_result;
        auto it = cte_cache.find(cte_id);
        if (it != cte_cache.end())
            cte_root_result.emplace_back(it->second);
        else
        {
            StepAndOutputOrder root_res = VisitorUtil::accept(cte_info.getCTEs().at(cte_id), *this, res);
            cte_cache.emplace(cte_id, root_res);
            cte_root_result.emplace_back(std::move(root_res));
        }
        StepAndOutputOrder step_result = step_normalizer.normalize(cte_step, std::move(cte_root_result));
        res.addOrUpdate(node.shared_from_this(), step_result.normal_step);
        return step_result;
    }

private:
    const CTEInfo & cte_info;
    StepNormalizer step_normalizer;
    const CTEInfo & cte_info;
    std::unordered_map<CTEId, StepAndOutputOrder> cte_cache;

    StepAndOutputOrder visitPlanNodeImpl(const PlanNodeBase & node, PlanNormalizeResult & res)
    {
        std::vector<StepAndOutputOrder> children_results{};
        for (const auto & child : node.getChildren())
            children_results.emplace_back(VisitorUtil::accept(child, *this, res));
        StepAndOutputOrder step_result = step_normalizer.normalize(node.getStep(), std::move(children_results));
        res.addOrUpdate(node.shared_from_this(), step_result.normal_step);
        return step_result;
    }
};
} // anonymous namespace

void PlanNormalizeResult::addOrUpdate(std::shared_ptr<const PlanNodeBase> node, QueryPlanStepPtr normal_step)
{
    auto it = normal_steps.find(node);
    if (it != normal_steps.end())
        it->second = normal_step;
    else
        normal_steps.emplace(node, normal_step);
}

size_t PlanNormalizeResult::getNormalizedHash(std::shared_ptr<const PlanNodeBase> node) const
{
    auto it = normal_steps.find(node);
    return it != normal_steps.end() ? it->second->hash() : node->getStep()->hash();
}

PlanNodePtr PlanNormalizeResult::buildNormalizedPlanImpl(std::shared_ptr<const PlanNodeBase> node, ContextPtr context) const
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
