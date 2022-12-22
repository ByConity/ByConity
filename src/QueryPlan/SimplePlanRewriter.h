#pragma once

#include <memory>
#include <Interpreters/Context.h>
#include <QueryPlan/CTEVisitHelper.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/CTERefStep.h>
#include <QueryPlan/PlanNode.h>

namespace DB
{
template <typename C>
class SimplePlanRewriter : public PlanNodeVisitor<PlanNodePtr, C>
{
public:
    SimplePlanRewriter(ContextMutablePtr context_, CTEInfo & cte_info) : context(std::move(context_)), cte_helper(cte_info) { }

    PlanNodePtr visitPlanNode(PlanNodeBase & node, C & c) override
    {

        if (node.getChildren().empty())
            return node.shared_from_this();
        PlanNodes children;
        DataStreams inputs;
        for (const auto & item : node.getChildren())
        {
            PlanNodePtr child = VisitorUtil::accept(*item, *this, c);
            children.emplace_back(child);
            inputs.push_back(child->getStep()->getOutputStream());
        }

        auto new_step = node.getStep()->copy(context);
        new_step->setInputStreams(inputs);
        node.setStep(new_step);

        node.replaceChildren(children);
        return node.shared_from_this();
    }

    PlanNodePtr visitCTERefNode(CTERefNode & node, C & c) override
    {
        auto cte_step = node.getStep();
        auto cte_id = cte_step->getId();
        auto cte_plan = cte_helper.acceptAndUpdate(cte_id, *this, c);
        return cte_plan;
    }

protected:
    ContextMutablePtr context;
    CTEPreorderVisitHelper cte_helper;
};
}
