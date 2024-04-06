#pragma once

#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/QueryPlan.h>

#include <memory>
#include <vector>

namespace DB
{

class InnerJoinCollector : public PlanNodeVisitor<PlanNodes, Void>
{
public:
    void collect(const PlanNodePtr & plan)
    {
        Void c;
        inner_sources = VisitorUtil::accept(plan, *this, c);
    }

    PlanNodes getInnerSources() const { return inner_sources; }
    PlanNodes getOuterSources() const { return outer_sources; }

protected:
    PlanNodes visitPlanNode(PlanNodeBase & node, Void & c) override
    {
        PlanNodes res;
        for (const auto & child : node.getChildren())
        {
            auto child_res = VisitorUtil::accept(*child, *this, c);
            res.insert(res.end(), child_res.begin(), child_res.end());
        }
        return res;
    }

    PlanNodes visitTableScanNode(TableScanNode & node, Void &) override { return {node.shared_from_this()}; }

    PlanNodes visitJoinNode(JoinNode & node, Void & c) override
    {
        auto & step = node.getStep();
        if (step->getKind() == ASTTableJoin::Kind::Inner)
        {
            auto left = VisitorUtil::accept(*node.getChildren()[0], *this, c);
            auto right = VisitorUtil::accept(*node.getChildren()[1], *this, c);
            left.insert(left.end(), right.begin(), right.end());
            return left;
        }
        else if (step->getKind() == ASTTableJoin::Kind::Left)
        {
            auto left = VisitorUtil::accept(*node.getChildren()[0], *this, c);
            auto right = VisitorUtil::accept(*node.getChildren()[1], *this, c);
            outer_sources.insert(outer_sources.end(), right.begin(), right.end());
            return left;
        }
        else if (step->getKind() == ASTTableJoin::Kind::Right)
        {
            auto left_result = VisitorUtil::accept(*node.getChildren()[0], *this, c);
            outer_sources.insert(outer_sources.end(), left_result.begin(), left_result.end());
            return VisitorUtil::accept(*node.getChildren()[1], *this, c);
        }
        return {};
    }

private:
    PlanNodes inner_sources;
    PlanNodes outer_sources;
};
}
