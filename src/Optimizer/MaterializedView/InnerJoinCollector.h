#pragma once

#include <QueryPlan/QueryPlan.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/PlanNode.h>

#include <memory>
#include <vector>

namespace DB
{

class InnerJoinCollector : public PlanNodeVisitor<PlanNodes, Void>
{
public:

    static PlanNodes collect(const PlanNodePtr & plan)
    {
        static InnerJoinCollector collector;
        Void c;
        return VisitorUtil::accept(plan, collector, c);
    }

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
            auto res = VisitorUtil::accept(*node.getChildren()[0], *this, c);
            auto right = VisitorUtil::accept(*node.getChildren()[1], *this, c);
            res.insert(res.end(), right.begin(), right.end());
            return res;
        }
        else if (step->getKind() == ASTTableJoin::Kind::Left)
        {
            return VisitorUtil::accept(*node.getChildren()[0], *this, c);
        }
        else if (step->getKind() == ASTTableJoin::Kind::Right)
        {
            return VisitorUtil::accept(*node.getChildren()[1], *this, c);
        }
        return {};
    }
};
}
