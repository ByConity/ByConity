#include <Optimizer/DistinctOutputUtil.h>

#include <QueryPlan/ExceptStep.h>
#include <QueryPlan/IntersectStep.h>
#include <QueryPlan/LimitStep.h>
#include <QueryPlan/MergingSortedStep.h>
#include <QueryPlan/ValuesStep.h>

namespace DB
{
bool DistinctOutputQueryUtil::isDistinct(PlanNodeBase & node)
{
    IsDistinctPlanVisitor visitor;
    Void context;
    return VisitorUtil::accept(node, visitor, context);
}

bool IsDistinctPlanVisitor::visitPlanNode(PlanNodeBase &, Void &)
{
    return false;
}

bool IsDistinctPlanVisitor::visitValuesNode(ValuesNode & node, Void &)
{
    return dynamic_cast<const ValuesStep *>(node.getStep().get())->getRows() <= 1;
}

bool IsDistinctPlanVisitor::visitLimitNode(LimitNode & node, Void &)
{
    return dynamic_cast<const LimitStep *>(node.getStep().get())->getLimit() <= 1;
}

bool IsDistinctPlanVisitor::visitIntersectNode(IntersectNode & node, Void & context)
{
    if (dynamic_cast<const IntersectStep *>(node.getStep().get())->isDistinct())
        return true;

    for (auto & child : node.getChildren())
    {
        if (!VisitorUtil::accept(child, *this, context))
            return false;
    }
    return true;
}

bool IsDistinctPlanVisitor::visitEnforceSingleRowNode(EnforceSingleRowNode &, Void &)
{
    return true;
}

bool IsDistinctPlanVisitor::visitAggregatingNode(AggregatingNode &, Void &)
{
    return true;
}

bool IsDistinctPlanVisitor::visitAssignUniqueIdNode(AssignUniqueIdNode &, Void &)
{
    return true;
}

bool IsDistinctPlanVisitor::visitFilterNode(FilterNode & node, Void & context)
{
    return VisitorUtil::accept(node.getChildren()[0], *this, context);
}

bool IsDistinctPlanVisitor::visitDistinctNode(DistinctNode &, Void &)
{
    return true;
}

bool IsDistinctPlanVisitor::visitExceptNode(ExceptNode & node, Void & context)
{
    return dynamic_cast<const ExceptStep *>(node.getStep().get())->isDistinct() || VisitorUtil::accept(node.getChildren()[0], *this, context);
}

bool IsDistinctPlanVisitor::visitMergingSortedNode(MergingSortedNode & node, Void &)
{
    return dynamic_cast<const MergingSortedStep *>(node.getStep().get())->getLimit() <= 1;
}

}
