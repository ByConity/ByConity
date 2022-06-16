#pragma once

#include <QueryPlan/PlanVisitor.h>

namespace DB
{
class DistinctOutputQueryUtil
{
public:
    static bool isDistinct(PlanNodeBase &);
};

class IsDistinctPlanVisitor : public PlanNodeVisitor<bool, Void>
{
public:
    bool visitPlanNode(PlanNodeBase & node, Void & c) override;
    bool visitValuesNode(ValuesNode & node, Void & context) override;
    bool visitLimitNode(LimitNode & node, Void & context) override;
    bool visitIntersectNode(IntersectNode & node, Void & context) override;
    bool visitEnforceSingleRowNode(EnforceSingleRowNode & node, Void & context) override;
    bool visitAggregatingNode(AggregatingNode & node, Void & context) override;
    bool visitAssignUniqueIdNode(AssignUniqueIdNode & node, Void & context) override;
    bool visitFilterNode(FilterNode & node, Void & context) override;
    bool visitDistinctNode(DistinctNode & node, Void & context) override;
    bool visitExceptNode(ExceptNode & node, Void & context) override;
    bool visitMergingSortedNode(MergingSortedNode & node, Void & context) override;
};
}
