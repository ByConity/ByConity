#pragma once

#include <Interpreters/Context.h>
#include <Optimizer/Property/Property.h>
#include <QueryPlan/PlanNode.h>

namespace DB
{
class GroupExpression;
using GroupExprPtr = std::shared_ptr<GroupExpression>;

class PropertyEnforcer
{
public:
    static PlanNodePtr
    enforceNodePartitioning(const PlanNodePtr & node, const Property & required, const Property & actual, Context & context);
    static PlanNodePtr
    enforceStreamPartitioning(const PlanNodePtr & node, const Property & required, const Property & actual, Context & context);
    static GroupExprPtr
    enforceNodePartitioning(const GroupExprPtr & group_expr, const Property & required, const Property & actual, const Context & context);
    static GroupExprPtr
    enforceStreamPartitioning(const GroupExprPtr & group_expr, const Property & required, const Property & actual, const Context & context);
    static QueryPlanStepPtr
    enforceNodePartitioning(ConstQueryPlanStepPtr step, const Property & required, const Property & actual, const Context & context);
    static QueryPlanStepPtr
    enforceStreamPartitioning(ConstQueryPlanStepPtr step, const Property & required, const Property & actual, const Context & context);
};
}
