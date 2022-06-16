#pragma once
#include <Optimizer/CostModel/PlanNodeCost.h>
#include <QueryPlan/ProjectionStep.h>

namespace DB
{
struct CostContext;

class ProjectionCost
{
public:
    static PlanNodeCost calculate(const ProjectionStep & step, CostContext & context);
};

}
