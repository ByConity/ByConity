#include <Optimizer/CostModel/ProjectionCost.h>

#include <Optimizer/CostModel/CostCalculator.h>
#include <QueryPlan/ProjectionStep.h>

namespace DB
{
PlanNodeCost ProjectionCost::calculate(const ProjectionStep &, CostContext & context)
{
    PlanNodeStatisticsPtr children_stats = context.children_stats[0];
    if (!children_stats)
        return PlanNodeCost::ZERO;
    return PlanNodeCost::cpuCost(children_stats->getRowCount()) * context.cost_model.getProjectionCostWeight();
}
}
