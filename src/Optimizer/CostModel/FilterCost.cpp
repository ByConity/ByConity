#include <Optimizer/CostModel/FilterCost.h>

#include <Optimizer/CostModel/CostCalculator.h>
#include <QueryPlan/FilterStep.h>

namespace DB
{
PlanNodeCost FilterCost::calculate(const FilterStep &, CostContext & context)
{
    PlanNodeStatisticsPtr children_stats = context.children_stats[0];
    if (!children_stats)
        return PlanNodeCost::ZERO;
    return PlanNodeCost::cpuCost(children_stats->getRowCount()) * context.cost_model.getProjectionCostWeight();
}
}
