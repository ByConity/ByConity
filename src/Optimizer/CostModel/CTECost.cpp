#include <Optimizer/CostModel/CTECost.h>

#include <Optimizer/CostModel/CostCalculator.h>
#include <QueryPlan/CTERefStep.h>

namespace DB
{
PlanNodeCost CTECost::calculate(const CTERefStep &, CostContext & context)
{
    PlanNodeStatisticsPtr stats = context.stats;
    if (!stats)
        return PlanNodeCost::ZERO;

    return PlanNodeCost::cpuCost(stats->getRowCount()) * context.cost_model.getCTECostWeight()
        + PlanNodeCost::netCost(stats->getRowCount()) + PlanNodeCost::memCost(stats->getRowCount());
}
}
