#include <Optimizer/CostModel/ValuesCost.h>

#include <Optimizer/CostModel/CostCalculator.h>

namespace DB
{
PlanNodeCost ValuesCost::calculate(const ValuesStep & step, CostContext & context)
{
    return PlanNodeCost::cpuCost(step.getRows()) * context.cost_model.getProjectionCostWeight();
}
}
