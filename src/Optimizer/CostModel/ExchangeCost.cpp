#include <Optimizer/CostModel/ExchangeCost.h>

#include <Optimizer/CostModel/CostCalculator.h>
#include <QueryPlan/ExchangeStep.h>

namespace DB
{
PlanNodeCost ExchangeCost::calculate(const ExchangeStep & step, CostContext & context)
{
    if (!context.stats)
    {
        return PlanNodeCost::ZERO;
    }

    if (step.getSchema().getPartitioningHandle() == Partitioning::Handle::FIXED_ARBITRARY)
    {
        return PlanNodeCost::ZERO;
    }

    return PlanNodeCost::netCost(context.stats->getRowCount());
}

}
