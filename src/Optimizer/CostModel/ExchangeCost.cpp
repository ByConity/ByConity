#include <Optimizer/CostModel/ExchangeCost.h>

#include <Optimizer/CostModel/CostCalculator.h>
#include <QueryPlan/ExchangeStep.h>

namespace DB
{
PlanNodeCost ExchangeCost::calculate(const ExchangeStep & step, CostContext & context)
{
    // if shuffle cost is bigger then no shuffle.
    double base_cost = 1;

    // more shuffle keys is better than less shuffle keys.
    // todo data skew
    if (!step.getSchema().getPartitioningColumns().empty() && step.getSchema().getPartitioningHandle() == Partitioning::Handle::FIXED_HASH)
        base_cost += 1.0 / step.getSchema().getPartitioningColumns().size();

    if (!context.stats)
        return PlanNodeCost::netCost(base_cost);

    if (step.getSchema().getPartitioningHandle() == Partitioning::Handle::FIXED_ARBITRARY)
        return PlanNodeCost::ZERO;

    auto single_worker_cost = context.stats->getRowCount() + base_cost;
    return PlanNodeCost::netCost(
        step.getSchema().getPartitioningHandle() == Partitioning::Handle::FIXED_BROADCAST ? single_worker_cost * context.worker_size
                                                                                          : single_worker_cost);
}

}
