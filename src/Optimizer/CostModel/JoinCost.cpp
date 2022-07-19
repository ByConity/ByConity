#include <Optimizer/CostModel/JoinCost.h>

#include <Optimizer/CostModel/CostCalculator.h>
#include <QueryPlan/JoinStep.h>

namespace DB
{
PlanNodeCost JoinCost::calculate(const JoinStep & step, CostContext & context)
{
    PlanNodeStatisticsPtr join_stats = context.stats;
    PlanNodeStatisticsPtr left_stats = context.children_stats[0];
    PlanNodeStatisticsPtr right_stats = context.children_stats[1];

    if (!join_stats || !left_stats || !right_stats)
        return PlanNodeCost::ZERO;

    bool is_broadcast = step.getDistributionType() == DistributionType::BROADCAST;

    // hash-join
    // cpu cost
    // probe
    PlanNodeCost left_cpu_cost = PlanNodeCost::cpuCost(left_stats->getRowCount()) * context.cost_model.getJoinProbeSideCostWeight();
    // build
    PlanNodeCost right_cpu_cost = (is_broadcast ? PlanNodeCost::cpuCost(right_stats->getRowCount() * context.worker_size)
                                                : PlanNodeCost::cpuCost(right_stats->getRowCount()))
        * context.cost_model.getJoinBuildSideCostWeight();
    PlanNodeCost join_cpu_cost = PlanNodeCost::cpuCost(join_stats->getRowCount()) * context.cost_model.getJoinOutputCostWeight();

    // memory cost
    PlanNodeCost right_mem_cost = is_broadcast ? PlanNodeCost::memCost(right_stats->getRowCount() * context.worker_size)
                                               : PlanNodeCost::memCost(right_stats->getRowCount());

    return left_cpu_cost + right_cpu_cost + join_cpu_cost + right_mem_cost;
}
}
