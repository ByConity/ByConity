/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <Optimizer/CostModel/JoinCost.h>

#include <Optimizer/CostModel/CostCalculator.h>
#include <QueryPlan/JoinStep.h>

namespace DB
{
double calculateKeySize()
{
    return 1;
}

double getAvgProbeCost(const JoinStep & step, CostContext & context)
{
    PlanNodeStatisticsPtr join_stats = context.stats;
    PlanNodeStatisticsPtr left_stats = context.children_stats[0];
    PlanNodeStatisticsPtr right_stats = context.children_stats[1];

    double cache_penalty_factor;
    double parallel_factor = context.worker_size;
    double key_size = left_stats->getColumnSize(step.getLeftKeys()) + right_stats->getColumnSize(step.getRightKeys());
    double map_size = std::min(1.0, key_size) * right_stats->getRowCount();

    if (step.getDistributionType() == DistributionType::BROADCAST)
    {
        cache_penalty_factor = std::max(1.0, std::log(map_size / 100000));
        // normalize ration when it hits the limit
        cache_penalty_factor = std::min(12.0, cache_penalty_factor);
    }
    else
    {
        cache_penalty_factor = std::max(1.0, (std::log(map_size / 100000) - std::log(parallel_factor) / std::log(2)));
        // normalize ration when it hits the limit
        cache_penalty_factor = std::min(3.0, cache_penalty_factor);
    }
    return cache_penalty_factor;
}

PlanNodeCost JoinCost::calculate(const JoinStep & step, CostContext & context)
{
    PlanNodeStatisticsPtr join_stats = context.stats;
    PlanNodeStatisticsPtr left_stats = context.children_stats[0];
    PlanNodeStatisticsPtr right_stats = context.children_stats[1];

    if (!left_stats || !right_stats)
        return PlanNodeCost::ZERO;

    bool is_broadcast = step.getDistributionType() == DistributionType::BROADCAST;

    // memory cost
    PlanNodeCost right_mem_cost = is_broadcast ? PlanNodeCost::memCost(right_stats->getRowCount() * context.worker_size)
                                               : PlanNodeCost::memCost(right_stats->getRowCount());

    if (context.cost_model.isEnableUseByteSize() && context.cost_model.isEnableUseByteSizeInJoin())
    {
        PlanNodeCost left_cpu_cost = PlanNodeCost::cpuCost(left_stats->getOutputSizeInBytes()) * getAvgProbeCost(step, context);
        PlanNodeCost right_cpu_cost
            = is_broadcast ? PlanNodeCost::cpuCost(right_stats->getOutputSizeInBytes()) * context.worker_size
                            : PlanNodeCost::cpuCost(right_stats->getOutputSizeInBytes()) ;
        PlanNodeCost join_cpu_cost = join_stats ? PlanNodeCost::cpuCost(join_stats->getOutputSizeInBytes()) : PlanNodeCost::ZERO;

        // LoggerPtr log = getLogger("JoinCost");
        // LOG_DEBUG(log, "left {}  avg {} right {} join key {} join type {} ", left_cpu_cost.getCpuValue(), getAvgProbeCost(step, context), right_cpu_cost.getCpuValue(), step.getLeftKeys()[0], step.getDistributionType() == DistributionType::REPARTITION);
        return (left_cpu_cost * context.cost_model.getJoinProbeSideCostWeight()
                + right_cpu_cost * context.cost_model.getJoinBuildSideCostWeight() + join_cpu_cost)
            * context.cost_model.getByteSizeWeight()
            + right_mem_cost * 30.0;
    }
    else
    {
        // hash-join
        // cpu cost
        // probe
        PlanNodeCost left_cpu_cost = PlanNodeCost::cpuCost(left_stats->getRowCount()) * context.cost_model.getJoinProbeSideCostWeight();

        // build
        PlanNodeCost right_cpu_cost = (is_broadcast ? PlanNodeCost::cpuCost(right_stats->getRowCount() * context.worker_size)
                                                    : PlanNodeCost::cpuCost(right_stats->getRowCount()))
            * context.cost_model.getJoinBuildSideCostWeight();
        PlanNodeCost join_cpu_cost = join_stats
            ? PlanNodeCost::cpuCost(join_stats->getRowCount()) * context.cost_model.getJoinOutputCostWeight()
            : PlanNodeCost::ZERO;
        return left_cpu_cost + right_cpu_cost + join_cpu_cost + right_mem_cost;
    }
}
}
