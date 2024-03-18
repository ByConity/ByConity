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

#include <Optimizer/CardinalityEstimate/LimitEstimator.h>

namespace DB
{
PlanNodeStatisticsPtr LimitEstimator::estimate(PlanNodeStatisticsPtr & child_stats, const LimitStep & step)
{
    return step.hasPreparedParam() ? child_stats : getLimitStatistics(child_stats, step.getLimitValue());
}

PlanNodeStatisticsPtr LimitEstimator::estimate(PlanNodeStatisticsPtr & child_stats, const LimitByStep & step)
{
    size_t limit = step.getGroupLength();
    return getLimitStatistics(child_stats, limit);
}

PlanNodeStatisticsPtr LimitEstimator::estimate(PlanNodeStatisticsPtr & child_stats, const OffsetStep & offset)
{
    if (!child_stats)
    {
        return nullptr;
    }

    if (child_stats->getRowCount() <= offset.getOffset())
    {
        return std::make_shared<PlanNodeStatistics>(0, child_stats->getSymbolStatistics());
    }

    return std::make_shared<PlanNodeStatistics>(child_stats->getRowCount() - offset.getOffset(), child_stats->getSymbolStatistics());
}

PlanNodeStatisticsPtr LimitEstimator::getLimitStatistics(PlanNodeStatisticsPtr & child_stats, size_t limit)
{
    if (!child_stats)
    {
        return std::make_shared<PlanNodeStatistics>(limit);
    }

    if (child_stats->getRowCount() <= limit)
    {
        return child_stats->copy();
    }

    return std::make_shared<PlanNodeStatistics>(limit, child_stats->getSymbolStatistics());
}

}
