#include <Optimizer/CardinalityEstimate/LimitEstimator.h>

namespace DB
{
PlanNodeStatisticsPtr LimitEstimator::estimate(PlanNodeStatisticsPtr & child_stats, const LimitStep & step)
{
    size_t limit = step.getLimit();
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

PlanNodeStatisticsPtr LimitEstimator::estimate(PlanNodeStatisticsPtr & child_stats, const LimitByStep & step)
{
    UInt64 limit = step.getGroupLength();
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
