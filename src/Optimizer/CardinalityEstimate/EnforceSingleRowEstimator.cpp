#include <Optimizer/CardinalityEstimate/EnforceSingleRowEstimator.h>

namespace DB
{
PlanNodeStatisticsPtr EnforceSingleRowEstimator::estimate(PlanNodeStatisticsPtr & child_stats, const EnforceSingleRowStep &)
{
    if (!child_stats)
    {
        return nullptr;
    }

    auto stats = child_stats->copy();
    stats->updateRowCount(1);
    for (auto & item : stats->getSymbolStatistics())
    {
        item.second->setNdv(1);
    }
    return stats;
}

}
