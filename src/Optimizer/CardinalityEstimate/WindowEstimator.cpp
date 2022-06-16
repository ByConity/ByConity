#include <Optimizer/CardinalityEstimate/WindowEstimator.h>

namespace DB
{
PlanNodeStatisticsPtr WindowEstimator::estimate(PlanNodeStatisticsPtr & child_stats, const WindowStep &)
{
    if (!child_stats)
    {
        return nullptr;
    }
    return child_stats->copy();
}

}
