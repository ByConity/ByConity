#include <Optimizer/CardinalityEstimate/SampleEstimator.h>

namespace DB
{
PlanNodeStatisticsPtr SampleEstimator::estimate(PlanNodeStatisticsPtr & child_stats, const FinalSampleStep & step)
{
    if (!child_stats)
    {
        return nullptr;
    }

    auto stats_rows = child_stats->getRowCount();
    auto sample_size = std::min(step.getSampleSize(), step.getMaxChunkSize());


    if (stats_rows < sample_size)
        return child_stats;

    return std::make_shared<PlanNodeStatistics>(sample_size, child_stats->getSymbolStatistics());
}

}
