#pragma once
#include <Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <QueryPlan/EnforceSingleRowStep.h>

namespace DB
{
class EnforceSingleRowEstimator
{
public:
    static PlanNodeStatisticsPtr estimate(PlanNodeStatisticsPtr & child_stats, const EnforceSingleRowStep & step);
};
}
