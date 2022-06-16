#pragma once
#include <Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <QueryPlan/LimitByStep.h>
#include <QueryPlan/LimitStep.h>

namespace DB
{
class LimitEstimator
{
public:
    static PlanNodeStatisticsPtr estimate(PlanNodeStatisticsPtr & child_stats, const LimitStep &);
    static PlanNodeStatisticsPtr estimate(PlanNodeStatisticsPtr & child_stats, const LimitByStep &);
};

}
