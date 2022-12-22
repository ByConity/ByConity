#pragma once

#include <Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <QueryPlan/AssignUniqueIdStep.h>

namespace DB
{
class AssignUniqueIdEstimator
{
public:
    static PlanNodeStatisticsPtr estimate(PlanNodeStatisticsPtr & child_stats, const AssignUniqueIdStep &);
};

}
