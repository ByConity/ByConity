#pragma once
#include <Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <QueryPlan/UnionStep.h>

namespace DB
{
class UnionEstimator
{
public:
    static PlanNodeStatisticsPtr estimate(std::vector<PlanNodeStatisticsPtr> & child_stats, const UnionStep & step);

private:
    static PlanNodeStatisticsPtr mapToOutput(PlanNodeStatistics & child_stats, const std::unordered_map<String, std::vector<String>> & out_to_input, size_t index);
};

}
