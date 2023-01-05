#pragma once
#include <Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <QueryPlan/ExchangeStep.h>

namespace DB
{
class ExchangeEstimator
{
public:
    static PlanNodeStatisticsPtr estimate(std::vector<PlanNodeStatisticsPtr> & child_stats, const ExchangeStep & step);

private:
    static PlanNodeStatisticsPtr
    mapToOutput(PlanNodeStatisticsPtr & child_stats, const std::unordered_map<String, std::vector<String>> & out_to_input, size_t index);
};
};
