#pragma once

#include <Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <QueryPlan/AggregatingStep.h>
#include <QueryPlan/DistinctStep.h>
#include <QueryPlan/MergingAggregatedStep.h>

namespace DB
{
class AggregateEstimator
{
public:
    static PlanNodeStatisticsPtr estimate(PlanNodeStatisticsPtr & child_stats, const AggregatingStep &);
    static PlanNodeStatisticsPtr estimate(PlanNodeStatisticsPtr & child_stats, const MergingAggregatedStep &);
    static PlanNodeStatisticsPtr estimate(PlanNodeStatisticsPtr & child_stats, const DistinctStep &);

private:
    static SymbolStatisticsPtr estimateAggFun(AggregateFunctionPtr agg_function, UInt64 row_count, DataTypePtr);
};

}
