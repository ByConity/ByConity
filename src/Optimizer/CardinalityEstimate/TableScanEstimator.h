#pragma once
#include <Interpreters/Context.h>
#include <Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <QueryPlan/TableScanStep.h>

namespace DB
{
class TableScanEstimator
{
public:
    static PlanNodeStatisticsPtr estimate(ContextMutablePtr context, const TableScanStep & step);
    static std::optional<PlanNodeStatisticsPtr> estimate(
        ContextMutablePtr context, const StorageID & storage_id, const Names & columns = {});
};

}
