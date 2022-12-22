#include <Optimizer/CardinalityEstimate/AssignUniqueIdEstimator.h>

namespace DB
{
PlanNodeStatisticsPtr AssignUniqueIdEstimator::estimate(PlanNodeStatisticsPtr & child_stats, const AssignUniqueIdStep & step)
{
    if (!child_stats)
    {
        return nullptr;
    }

    String unique_symbol = step.getUniqueId();
    auto unique_stats = std::make_shared<SymbolStatistics>(child_stats->getRowCount(), 0, 0, 0, 8);

    std::unordered_map<String, SymbolStatisticsPtr> symbol_statistics;
    for (auto & stats : child_stats->getSymbolStatistics())
    {
        symbol_statistics[stats.first] = stats.second->copy();
    }
    symbol_statistics[unique_symbol] = unique_stats;

    auto stats = std::make_shared<PlanNodeStatistics>(child_stats->getRowCount(), symbol_statistics);
    return stats;
}

}
