#include <Optimizer/CardinalityEstimate/TableScanEstimator.h>
#include <Statistics/StatisticsCollector.h>
#include <Statistics/StatsTableBasic.h>
#include <Poco/Logger.h>
#include <common/ErrorHandlers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
}

PlanNodeStatisticsPtr TableScanEstimator::estimate(ContextMutablePtr context, const TableScanStep & step)
{
    auto catalog = Statistics::createCatalogAdaptor(context);
    auto table_info_opt = catalog->getTableIdByName(step.getDatabase(), step.getTable());
    if (!table_info_opt.has_value())
    {
        // TODO: give a warning here?
        return nullptr;
    }

    PlanNodeStatisticsPtr plan_node_stats;
    try {
        Statistics::StatisticsCollector collector(context, catalog, table_info_opt.value(), 0);
        collector.readFromCatalog(step.getColumnNames());
        auto plan_node_stats_opt = collector.toPlanNodeStatistics();
        if (!plan_node_stats_opt.has_value())
        {
            return nullptr;
        }
        plan_node_stats = std::move(plan_node_stats_opt.value());
    } 
    catch(...)
    {
        auto logger = &Poco::Logger::get("TableScanEstimator");
        tryLogCurrentException(logger);
        return nullptr; 
    }
    

    NameToNameMap alias_to_column;
    for (auto & item : step.getColumnAlias())
    {
        alias_to_column[item.second] = item.first;
    }

    for (const auto & col : step.getOutputStream().header)
    {
        String column = col.name;
        if (alias_to_column.contains(col.name) && col.name != alias_to_column[col.name]
            && plan_node_stats->getSymbolStatistics().contains(alias_to_column[col.name]))
        {
            // rename
            plan_node_stats->getSymbolStatistics()[col.name] = plan_node_stats->getSymbolStatistics()[alias_to_column[col.name]];
            plan_node_stats->getSymbolStatistics().erase(alias_to_column[col.name]);
        }
        if (plan_node_stats->getSymbolStatistics().contains(col.name))
        {
            plan_node_stats->getSymbolStatistics()[col.name]->setType(col.type);
            plan_node_stats->getSymbolStatistics()[col.name]->setDbTableColumn(
                step.getDatabase() + "-" + step.getTable() + "-" + alias_to_column[col.name]);
        }
    }

    return plan_node_stats;
}
}
