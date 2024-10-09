#include <Advisor/WorkloadTableStats.h>

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Poco/Logger.h>
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/StatisticsCollector.h>
#include <Statistics/SubqueryHelper.h>
#include <Statistics/TypeUtils.h>
#include <fmt/core.h>

#include <memory>
#include <set>


namespace DB
{

static std::vector<WorkloadExtendedStatsType> extended_stats_types = {
    WorkloadExtendedStatsType::COUNT_TO_UINT32_OR_NULL,
    WorkloadExtendedStatsType::COUNT_TO_FLOAT32_OR_NULL,
    WorkloadExtendedStatsType::COUNT_TO_DATE_OR_NULL,
    WorkloadExtendedStatsType::COUNT_TO_DATE_TIME_OR_NULL
};

WorkloadTableStats WorkloadTableStats::build(ContextPtr context, const String & database_name, const String & table_name)
{
    auto stats_catalog = Statistics::createCatalogAdaptor(context);
    auto stats_table_id = stats_catalog->getTableIdByName(database_name, table_name);
    if (!stats_table_id)
    {
        return WorkloadTableStats{nullptr};
    }

    PlanNodeStatisticsPtr basic_stats;
    try
    {
        Statistics::StatisticsCollector collector(context, stats_catalog, stats_table_id.value(), {});
        collector.readAllFromCatalog();
        basic_stats = collector.toPlanNodeStatistics().value_or(nullptr);
        if (basic_stats)
            LOG_DEBUG(getLogger("WorkloadTableStats"), "Stats for table {}.{}: {} rows, {} symbols",
                      database_name, table_name, basic_stats->getRowCount(), basic_stats->getSymbolStatistics().size());
    } catch (...) {}

    return WorkloadTableStats{basic_stats};
}

WorkloadExtendedStatsPtr WorkloadTableStats::collectExtendedStats(
    ContextPtr context,
    const String & database,
    const String & table,
    const NamesAndTypesList & columns)
{
    std::set<String> columns_to_collect; /* ordered set */
    for (const auto & column : columns)
    {
        /// TODO: currently only String type is targeted for collection of extended statistics
        auto decayed_tpe = Statistics::decayDataType(column.type);
        bool is_string = decayed_tpe->getTypeId() == TypeIndex::String || decayed_tpe->getTypeId() == TypeIndex::FixedString;
        if (is_string && !extended_stats->contains(column.name))
            columns_to_collect.emplace(column.name);
    }

    if (columns_to_collect.empty())
        return extended_stats;

    String query = "SELECT ";
    for (const auto & column : columns_to_collect)
    {
        for (const auto & type : extended_stats_types)
        {
            query += fmt::format(getStatsAggregation(type), column) + ", ";
        }
    }
    query.pop_back();
    query.pop_back();
    query += fmt::format(" FROM {}.{}", database, table);

    LOG_DEBUG(getLogger("WorkloadTableStats"), "Collecting extended stats for table: {}", query);

    auto query_context = Statistics::SubqueryHelper::createQueryContext(context);
    auto result = executeSubQueryWithOneRow(query, query_context, true);

    auto column_it = result.cbegin();

    for (const auto & column : columns_to_collect)
    {
        for (const auto & type : extended_stats_types)
        {
            (*extended_stats)[column][type] = (*column_it->column)[0];
            ++column_it;
        }
    }
    
    return extended_stats;
}


} // namespace DB
