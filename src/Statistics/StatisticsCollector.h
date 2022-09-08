#pragma once
#include <utility>
#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Optimizer/CardinalityEstimate/SymbolStatistics.h>
#include <Protos/optimizer_statistics.pb.h>
#include <Statistics/Base64.h>
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/StatisticsCollectorObjects.h>
#include <common/logger_useful.h>

namespace DB::Statistics
{

// uuid level
class StatisticsCollector
{
public:
    friend class CollectStep;

    using TableStats = StatisticsImpl::TableStats;
    using ColumnStats = StatisticsImpl::ColumnStats;
    using ColumnStatsMap = StatisticsImpl::ColumnStatsMap;
    using ColumnName = String;

    StatisticsCollector(ContextPtr context_, CatalogAdaptorPtr catalog_, const StatsTableIdentifier & table_info_, TxnTimestamp timestamp_)
        : context(context_), catalog(catalog_), table_info(table_info_), timestamp(timestamp_)
    {
        String str;
        auto & settings = catalog->getSettingsRef();
        collect_debug_level = settings.statistics_collect_debug_level;
        logger = &Poco::Logger::get("StatisticsLogger" + table_info.getDbTableName());
    }

    void collectFull();
    void collect(const ColumnDescVector & col_names);

    void writeToCatalog();
    void readAllFromCatalog();
    void readFromCatalog(const ColumnDescVector & cols_desc);
    void readFromCatalog(const std::vector<String> & cols_name);

    std::optional<PlanNodeStatisticsPtr> toPlanNodeStatistics() const;

    const auto & getTableStats() const { return table_stats; }
    const auto & getColumnsStats() const { return columns_stats; }
    void setTableStats(TableStats && stats) { table_stats = std::move(stats); }
    void setColumnStats(String col_name, ColumnStats && col_stats) { columns_stats[col_name] = std::move(col_stats); }

private:
    ContextPtr context;
    Poco::Logger * logger;
    CatalogAdaptorPtr catalog;
    StatsTableIdentifier table_info;
    [[maybe_unused]] TxnTimestamp timestamp;

    // table stats
    TableStats table_stats;

    // column stats
    ColumnStatsMap columns_stats;


    int collect_debug_level = 0;
};
}
