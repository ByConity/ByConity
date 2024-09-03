#pragma once
#include <Statistics/CollectorSettings.h>
#include <Statistics/StatisticsCollector.h>

namespace DB::Statistics
{

struct CollectTarget
{
    CollectTarget(
        ContextPtr context, StatsTableIdentifier table_identifier_, CollectorSettings settings_, const std::vector<String> & columns_name)
        : table_identifier(table_identifier_), settings(settings_)
    {
        init(context, columns_name);
    }

    StatsTableIdentifier table_identifier;
    CollectorSettings settings;
    bool implicit_all_columns = true;
    ColumnDescVector columns_desc;

private:
    void init(ContextPtr context, const std::vector<String> & columns_name);
};

std::optional<UInt64> collectStatsOnTarget(ContextPtr context, const CollectTarget & collect_target);

}
