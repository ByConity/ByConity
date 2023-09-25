#pragma once

#include <Core/Field.h>
#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <fmt/core.h>

#include <memory>
#include <unordered_map>


namespace DB
{

enum class WorkloadExtendedStatsType
{
    HLL_STATS = 0,
    COUNT_DISTINCT_STATS = 1
};

/* column -> (type -> value) */
using WorkloadExtendedStats = std::unordered_map<String, std::unordered_map<WorkloadExtendedStatsType, Field>>;
using WorkloadExtendedStatsPtr = std::shared_ptr<WorkloadExtendedStats>;

class WorkloadTableStats
{
public:
    static WorkloadTableStats build(ContextPtr context, const String & database_name, const String & table_name);

    WorkloadExtendedStatsPtr collectExtendedStats(ContextPtr context,
                                                  const String & database_name,
                                                  const String & table_name,
                                                  const NamesAndTypesList & columns);

    PlanNodeStatisticsPtr basic_stats;
    WorkloadExtendedStatsPtr extended_stats;

private:
    explicit WorkloadTableStats(PlanNodeStatisticsPtr basic_stats_)
        : basic_stats(basic_stats_)
        , extended_stats(std::make_shared<WorkloadExtendedStats>())
    {
    }

    static const char * getStatsAggregation(const WorkloadExtendedStatsType & type)
    {
        switch (type)
        {
            case WorkloadExtendedStatsType::HLL_STATS: return "uniqHLL12({})";
            case WorkloadExtendedStatsType::COUNT_DISTINCT_STATS: return "count(distinct {})";
        }
    }

};

}
