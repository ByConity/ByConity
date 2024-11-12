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
    COUNT_TO_UINT32_OR_NULL = 0,
    COUNT_TO_FLOAT32_OR_NULL = 1,
    COUNT_TO_DATE_OR_NULL = 2,
    COUNT_TO_DATE_TIME_OR_NULL = 3,
};

/* column -> (type -> value) */
using WorkloadExtendedStat = std::unordered_map<WorkloadExtendedStatsType, Field>;
using WorkloadExtendedStats = std::unordered_map<String, WorkloadExtendedStat>;
using WorkloadExtendedStatsPtr = std::shared_ptr<WorkloadExtendedStats>;

class WorkloadTableStats
{
public:
    static WorkloadTableStats build(ContextPtr context, const String & database_name, const String & table_name);

    WorkloadExtendedStatsPtr collectExtendedStats(ContextPtr context,
                                                  const String & database_name,
                                                  const String & table_name,
                                                  const NamesAndTypesList & columns);

    PlanNodeStatisticsPtr getBasicStats() { return basic_stats; }

private:
    explicit WorkloadTableStats(PlanNodeStatisticsPtr basic_stats_)
        : basic_stats(basic_stats_)
        , extended_stats(std::make_shared<WorkloadExtendedStats>())
    {
    }

    PlanNodeStatisticsPtr basic_stats;
    WorkloadExtendedStatsPtr extended_stats;

    static const char * getStatsAggregation(const WorkloadExtendedStatsType & type)
    {
        switch (type)
        {
            case WorkloadExtendedStatsType::COUNT_TO_UINT32_OR_NULL: return "count(toUInt32OrNull({}))";
            case WorkloadExtendedStatsType::COUNT_TO_FLOAT32_OR_NULL: return "count(toFloat32OrNull({}))";
            case WorkloadExtendedStatsType::COUNT_TO_DATE_OR_NULL: return "count(toDateOrNull({}))";
            case WorkloadExtendedStatsType::COUNT_TO_DATE_TIME_OR_NULL: return "count(toDateTimeOrNull({}))";
        }
    }

};

}
