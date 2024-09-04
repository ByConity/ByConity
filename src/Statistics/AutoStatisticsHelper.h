#pragma once
#include <chrono>
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/CollectorSettings.h>
#include <Statistics/SettingsMap.h>
#include <Statistics/StatisticsSettings.h>
#include <Statistics/StatsTableIdentifier.h>
#include <Statistics/ASTHelpers.h>
#include <Common/DayNum.h>

namespace DB::Statistics::AutoStats
{
void autoStatisticsCallback(ContextPtr context, const ASTPtr & ast_raw, UInt64 udi_count);

using Time = DateLUTImpl::Time;
using TimePoint = std::chrono::system_clock::time_point;
using Status = Protos::AutoStats::TaskStatus;


///              +--------+                              +-------+
///              |        |   Retry times exceeds limit  |       |
///              | Failed <------------------------------+ Error <-----------+
///              |        |                              |       |           |
///              +--------+                              +-+---+-+           |
///                                                        |   |             |
///                                                        |   |             |
///                      +-----------+   Duplicated task   |   |             |
///                      |           <---------------------+   |             |
///                      | Cancelled |                         | Retry       |
///                      |           <---------------------+   |             |
///                      +-----^-----+   Duplicated task   |   |             |
///                            |                           |   |             | Encounter
///                            | Duplicated task           |   |             | exceptions
///                            |                           |   |             | while
///  Manually async SQL   +----+----+                   +--+---v--+          | collecting
///  or auto statistics   |         |                   |         |          | statistics
/// ----------------------> Created +-------------------> Pending |          |
///  write task to log    |         | Add to TaskQueue  |         |          |
///                       +---------+                   +----+----+          |
///                                                          |               |
///                                                          |               |
///                                                          |               |
///                                     Start creating stats |               |
///                                                          |               |
///                                                          |               |
///              +---------+                            +----v----+          |
///              |         |                            |         |          |
///              | Success <----------------------------+ Running +----------+
///              |         |  Finish collecting stats   |         |
///              +---------+                            +---------+
using TaskType = Protos::AutoStats::TaskType;

std::optional<Time> getTimeFromString(const String & text);

// a simplified version of AutoStatsTaskLogElement
// only important fields are kept
struct TaskInfoCore
{
    // fixed fields, no need to lock
    UUID task_uuid;
    TaskType task_type;
    StatsTableIdentifier table = StatsTableIdentifier(StorageID::createEmpty());
    std::vector<String> columns_name;
    String settings_json;

    // dynamic fields, require lock protection in TaskInfo
    UInt64 stats_row_count;
    UInt64 udi_count;
    double priority;
    UInt64 retry_times;
    Status status;
};

bool betweenTime(Time target, Time beg, Time end);

DateTime64 convertToDateTime64(TimePoint time);

TimePoint convertToTimePoint(DateTime64 time);
ExtendedDayNum convertToDate(DateTime64);

String serializeToText(DateTime64 time);
String serializeToText(ExtendedDayNum date);

TimePoint nowTimePoint();


using InternalConfig = AutoStatsManagerSettings;

std::optional<double> calcPriority(const InternalConfig & cfg, UInt64 total_udi, UInt64 table_row_count);

DataTypePtr enumDataTypeForTaskType();
DataTypePtr enumDataTypeForStatus();

std::vector<StatisticsScope> getChildrenScope(ContextPtr context, const StatisticsScope & scope);
std::vector<StatsTableIdentifier> getTablesInScope(ContextPtr context, const StatisticsScope & scope);
std::tuple<UInt64, DateTime64> getTableStatsFromCatalog(CatalogAdaptorPtr catalog, const StatsTableIdentifier & table);

struct PredicatesHint
{
    std::optional<String> database_opt;
    std::optional<String> table_opt;
};

PredicatesHint extractPredicates(const IAST & query, ContextPtr context);
}
