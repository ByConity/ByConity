#pragma once
#include <chrono>
#include <Interpreters/Context.h>
#include <Protos/auto_statistics.pb.h>
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/CollectorSettings.h>
#include <Statistics/StatsTableIdentifier.h>
#include <common/DayNum.h>

namespace DB::Statistics::AutoStats
{
std::tuple<UInt64, DateTime64> getTableStatsFromCatalog(CatalogAdaptorPtr catalog, const StatsTableIdentifier & table);

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
    UUID table_uuid;
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


struct InternalConfig
{
    std::chrono::seconds schedule_period{};

    // default value is set at AutoStatisticsManager::prepareNewConfig
    // here just to make sanitizer happy
    UInt64 max_retry_times = 0;
    bool collect_empty_stats_immediately = false;
    double update_ratio_threshold = 0;
    UInt64 update_row_count_threshold = 0;

    Time begin_time{};
    Time end_time{};
    std::chrono::seconds collect_interval_for_one_table{};
    std::chrono::seconds udi_flush_interval{};
    std::chrono::days task_expire{};
    std::chrono::seconds log_flush_interval_seconds;

    bool enable_async_tasks = true;

    // including enable_sample, sample_ratio, sample_row_count
    CollectorSettings collector_settings;

    explicit InternalConfig(const Settings & settings) : collector_settings(settings) { }
};

std::optional<double> calcPriority(const InternalConfig & cfg, UInt64 total_udi, UInt64 table_row_count);

}
