#pragma once
#include <mutex>
#include <shared_mutex>
#include <Interpreters/AutoStatsTaskLog.h>
#include <Protos/auto_statistics.pb.h>
#include <Statistics/AutoStatisticsHelper.h>
#include <Statistics/StatsTableIdentifier.h>
#include <common/logger_useful.h>

namespace DB::Statistics::AutoStats
{

struct TaskInfoLog;


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
struct TaskInfo : private TaskInfoCore
{
public:
    TaskInfo(const TaskInfoCore & core_, TimePoint start_lease_) : TaskInfoCore(core_), start_lease(start_lease_) { }

    UUID getTaskUUID() const { return task_uuid; }
    StatsTableIdentifier getTable() const { return table; }
    UInt64 getRetryTimes() const { return retry_times; }
    TaskType getTaskType() const { return task_type; }
    const std::vector<String> & getColumnsName() const { return columns_name; }
    const String & getSettingsJson() const { return settings_json; }

    double getPriority() const
    {
        std::unique_lock lck(task_mu);
        return priority;
    }

    TimePoint getStartLease() const
    {
        std::unique_lock lck(task_mu);
        return start_lease;
    }

    void setStartLease(TimePoint start_lease_)
    {
        std::unique_lock lck(task_mu);
        start_lease = start_lease_;
    }

    TaskInfoCore getCore() const
    {
        std::unique_lock lck(task_mu);
        return static_cast<TaskInfoCore>(*this);
    }


    std::tuple<UInt64, UInt64, double> getParams() const
    {
        std::unique_lock lck(task_mu);
        return std::make_tuple(udi_count, stats_row_count, priority);
    }

    void setParams(UInt64 udi_count_, UInt64 stats_row_count_, double priority_)
    {
        std::unique_lock lck(task_mu);
        udi_count = udi_count_;
        stats_row_count = stats_row_count_;
        priority = priority_;
    }

    // if status == Error, just set Exception
    // no need to call this
    void setStatus(Status status_)
    {
        std::unique_lock lck(task_mu);
        status = status_;
    }

    Status getStatus() const
    {
        std::unique_lock lck(task_mu);
        return status;
    }

    // atomically set these values for retry
    void resetWith(Status status_, TimePoint start_lease_, UInt64 retry_times_)
    {
        std::unique_lock lck(task_mu);
        status = status_;
        start_lease = start_lease_;
        retry_times = retry_times_;
    }

private:
    mutable std::mutex task_mu;

    // when udi is collected
    // wait for at least collect_interval_for_one_table before collection
    // to make sure table has been stable
    TimePoint start_lease;
};

class TaskQueue : WithContext
{
public:
    TaskQueue(ContextPtr context_, const InternalConfig & internal_config_) : WithContext(context_), internal_config(internal_config_) { }

    struct Mode
    {
        // async tasks are submitted manually by user
        bool enable_async_tasks;
        // tables have empty stats yet usually should be collected immediately
        bool enable_immediate_auto_stats;
        // others are just normal tasks
        bool enable_normal_auto_stats;
    };

    std::shared_ptr<TaskInfo> chooseTask(Mode mode);

    std::shared_ptr<TaskInfo> tryGetTaskInfo(UUID uuid);

    void updateTasksFromLogIfNeeded(const TaskInfoLog & log);

    void erase(UUID uuid);

    size_t size() const;
    void clear();


private:
    mutable std::mutex mtx;

    // table uuid -> task
    std::unordered_map<UUID, std::shared_ptr<TaskInfo>> task_infos;
    Poco::Logger * logger = &Poco::Logger::get("AutoStats::TaskQueue");
    const InternalConfig & internal_config;
};

}
