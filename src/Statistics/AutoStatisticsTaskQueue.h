#pragma once
#include <Common/Logger.h>
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

// TaskInfo
struct TaskInfo : public TaskInfoCore
{
public:
    TaskInfo(const TaskInfoCore & core_, TimePoint create_time_, std::optional<TimePoint> stats_time_opt_)
        : TaskInfoCore(core_), create_time(create_time_), stats_time_opt(stats_time_opt_)
    {
    }

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

    TimePoint getLease(const AutoStatsManagerSettings & settings) const
    {
        std::unique_lock lck(task_mu);
        auto lease = create_time + std::chrono::seconds(settings.schedule_period_seconds());
        if (stats_time_opt)
        {
            auto lease2 = stats_time_opt.value() + std::chrono::seconds(settings.update_interval_seconds());
            lease = std::max(lease, lease2);
        }
        return lease;
    }

    // void setTime(TimePoint create_time_, std::optional<DateTime64> stats_time_opt_)
    // {
    //     std::unique_lock lck(task_mu);
    //     create_time = create_time_;
    //     stats_time_opt = stats_time_opt_;
    // }

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

private:
    mutable std::mutex task_mu;

    // use create time instead of lease
    // to make sure changed settings will be effective immediately
    TimePoint create_time;
    std::optional<TimePoint> stats_time_opt;
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
    std::vector<TaskInfoCore> getAllTasks();

    void erase(UUID uuid);

    size_t size() const;
    void clear();

private:
    mutable std::mutex mtx;

    // table uuid -> task
    std::unordered_map<UUID, std::shared_ptr<TaskInfo>> task_infos;
    LoggerPtr logger = getLogger("AutoStats::TaskQueue");
    const InternalConfig & internal_config;
};

}
