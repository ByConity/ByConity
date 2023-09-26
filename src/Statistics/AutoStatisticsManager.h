#pragma once
#include <shared_mutex>
#include <Interpreters/Context.h>
#include <Protos/auto_statistics.pb.h>
#include <Protos/data_models.pb.h>
#include <Statistics/AutoStatisticsHelper.h>
#include <Statistics/AutoStatisticsMemoryRecord.h>
#include <Statistics/AutoStatisticsTaskQueue.h>
#include <Statistics/CatalogAdaptor.h>


namespace DB::Statistics::AutoStats
{

class AutoStatisticsManager : boost::noncopyable
{
public:
    ~AutoStatisticsManager();

    // control if MemoryRecord should be enabled
    static bool configIsEnabled();

    void prepareNewConfig(const Poco::Util::AbstractConfiguration & config);

    static void initialize(ContextPtr context_, const Poco::Util::AbstractConfiguration & config);

    static AutoStatisticsManager * tryGetInstance();

    explicit AutoStatisticsManager(ContextPtr context_);

    // gather udi_count from all servers
    // sum them up to <table_uuid, count> map
    // send them back to corresponding server, by hash of table_uuid
    void scheduleDistributeUdiCount();

    // schedule auto stats collect tasks
    void scheduleCollect();

    void writeMemoryRecord(const std::unordered_map<UUID, UInt64> & record);

private:
    void run();

    // update udi info for tables, and append task to queue if needed
    void updateUdiInfo();

    // execute a task
    // return true if succeed
    bool executeOneTask(const std::shared_ptr<TaskInfo> & chosen_task);

    // clear all information
    void clearOnException();

    // fetch udi and row count from catalog
    // ensure it's good to know
    void initializeInfo();

    // check if current time is in valid time range
    bool isNowValidTimeRange();

    // update config with parse config
    void loadNewConfigIfNeeded();

    // this mutex is to protect the whole manager
    std::mutex manager_mtx;

    std::unique_ptr<ThreadPool> thread_pool;

    ContextPtr context;
    // BackgroundSchedulePoolTaskHolder task_handle;
    // use auto_stats_task_log to implement to update task queue
    TimePoint updateTaskQueueFromLog(TimePoint min_event_time);

    void logTaskIfNeeded(const StatsTableIdentifier & table, UInt64 udi_count, UInt64 stats_row_count, DateTime64 timestamp);
    Poco::Logger * logger;

    // we don't have lock to protect internal_config since it will be accessed only single-threaded
    InternalConfig internal_config;
    std::mutex config_mtx;
    // new_internal_config will be protected by config_mtx
    std::optional<InternalConfig> new_internal_config;

    // std::unordered_map<UUID, std::shared_ptr<TaskInfo>> task_infos;
    TaskQueue task_queue;

    bool information_is_valid = false;

    // lease to ensure interval of two udi_counter
    TimePoint udi_flush_lease{};
    // to access it concurrently, make it atomic
    std::atomic<TimePoint> schedule_lease;

    AutoStatisticsMemoryRecord internal_memory_record;

    // this settings may be accessed multithreading
    static std::atomic_bool is_initialized;
    static std::atomic_bool config_is_enabled;
    static std::unique_ptr<AutoStatisticsManager> the_instance;

    TimePoint next_min_event_time{};
};

} // DB::Statistics::AutoStats
