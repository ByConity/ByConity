#pragma once
#include "Common/tests/gtest_global_context.h"
#include <Common/Logger.h>
#include <shared_mutex>
#include <Interpreters/Context.h>
#include <Protos/auto_statistics.pb.h>
#include <Protos/data_models.pb.h>
#include <Statistics/AutoStatisticsHelper.h>
#include <Statistics/AutoStatisticsMemoryRecord.h>
#include <Statistics/AutoStatisticsTaskQueue.h>
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/SettingsManager.h>


namespace DB::Statistics::AutoStats
{

ContextMutablePtr createContextWithAuth(ContextPtr global_context);

class AutoStatisticsManager : boost::noncopyable
{
public:
    friend class AutoStatisticsCommand;
    ~AutoStatisticsManager();

    // control if MemoryRecord should be enabled
    static bool xmlConfigIsEnable();

    void prepareNewConfig(const Poco::Util::AbstractConfiguration & config);

    static void initialize(ContextMutablePtr context_, const Poco::Util::AbstractConfiguration & config);

    explicit AutoStatisticsManager(ContextMutablePtr auth_context_);

    void markCollectableCandidates(const std::vector<StatsTableIdentifier> & candidates, bool force_collect_if_failed_to_query_row_count);

    SettingsManager & getSettingsManager() { return settings_manager; }


    // gather udi_count from all servers
    // sum them up to <table_uuid, count> map
    // send them back to corresponding server, by hash of table_uuid
    void scheduleDistributeUdiCount();

    // schedule auto stats collect tasks
    void scheduleCollect();

    void writeMemoryRecord(const std::unordered_map<UUID, UInt64> & record);

    std::vector<TaskInfoCore> getAllTasks() { return task_queue.getAllTasks(); }

    ContextMutablePtr getContext()
    {
        return auth_context;
    }

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

    void scanAllTables();

    // use auto_stats_task_log to implement to update task queue
    TimePoint updateTaskQueueFromLog(TimePoint min_event_time);

    void logTaskIfNeeded(const StatsTableIdentifier & table, UInt64 udi_count, UInt64 stats_row_count);

    void createTask(const StatisticsScope & scope);
    

    ContextMutablePtr auth_context;
    LoggerPtr logger;

    // we don't have lock to protect internal_config since it will be accessed only single-threaded
    InternalConfig internal_config;

    // std::unordered_map<UUID, std::shared_ptr<TaskInfo>> task_infos;
    TaskQueue task_queue;

    bool information_is_valid = false;

    // store last time instead of lease to make sure settings is effective immediately
    TimePoint last_time_udi_flush{};
    TimePoint last_time_scan_all_tables{};

    SettingsManager settings_manager;
    AutoStatisticsMemoryRecord internal_memory_record;

    // this settings may be accessed multithreading
    static std::atomic_bool is_initialized;
    static std::atomic_bool xml_config_is_enable;

    std::optional<TimePoint> next_min_event_time;

    std::mutex manager_mtx;
    std::unique_ptr<ThreadPool> thread_pool;
    // to access it concurrently, make it atomic
    std::atomic<TimePoint> schedule_lease;
};

} // DB::Statistics::AutoStats
