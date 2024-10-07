#pragma once

#include <Common/Logger.h>
#include <CloudServices/CnchDedupHelper.h>
#include <CloudServices/DedupGran.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/StorageID.h>

namespace DB
{
class DedupDataChecker : public WithContext
{
public:
    DedupDataChecker(ContextPtr context_, String logger_name_, const MergeTreeMetaBase & storage_);
    ~DedupDataChecker();

    void start()
    {
        data_checker_task->activateAndSchedule();
        is_stopped = false;
    }

    void stop()
    {
        if (data_checker_task)
        {
            data_checker_task->deactivate();
        }
        is_stopped = true;
    }

    bool isActive() { return !is_stopped; }

    DedupGranTimeMap getNeedRepairGrans(StorageCnchMergeTree & cnch_table);

    /// Used by the "SYSTEM SYNC REPAIR TASK" command to wait for duplicate data repair to complete
    /// Mainly used by ci cases
    void waitForNoDuplication(ContextPtr context);

private:
    /// For tables with relatively small data volume, check at once
    /// For tables regarded as big table, check one by one
    static constexpr auto DATA_CHECKER_QUERY = "SELECT {} count() cnt from {} {} group by {} {} having cnt > 1";
    static constexpr auto GET_REPAIR_GRAN_QUERY = R"(SELECT event_time, task_info[1] task_type, task_info[2] partition_id, task_info[3] bucket_num from cnch_system.cnch_unique_table_log where event_date = today() and database = '{}' and table = '{}' and has_error = 1 and metric = {} and event_time > {})";

    void run();
    void iterate();

    BlockIO tryToExecuteQuery(const String & query_to_execute);
    Names getCheckerQuery(StoragePtr & storage, StorageCnchMergeTree & cnch_table, bool force_check_at_once = false);
    String getRepairGranQuery(const UInt64 & event_time_filter);
    void processCheckerResult(StorageCnchMergeTree & cnch_table, Block & input_block);

    String log_name;
    LoggerPtr log;
    /// Data unique checker in background
    BackgroundSchedulePool::TaskHolder data_checker_task;
    std::atomic<bool> is_stopped{false};

    StorageID storage_id;
    UInt64 check_interval;
    UInt64 last_repair_time{0};
};
}
