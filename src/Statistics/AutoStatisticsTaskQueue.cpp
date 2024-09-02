#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/now64.h>
#include <Statistics/AutoStatisticsTaskQueue.h>
#include <Statistics/AutoStatsTaskLogHelper.h>

namespace DB::Statistics::AutoStats
{

static bool canExecuteTask(TaskQueue::Mode mode, TaskType task_type, bool is_empty_table)
{
    if (task_type == TaskType::Manual)
        return mode.enable_async_tasks;
    else if (is_empty_table)
        return mode.enable_immediate_auto_stats;
    else
        return mode.enable_normal_auto_stats;
}


std::shared_ptr<TaskInfo> TaskQueue::chooseTask(Mode mode)
{
    std::unique_lock lck(mtx);

    if (task_infos.empty())
    {
        return nullptr;
    }

    double max_priority = -1e9;
    std::shared_ptr<TaskInfo> chosen_task;
    // int out_of_lease = 0;
    auto now = nowTimePoint();
    for (auto & [unique_key, task_info] : task_infos)
    {
        if (now < task_info->getStartLease())
        {
            // ++out_of_lease;
            continue;
        }

        if (task_info->getStatus() != Status::Pending)
            continue;

        auto [udi_count, stats_row_count, task_priority] = task_info->getParams();

        if (!canExecuteTask(mode, task_info->getTaskType(), stats_row_count == 0))
            continue;

        // retry tasks has the lowest priority
        double real_priority = task_priority - 10.0 * task_info->getRetryTimes();
        if (real_priority > max_priority)
        {
            max_priority = real_priority;
            chosen_task = task_info;
        }
    }

    return chosen_task;
}

void TaskQueue::clear()
{
    std::unique_lock lck(mtx);
    task_infos.clear();
}

void TaskQueue::erase(UUID unique_key)
{
    std::unique_lock lck(mtx);
    task_infos.erase(unique_key);
}

size_t TaskQueue::size() const
{
    std::unique_lock lck(mtx);
    return task_infos.size();
}

void TaskQueue::updateTasksFromLogIfNeeded(const TaskInfoLog & log)
{
    std::unique_lock lck(mtx);
    auto context = getContext();
    auto unique_key = log.table.getUniqueKey();
    auto min_start_lease = nowTimePoint() + std::chrono::seconds(internal_config.schedule_period_seconds());

    if (task_infos.count(unique_key))
    {
        auto old_task_info = task_infos.at(unique_key);

        if (old_task_info->getTaskUUID() != log.task_uuid)
        {
            // here two tasks working on a same table

            bool choose_old = false;
            auto core_old = old_task_info->getCore();
            auto core_new = static_cast<TaskInfoCore>(log);

            // users may submit a new manual task, and the AutoStatsManager may submit a new auto task
            //   that duplicates with existing auto task or old manual task
            // we should keep the oldest manual task
            if (core_old.task_type == TaskType::Manual)
            {
                // if the old is manual, we just keep it, and cancel the new one
                // wherever the new one is auto task or manual task
                choose_old = true;
            }
            else if (core_new.task_type == TaskType::Manual)
            {
                // cancel the existing auto task
                choose_old = false;
            }
            else
            {
                // duplicated auto task
                // it should be not possible
                // but here we just choose the higher priority one
                choose_old = core_old.priority >= core_new.priority;
            }

            if (choose_old)
            {
                core_new.status = Status::Cancelled;
                auto info = fmt::format(FMT_STRING("Replaced by another task {}"), toString(core_old.task_uuid));
                writeTaskLog(context, core_new, info);
                return;
            }
            else
            {
                core_old.status = Status::Cancelled;
                auto info = fmt::format(FMT_STRING("Replaced by another task {}"), toString(core_new.task_uuid));
                writeTaskLog(context, core_old, info);
                task_infos.erase(unique_key);
                // continue execution as if we have no old task
            }
        }
        else
        {
            // get log of the same task, possibly due to restart or fetch the same log again
            // we just update the params to latest
            old_task_info->setParams(log.udi_count, log.stats_row_count, log.priority);
            return;
        }
    }

    switch (log.status)
    {
        case Status::Created: {
            // find new task, put it into TaskQueue, and set status=Pending
            auto core = static_cast<TaskInfoCore>(log);
            core.status = Status::Pending;
            writeTaskLog(context, core);
            auto new_task = std::make_shared<TaskInfo>(core, min_start_lease);
            task_infos.emplace(unique_key, new_task);
            return;
        }
        case Status::Pending: {
            // find pending task not in TaskQueue, possibly due to restart
            // just add it into TaskQueue
            auto core = static_cast<TaskInfoCore>(log);
            auto new_task = std::make_shared<TaskInfo>(core, min_start_lease);
            task_infos.emplace(unique_key, new_task);
            return;
        }
        case Status::Error: {
            // find task in error status
            // we will attempt to retry it
            auto core = static_cast<TaskInfoLog>(log);

            if (core.retry_times >= internal_config.auto_stats_max_retry_times())
            {
                // retry time exceeds limit, we just fail the task
                core.status = Status::Failed;
                auto info = fmt::format(FMT_STRING("Retry {} times, failed"), core.retry_times);
                writeTaskLog(context, core, info);
                task_infos.erase(unique_key);
                return;
            }
            // retry the task by add it to log
            // this task will be added to queue next time fetching log
            core.status = Status::Pending;
            core.retry_times += 1;
            writeTaskLog(context, core, "Retry");

            return;
        }
        default:
            auto err_msg
                = fmt::format("Unexpected status {} for task {}", Protos::AutoStats::TaskStatus_Name(log.status), toString(log.task_uuid));
            LOG_ERROR(logger, err_msg);
            return;
    }
}

std::shared_ptr<TaskInfo> TaskQueue::tryGetTaskInfo(UUID uuid)
{
    std::unique_lock lck(mtx);
    if (task_infos.count(uuid))
        return task_infos.at(uuid);
    else
        return nullptr;
}

}
