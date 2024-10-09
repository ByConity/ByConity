#include <cmath>
#include <Backups/BackupCopyTool.h>
#include <Backups/BackupUtils.h>
#include <CloudServices/CnchWorkerClient.h>
#include <Disks/DiskByteS3.h>
#include <Interpreters/WorkerGroupHandle.h>
#include <Common/Logger.h>
#include <Common/ProfileEvents.h>
#include <Common/setThreadName.h>
#include <common/logger_useful.h>


namespace ProfileEvents
{
    extern const Event S3CopyObject;
}

namespace DB
{

Protos::BackupCopyTask createBackupCopyTask(
    const String & source_disk, const String & source_path, const String & destination_disk, const String & destination_path)
{
    Protos::BackupCopyTask task;
    task.set_source_disk(source_disk);
    task.set_source_path(source_path);
    task.set_destination_disk(destination_disk);
    task.set_destination_path(destination_path);
    return task;
}

void updateCopyTaskProgressIfNeeded(
    const ContextPtr & context,
    BackupTaskPtr & backup_task,
    size_t total_tasks_number,
    size_t finished_tasks_number,
    size_t & current_progress_percent)
{
    size_t new_progress_percent = std::min(100.0, std::ceil(100.0 * finished_tasks_number / total_tasks_number));
    if (new_progress_percent > current_progress_percent)
    {
        current_progress_percent = new_progress_percent;
        backup_task->updateProgressWithLock(fmt::format("Copying data {}%", new_progress_percent), context);
    }
}

void sendCopyTasksToWorker(BackupTaskPtr & backup_task, const BackupCopyTasks & copy_tasks, const ContextPtr & context)
{
    checkBackupTaskNotAborted(backup_task->getId(), context);
    backup_task->updateProgress("Start copying data", context);

    // for progress calculation
    size_t total_tasks_number = copy_tasks.size();
    size_t hdfs_finished_files = 0;
    std::atomic<size_t> s3_finished_files = 0;
    size_t current_progress_percent = 0;

    // Tasks from s3 to s3
    BackupCopyTasks s3_tasks;
    // Tasks including HDFS
    std::list<Protos::BackupCopyTask> hdfs_tasks;

    for (const auto & copy_task : copy_tasks)
    {
        DiskPtr source_disk = context->getDisk(copy_task.source_disk());
        DiskPtr destination_disk = context->getDisk(copy_task.destination_disk());

        if (source_disk->getType() == DiskType::Type::ByteS3 && destination_disk->getType() == DiskType::Type::ByteS3)
            s3_tasks.push_back(std::move(copy_task));
        else
            hdfs_tasks.push_back(std::move(copy_task));
    }

    auto hdfs_copy_function = [&]() {
        auto hdfs_task_iterator = hdfs_tasks.begin();
        while (hdfs_task_iterator != hdfs_tasks.end())
        {
            WorkerGroupHandle worker_group = context->getCurrentWorkerGroup();
            std::vector<CnchWorkerClientPtr> worker_clients = worker_group->getWorkerClients();
            // If the number of hdfs tasks is large, we allocate 8 tasks as batch to each worker every time
            // Else, we allocate all tasks averagely to each worker
            size_t task_number_each_client = std::min(
                context->getSettingsRef().backup_copy_tasks_per_worker.value,
                static_cast<size_t>(std::ceil(1.0 * hdfs_tasks.size() / worker_clients.size())));

            // Check if the backup task is aborted during copying data
            checkBackupTaskNotAborted(backup_task->getId(), context);

            CopyTaskCheckers checkers;
            checkers.reserve(worker_clients.size());
            for (const auto & worker_client : worker_clients)
            {
                BackupCopyTasks tasks_for_client;
                size_t current_task_number = 0;
                while (current_task_number < task_number_each_client && hdfs_task_iterator != hdfs_tasks.end())
                {
                    tasks_for_client.push_back(std::move(*hdfs_task_iterator));
                    hdfs_tasks.erase(hdfs_task_iterator++);
                    current_task_number++;
                }

                // MAKE SURE this method is asynchronous
                ExceptionHandlerPtr handler = std::make_shared<ExceptionHandler>();
                brpc::CallId call_id = worker_client->sendBackupCopyTask(context, backup_task->getId(), tasks_for_client, handler);
                checkers.emplace_back(std::move(call_id), handler, std::move(tasks_for_client));
                LOG_INFO(
                    getLogger("BackupCopy"), "Send {} copy file tasks to worker {}", tasks_for_client.size(), worker_client->getName());
            }

            // Wait for batch tasks finish
            if (!checkers.empty())
            {
                for (auto & checker : checkers)
                {
                    brpc::Join(checker.call_id);
                    // retry if exception happens
                    if (checker.handler->hasException())
                    {
                        for (auto & copy_task : checker.tasks_for_client)
                        {
                            DiskPtr destination_disk = context->getDisk(copy_task.destination_disk());
                            if (destination_disk->fileExists(copy_task.destination_path()))
                            {
                                DiskPtr source_disk = context->getDisk(copy_task.source_disk());
                                size_t source_file_size = source_disk->getFileSize(copy_task.source_path());
                                if (destination_disk->getFileSize(copy_task.destination_path()) == source_file_size)
                                    continue;
                                else
                                    destination_disk->removeFile(copy_task.destination_path());
                            }
                            // When error occurs, the file does not exist or file size is not right, retry it
                            hdfs_tasks.push_back(std::move(copy_task));
                        }
                        // reset the iterator
                        hdfs_task_iterator = hdfs_tasks.begin();
                    }
                    else
                    {
                        hdfs_finished_files += task_number_each_client;
                        updateCopyTaskProgressIfNeeded(
                            context,
                            backup_task,
                            total_tasks_number,
                            hdfs_finished_files + s3_finished_files.load(),
                            current_progress_percent);
                    }
                }
            }
        }
        LOG_INFO(getLogger("BackupCopy"), "Complete {} copy hdfs file tasks", hdfs_tasks.size());
    };

    // To start HDFS and S3 task concurrently, we let HDFS task execute in a async thread
    std::future<void> hdfs_result;
    if (!hdfs_tasks.empty() && !s3_tasks.empty())
    {
        hdfs_result = std::async(std::launch::async, hdfs_copy_function);
    }
    else if (!hdfs_tasks.empty())
    {
        hdfs_copy_function();
    }

    // For s3-to-s3 task, we will call the native copy interface of S3, so don't worry about the bandwidth.
    if (!s3_tasks.empty())
    {
        size_t pool_size = std::min(s3_tasks.size(), 128UL);
        ThreadPool pool(pool_size);
        auto thread_group = CurrentThread::getGroup();
        LoggerPtr logger = getLogger("BackupCopy");
        LOG_INFO(logger, "Start sending {} copy file tasks to S3", s3_tasks.size());

        size_t executing_task = 0;
        for (const auto & s3_task : s3_tasks)
        {
            // To avoid copy task can't be cancelled when backup task is killed, we submit pool_size tasks at a time
            if (executing_task >= pool_size)
            {
                pool.wait();
                executing_task = 0;
                checkBackupTaskNotAborted(backup_task->getId(), context);
            }
            pool.scheduleOrThrowOnError([&, s3_task = std::move(s3_task)] {
                setThreadName("S3CopyFileThr");
                if (thread_group)
                    CurrentThread::attachToIfDetached(thread_group);

                DiskByteS3Ptr source_s3_disk = std::dynamic_pointer_cast<DiskByteS3>(context->getDisk(s3_task.source_disk()));
                DiskByteS3Ptr destination_s3_disk = std::dynamic_pointer_cast<DiskByteS3>(context->getDisk(s3_task.destination_disk()));

                // Add disk base dir
                String to_path = fs::path(destination_s3_disk->getPath()) / s3_task.destination_path();
                LOG_TRACE(logger, "Copy backup file {} to {}", s3_task.source_path(), to_path);
                size_t retry_max = 3;
                size_t sleep_ms = 100;
                for (size_t retry = 0; retry < retry_max; retry++)
                {
                    SCOPE_EXIT({ ProfileEvents::increment(ProfileEvents::S3CopyObject); });
                    try
                    {
                        source_s3_disk->copyFile(s3_task.source_path(), destination_s3_disk->getS3Bucket(), to_path);
                        break;
                    }
                    catch (...)
                    {
                        LOG_ERROR(logger, "Failed to copy s3 file from {} to {}, retry {}", s3_task.source_path(), to_path, retry);
                        if (retry + 1 >= retry_max)
                            throw;
                        else
                        {
                            sleepForMilliseconds(sleep_ms);
                            sleep_ms *= 2;
                            continue;
                        }
                    }
                }

                s3_finished_files++;
                updateCopyTaskProgressIfNeeded(
                    context, backup_task, total_tasks_number, hdfs_finished_files + s3_finished_files.load(), current_progress_percent);
            });
            executing_task++;
        }
        // Wait for s3 tasks finish
        pool.wait();
        LOG_INFO(logger, "Complete {} copy s3 file tasks", s3_tasks.size());
    }

    if (hdfs_result.valid())
        hdfs_result.wait();
}

}
