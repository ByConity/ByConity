#include <Backups/BackupStatus.h>
#include <Backups/BackupUtils.h>
#include <CloudServices/CnchServerClient.h>
#include <CloudServices/CnchServerClientPool.h>
#include <DaemonManager/DaemonFactory.h>
#include <DaemonManager/DaemonJob.h>
#include <DaemonManager/DaemonJobBackup.h>
#include <DaemonManager/registerDaemons.h>
#include <Common/Stopwatch.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BACKUP_STATUS_ERROR;
    extern const int CNCH_SERVER_NOT_FOUND;
}
namespace DaemonManager
{

// Return 1. backup id if running task, 2. empty string if not, 3. nullopt if rpc failed
static std::optional<String> getCurrentServerTask(std::unordered_map<CnchServerClientPtr, String> & server_tasks, CnchServerClientPtr & server_client)
{
    std::optional<String> current_server_task;
    if (server_tasks.find(server_client) == server_tasks.end())
    {
        current_server_task = server_client->getRunningBackupTask();
        if (current_server_task)
            server_tasks[server_client] = *current_server_task;
    }
    else
        current_server_task = server_tasks[server_client];

    return current_server_task;
}

bool DaemonJobBackup::executeImpl()
{
    try
    {
        BackupTaskModels backup_task_models = catalog->getAllBackupJobs();
        std::unordered_map<CnchServerClientPtr, String> server_tasks;
        for (auto & backup_task_model : backup_task_models)
        {
            try
            {
                BackupStatus status = getBackupStatus(backup_task_model->status());
                String backup_id = backup_task_model->id();
                switch (status)
                {
                    case BackupStatus::ACCEPTED: {
                        LOG_INFO(log, "Got accepted backup task {} successfully", backup_id);
                        // find server to execute backup task
                        auto server_client = getContext()->getCnchServerClient(backup_task_model->server_address());

                        if (!server_client)
                        {
                            BackupTaskPtr backup_task = std::make_shared<BackupTask>(backup_task_model);
                            backup_task->updateStatusAndProgress(
                                BackupStatus::FAILED, "Server " + backup_task_model->server_address() + " is not found", getContext());
                        }
                        else
                        {
                            std::optional<String> current_server_task = getCurrentServerTask(server_tasks, server_client);
                            // It means current server is not running any backup task, submit it to server
                            if (current_server_task && current_server_task->empty())
                            {
                                // Avoid keeping status ACCEPTED and being scheduled multi times
                                server_tasks[server_client] = backup_id;
                                BackupTaskPtr backup_task = std::make_shared<BackupTask>(backup_task_model);
                                backup_task->updateStatus(BackupStatus::SCHEDULING, getContext(), std::nullopt);
                                LOG_INFO(log, "Schedule backup task {} to server {}", backup_id, server_client->getHostWithPortsID());
                                server_client->submitBackupTask(backup_id, backup_task_model->serialized_ast());
                            }
                            // else, wait for next schedule
                        }
                        break;
                    }
                    case BackupStatus::SCHEDULING:
                    case BackupStatus::RUNNING:
                    case BackupStatus::RESCHEDULING: {
                        // Check server is alive and current task is actually running, otherwise reschedule it.
                        auto server_client = getContext()->getCnchServerClient(backup_task_model->server_address());

                        // if server client does not exist, obviously current task running is false
                        bool is_current_task_running = !server_client;
                        if (server_client)
                        {
                            std::optional<String> current_server_task = getCurrentServerTask(server_tasks, server_client);
                            is_current_task_running = current_server_task && current_server_task == backup_id;
                        }
                        if (!is_current_task_running)
                        {
                            // Retry 3 times in case network reason, make sure this task has failed because of server failure
                            // avoid one task running in two server concurrently
                            if (retry_connect_tasks.find(backup_id) == retry_connect_tasks.end())
                                retry_connect_tasks[backup_id] = 1;
                            else
                                retry_connect_tasks[backup_id]++;

                            LOG_WARNING(log, "Backup task {} is not alive, retry times: {}", backup_id, retry_connect_tasks[backup_id]);

                            if (retry_connect_tasks[backup_id] >= MAX_RETRY)
                            {
                                retry_connect_tasks.erase(backup_id);

                                if (backup_task_model->enable_auto_recover())
                                {
                                    // find a random server to reschedule the backup task
                                    auto client_list = getContext()->getCnchServerClientPool().getAll();
                                    for (auto & client : client_list)
                                    {
                                        if (!client)
                                            continue;
                                        std::optional<String> current_server_task = getCurrentServerTask(server_tasks, client);
                                        // It means current server is not running any backup task, submit task to this server
                                        if (current_server_task && current_server_task->empty())
                                        {
                                            server_tasks[server_client] = backup_id;
                                            String new_address = client->getRPCAddress();
                                            String expected_value = backup_task_model->SerializeAsString();
                                            backup_task_model->set_status(static_cast<UInt64>(BackupStatus::RESCHEDULING));
                                            backup_task_model->set_server_address(new_address);
                                            catalog->updateBackupJobCAS(backup_task_model, expected_value);
                                            LOG_INFO(
                                                log, "Reschedule backup task {} to server {}", backup_id, client->getHostWithPortsID());
                                            client->submitBackupTask(backup_id, backup_task_model->serialized_ast());
                                            continue;
                                        }
                                    }

                                    // No server is available now, wait for next schedule
                                    LOG_WARNING(log, "Backup task {} reschedule failed because no server is available.", backup_id);
                                }
                                else
                                {
                                    BackupTaskPtr backup_task = std::make_shared<BackupTask>(backup_task_model);
                                    backup_task->updateStatus(
                                        BackupStatus::FAILED, getContext(), "Backup task is not alive and enable_auto_recover is false.");
                                    LOG_WARNING(log, "Backup task {} is not alive and enable_auto_recover is false.", backup_id);
                                }
                            }
                        }
                        else
                        {
                            // If failed once because of network and succeed afterwards, erase the fail record
                            if (retry_connect_tasks.find(backup_id) != retry_connect_tasks.end())
                                retry_connect_tasks.erase(backup_id);
                        }
                        // Clear the trash file if exists
                        break;
                    }
                    case BackupStatus::COMPLETED:
                    case BackupStatus::FAILED:
                    case BackupStatus::ABORTED: {
                        if (finished_tasks.find(backup_id) == finished_tasks.end())
                            finished_tasks[backup_id] = Stopwatch();
                        else
                        {
                            // For completed and failed task, we remove it after TTL.
                            Stopwatch & watch = finished_tasks[backup_id];
                            if (watch.elapsedSeconds() >= MAX_TTL_TIME)
                                catalog->removeBackupJob(backup_id);
                        }
                        break;
                    }
                    default:
                        throw Exception(ErrorCodes::BACKUP_STATUS_ERROR, "Unknown backup task status {}", status);
                }
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to schedule backup task " + backup_task_model->id());
                continue;
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to schedule backup tasks");
    }
    return true;
}

void registerBackupDaemon(DaemonFactory & factory)
{
    factory.registerLocalDaemonJob<DaemonJobBackup>("BACKUP");
}

}
}
