#include <CloudServices/CnchRefreshMaterializedViewThread.h>

#include <Catalog/Catalog.h>
#include <Catalog/DataModelPartWrapper.h>
#include <CloudServices/CnchPartsHelper.h>
#include <CloudServices/selectPartsToMerge.h>
#include <CloudServices/CnchWorkerClient.h>
#include <CloudServices/CnchWorkerClientPools.h>
#include <Common/Configurations.h>
#include <Common/ProfileEvents.h>
#include <Common/TestUtils.h>
#include <Interpreters/PartMergeLog.h>
#include <Interpreters/ServerPartLog.h>
#include <Parsers/formatAST.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Storages/PartCacheManager.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <WorkerTasks/ManipulationTaskParams.h>
#include <WorkerTasks/ManipulationType.h>

#include <common/sleep.h>
#include <chrono>

namespace ProfileEvents
{
    extern const Event Manipulation;
    extern const Event ManipulationSuccess;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int UNKNOWN_TABLE;
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_SIMULTANEOUS_TASKS;
    extern const int CNCH_LOCK_ACQUIRE_FAILED;
}

MvRefreshTaskRecord::~MvRefreshTaskRecord()
{
    try
    {

    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


CnchRefreshMaterializedViewThread::CnchRefreshMaterializedViewThread(ContextPtr context_, const StorageID & id)
    : ICnchBGThread(context_->getGlobalContext(), CnchBGThreadType::CnchRefreshMaterializedView, id)
{
}

CnchRefreshMaterializedViewThread::~CnchRefreshMaterializedViewThread()
{
    shutdown_called = true;

    try
    {
        stop();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

void CnchRefreshMaterializedViewThread::preStart()
{
    LOG_DEBUG(log, "preStarting RefreshMaterializedViewThread for table {}", storage_id.getFullTableName());
}

void CnchRefreshMaterializedViewThread::clearData()
{
    LOG_DEBUG(log, "stop RefreshMaterializedViewTask for table {}", storage_id.getFullTableName());

    std::unordered_set<CnchWorkerClientPtr> workers;
    {
        std::lock_guard lock(task_records_mutex);
        for (auto & [_, task_record] : task_records)
            workers.emplace(task_record->worker);
    }

    for (const auto & worker : workers)
    {
        try
        {
            // worker->shutdownManipulationTasks(storage_id.uuid);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to update status of tasks on " + worker->getHostWithPorts().toDebugString());
        }
    }

    {
        std::lock_guard lock(task_records_mutex);
        LOG_DEBUG(log, "Remove all {} refresh tasks when shutdown.", task_records.size());
        task_records.clear();

        running_tasks = 0;
    }
}

void CnchRefreshMaterializedViewThread::runHeartbeatTask()
{
}

StorageMaterializedView & CnchRefreshMaterializedViewThread::checkAndGetMaterializedViewTable(StoragePtr & storage)
{
    if (auto * t = dynamic_cast<StorageMaterializedView *>(storage.get()))
        return *t;
    throw Exception("Table " + storage->getStorageID().getNameForLogs() + " is not StorageCnchMergeTree", ErrorCodes::LOGICAL_ERROR);
}

UInt64 CnchRefreshMaterializedViewThread::checkAndGetRefreshInterval(StorageMaterializedView & storage)
{
    auto seconds = storage.checkAndCalRefreshSeconds();
    LOG_DEBUG(log, " refresh next delay seconds {}.", seconds);
    return seconds;
}

void CnchRefreshMaterializedViewThread::runImpl()
{
    UInt64 next_delay_seconds = 10;

    try
    {
        runHeartbeatTask();

        auto istorage = getStorageFromCatalog();
        auto & storage = checkAndGetMaterializedViewTable(istorage);
        auto target_istorage = storage.getTargetTable();
        auto & target = checkAndGetCnchTable(target_istorage);
        auto storage_settings = target.getSettings();

        if (istorage->is_dropped)
        {
            LOG_DEBUG(log, "Table was dropped, wait for removing...");
            scheduled_task->scheduleAfter(10 * 1000);
            return;
        }

        {
            std::lock_guard lock(worker_pool_mutex);
            vw_name = storage_settings->cnch_vw_write;
            vm_handle = getContext()->getVirtualWarehousePool().get(vw_name);
        }

        try
        {
            int max_bg_task_num = storage_settings->max_refresh_materialized_view_task_num;
            if (running_tasks < max_bg_task_num)
            {
                startRefreshTask(istorage, storage);
            }
            else
            {
                LOG_DEBUG(log, "Too many cnch materialized view refresh tasks (current: {}, max: {})", running_tasks, max_bg_task_num);
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        next_delay_seconds = checkAndGetRefreshInterval(storage);
        scheduled_task->scheduleAfter(next_delay_seconds * 1000);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);

        scheduled_task->scheduleAfter(next_delay_seconds * 1000);
    }
}

bool CnchRefreshMaterializedViewThread::constructAndScheduleRefreshTasks(StoragePtr & istorage, StorageMaterializedView & storage)
{
    ContextMutablePtr query_context = Context::createCopy(getContext());
    auto refresh_params = storage.getAsyncRefreshParams(query_context, false);
    std::vector<String> task_ids = {};

    for (const auto & refresh_param : refresh_params)
    {
        int max_bg_task_num = query_context->getSettingsRef().max_server_refresh_materialized_view_task_num;
        if (running_tasks >= max_bg_task_num)
        {
            LOG_WARNING(log,"Too many cnch materialized view refresh tasks (current: {}, max: {})", running_tasks, max_bg_task_num);
            break;
        }
        if (refresh_param->part_relation.empty())
        {
            LOG_WARNING(log,"skip refresh task : part_relation is empty");
            continue;
        }
        for (const auto & relation : refresh_param->part_relation)
        {
            String target_partition = relation.first;
            UInt64 current_ts = query_context->getPhysicalTimestamp() / 1000;
            {
                std::unique_lock lock(task_records_mutex);
                if (partition_stats.count(target_partition)
                    && partition_stats[target_partition]->status >= MvPartitionRefreshStatus::PREPARE)
                {
                    UInt64 duration_ts = current_ts - partition_stats[target_partition]->start_ts;
                    LOG_INFO(log, "skip task : {} partition id {} is in running for {} seconds.",
                        storage.getStorageID().getFullTableName(), target_partition, duration_ts);
                    continue;
                }

                partition_stats[target_partition] = std::make_shared<MvRefreshPartitionRecord>();
                partition_stats[target_partition]->status = MvPartitionRefreshStatus::PREPARE;
                partition_stats[target_partition]->start_ts = current_ts;
            }
        }
        auto record = std::make_unique<MvRefreshTaskRecord>(*this);
        record->type = ManipulationType::MvRefresh;
        record->refresh_param = refresh_param;

        if (query_context->getSettingsRef().async_mv_refresh_offload_mode) // TODO(baiyang):refresh on worker
        {
            submitTaskRecord(istorage, storage, std::move(record));
        }
        else
        {
            task_ids.emplace_back(executeTaskLocal(istorage, storage, std::move(record), query_context));
        }
    }

    for (const auto & task_id : task_ids)
    {
        handleRefreshTaskOnFinish(task_id, 0);
    }

    return true;
}

bool CnchRefreshMaterializedViewThread::startRefreshTask(StoragePtr & istorage, StorageMaterializedView & storage)
{
    LOG_TRACE(log, "running task {}", running_tasks);

    bool result = true;

    try
    {
        result = constructAndScheduleRefreshTasks(istorage, storage);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    return result;
}

void CnchRefreshMaterializedViewThread::removeTaskImpl(const String & task_id, std::lock_guard<std::mutex> &)
{
    auto it = task_records.find(task_id);
    if (it == task_records.end())
        return;

    for (const auto & partition : it->second->refresh_param->part_relation)
    {
        auto it_partition_stats = partition_stats.find(partition.first);
        if (it_partition_stats != partition_stats.end())
            partition_stats.erase(it_partition_stats);
    }

    --running_tasks;

    task_records.erase(it);
}

String CnchRefreshMaterializedViewThread::executeTaskLocal(
    StoragePtr /*istorage*/,
    StorageMaterializedView & storage,
    TaskRecordPtr record, 
    ContextMutablePtr query_context)
{
    String task_id = toString(UUIDHelpers::generateV4());
    auto & mv_refresh_param = record->refresh_param;

    try
    {
        {
            std::lock_guard lock(task_records_mutex);
            record->local_execute_thread = ThreadFromGlobalPool([&,
                                task_id = task_id,
                                mv_refresh_param = mv_refresh_param,
                                command_context = Context::createCopy(query_context)]() {
                    command_context->setCurrentTransaction(nullptr, false);
                    command_context->setCurrentVW(nullptr);
                    command_context->setCurrentWorkerGroup(nullptr);
                    command_context->makeSessionContext();
                    command_context->makeQueryContext();
                    auto settings = query_context->getSettings();
                    command_context->setSettings(settings);
                    CurrentThread::get().pushTenantId(command_context->getSettingsRef().tenant_id);

                    auto user_password = const_cast<const Context &> (*command_context).getCnchInterserverCredentials();
                    command_context->setUser(user_password.first, user_password.second, Poco::Net::SocketAddress{});
                    command_context->setCurrentQueryId(task_id);

                    storage.refreshAsync(mv_refresh_param, command_context);
                    command_context->setCurrentTransaction(nullptr);
                });

            task_records[task_id] = std::move(record);
            for (const auto & partition : mv_refresh_param->part_relation)
            {
                partition_stats[partition.first]->status = MvPartitionRefreshStatus::RUNNING;
                partition_stats[partition.first]->task_id = task_id;
            }
            ++running_tasks;
        }

        LOG_DEBUG(log, "Submitted refresh task {} to local.", task_id);
    }
    catch (...)
    {
        {
            std::lock_guard lock(task_records_mutex);
            removeTaskImpl(task_id, lock);
        }

        throw;
    }

    return task_id;
}


String CnchRefreshMaterializedViewThread::submitTaskRecord(
    StoragePtr istorage,
    StorageMaterializedView & storage,
    TaskRecordPtr record)
{
    auto local_context = getContext();

    auto & task_record = *record;
    auto type = task_record.type;

    // Create transaction
    auto & txn_coordinator = local_context->getCnchTransactionCoordinator();
    record->transaction = txn_coordinator.createTransaction(
        CreateTransactionOption().setInitiator(CnchTransactionInitiator::MvRefresh).setPriority(CnchTransactionPriority::low));

    auto transaction = task_record.transaction;
    auto transaction_id = transaction->getTransactionID();

    /// fill task parameters
    ManipulationTaskParams params(istorage);
    params.type = type;
    params.rpc_port = local_context->getRPCPort();
    params.task_id = toString(transaction_id.toUInt64());
    params.txn_id = transaction_id.toUInt64();
    /// create query && mv_refresh_params
    auto target_istorage = storage.getTargetTable();
    auto & target = checkAndGetCnchTable(target_istorage);
    params.create_table_query = target.genCreateTableQueryForWorker("");
    params.mv_refresh_param = record->refresh_param;

    auto worker_client = getWorker(type);
    task_record.task_id = params.task_id;
    task_record.worker = worker_client;

    try
    {
        {
            std::lock_guard lock(task_records_mutex);
            task_records[params.task_id] = std::move(record);
            for (const auto & partition : params.mv_refresh_param->part_relation)
            {
                partition_stats[partition.first]->status = MvPartitionRefreshStatus::PENDING;
                partition_stats[partition.first]->task_id = params.task_id;
            }
            ++running_tasks;
        }

        ProfileEvents::increment(ProfileEvents::Manipulation, 1);
        worker_client->submitMvRefreshTask(storage, params, transaction_id);
        LOG_DEBUG(log, "Submitted refresh task to {}, {}", worker_client->getHostWithPorts().toDebugString(), params.toDebugString());
    }
    catch (...)
    {
        {
            std::lock_guard lock(task_records_mutex);
            removeTaskImpl(params.task_id, lock);
        }

        throw;
    }

    {
        std::unique_lock lock(task_records_mutex);
        for (const auto & partition : params.mv_refresh_param->part_relation)
        {
            partition_stats[partition.first]->status = MvPartitionRefreshStatus::RUNNING;
        }
    }

    return params.task_id;
}

CnchWorkerClientPtr CnchRefreshMaterializedViewThread::getWorker(ManipulationType)
{
    std::lock_guard lock(worker_pool_mutex);
    return vm_handle->getWorker();
}

void CnchRefreshMaterializedViewThread::handleRefreshTaskOnFinish(String task_id, Int64)
{
    auto local_context = getContext();
    UInt64 current_ts = local_context->getPhysicalTimestamp() / 1000;
    {
        std::lock_guard lock(task_records_mutex);
        if (task_records.find(task_id) != task_records.end())
        {
            auto & task_record = task_records[task_id];
            task_record->local_execute_thread.join();
            if (!task_record->refresh_param->part_relation.empty())
            {
                auto & partition_map = task_record->refresh_param->part_relation;
                UInt64 duration_ts = current_ts - partition_stats[partition_map.begin()->first]->start_ts;
                LOG_DEBUG(log, "handleRefreshTaskOnFinish task {} duration {}", task_id, duration_ts);
            }
            else
            {
                LOG_WARNING(log, "task {} handleRefreshTaskOnFinish failed lost partitions.", task_id);
            }
            removeTaskImpl(task_id, lock);
        }
        else
        {
            LOG_WARNING(log, "task {} handleRefreshTaskOnFinish failed lost task.", task_id);
        }
    }
}

}
