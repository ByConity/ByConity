/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <CloudServices/CnchWorkerServiceImpl.h>

#include <CloudServices/CloudMergeTreeDedupWorker.h>
#include <CloudServices/CnchCreateQueryHelper.h>
#include <CloudServices/CnchPartsHelper.h>
#include <CloudServices/CnchWorkerResource.h>
#include <CloudServices/DedupWorkerStatus.h>
#include <Common/Stopwatch.h>
#include <Common/Configurations.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/NamedSession.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/trySetVirtualWarehouse.h>
#include <Protos/DataModelHelpers.h>
#include <Protos/RPCHelpers.h>
#include <Storages/DiskCache/IDiskCache.h>
#include <Storages/MergeTree/CnchMergeTreeMutationEntry.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Storages/MutationCommands.h>
#include <Storages/StorageCloudMergeTree.h>
#include <Storages/StorageDictCloudMergeTree.h>
#include <Storages/StorageMaterializedView.h>
#include <Transaction/CnchWorkerTransaction.h>
#include <Transaction/TxnTimestamp.h>
#include <WorkerTasks/ManipulationList.h>
#include <WorkerTasks/ManipulationTask.h>
#include <WorkerTasks/ManipulationTaskParams.h>
#include <WorkerTasks/StorageMaterializedViewRefreshTask.h>

#include <condition_variable>
#include <mutex>
#include <Storages/ColumnsDescription.h>
#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <brpc/stream.h>
#include <Common/Configurations.h>
#include <Common/Exception.h>
#include <IO/copyData.h>
#include <CloudServices/CnchDedupHelper.h>
#include <CloudServices/ManifestCache.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h>
#include <Core/SettingsEnums.h>
#include <MergeTreeCommon/GlobalDataManager.h>
#include <Backups/BackupUtils.h>

#if USE_RDKAFKA
#    include <Storages/Kafka/KafkaTaskCommand.h>
#    include <Storages/Kafka/StorageCloudKafka.h>
#endif

#if USE_MYSQL
#include <Databases/MySQL/DatabaseCloudMaterializedMySQL.h>
#include <Databases/MySQL/MaterializedMySQLCommon.h>
#endif

#include <Storages/Hive/StorageCloudHive.h>
#include <Storages/RemoteFile/IStorageCloudFile.h>
#include <Storages/Hive/StorageCloudHive.h>

namespace ProfileEvents
{
extern const Event PreloadExecTotalOps;
}

namespace ProfileEvents
{
    extern const Event QueryCreateTablesMicroseconds;
    extern const Event QuerySendResourcesMicroseconds;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int BRPC_EXCEPTION;
    extern const int TOO_MANY_SIMULTANEOUS_TASKS;
    extern const int PREALLOCATE_TOPOLOGY_ERROR;
    extern const int PREALLOCATE_QUERY_INTENT_NOT_FOUND;
    extern const int SESSION_NOT_FOUND;
    extern const int ABORTED;
}

CnchWorkerServiceImpl::CnchWorkerServiceImpl(ContextMutablePtr context_)
    : WithMutableContext(context_->getGlobalContext())
    , log(getLogger("CnchWorkerService"))
    , thread_pool(getNumberOfPhysicalCPUCores() * 4, getNumberOfPhysicalCPUCores() * 2, getNumberOfPhysicalCPUCores() * 8)
{
}

CnchWorkerServiceImpl::~CnchWorkerServiceImpl()
{
    try
    {
        LOG_TRACE(log, "Waiting local thread pool finishing");
        thread_pool.wait();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

#define THREADPOOL_SCHEDULE(func) \
    try \
    { \
        thread_pool.scheduleOrThrowOnError(std::move(func)); \
    } \
    catch (...) \
    { \
        tryLogCurrentException(log, __PRETTY_FUNCTION__); \
        RPCHelpers::handleException(response->mutable_exception()); \
        done->Run(); \
    }

#define SUBMIT_THREADPOOL(...) \
    auto _func = [=, this] { \
        brpc::ClosureGuard done_guard(done); \
        try \
        { \
            __VA_ARGS__; \
        } \
        catch (...) \
        { \
            tryLogCurrentException(log, __PRETTY_FUNCTION__); \
            RPCHelpers::handleException(response->mutable_exception()); \
        } \
    }; \
    Stopwatch watch; \
    THREADPOOL_SCHEDULE(_func); \
    UInt64 milliseconds = watch.elapsedMilliseconds(); \
    if (milliseconds > 100) LOG_DEBUG(log, "CnchWorkerService rpc request threadpool schedule cost : {} ", milliseconds);


void CnchWorkerServiceImpl::executeSimpleQuery(
    google::protobuf::RpcController * ,
    const Protos::ExecuteSimpleQueryReq * ,
    Protos::ExecuteSimpleQueryResp * response,
    google::protobuf::Closure * done)
{
    /// example
    SUBMIT_THREADPOOL({
        /// TODO:
    })

    /// Former impl
    /*
    brpc::ClosureGuard done_guard(done);

    try
    {
        /// TODO:
    }

    */

}

void CnchWorkerServiceImpl::submitMVRefreshTask(
    google::protobuf::RpcController * cntl,
    const Protos::SubmitMVRefreshTaskReq * request,
    Protos::SubmitMVRefreshTaskResp * response,
    google::protobuf::Closure * done)
{
    SUBMIT_THREADPOOL({
        LOG_DEBUG(log, "Received request {}", request->DebugString());

        if (request->task_id().empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Require non-empty task_id");

        auto txn_id = TxnTimestamp(request->txn_id());
        auto rpc_context = RPCHelpers::createSessionContextForRPC(getContext(), *cntl);
        rpc_context->setCurrentQueryId(request->task_id());
        rpc_context->getClientInfo().rpc_port = request->rpc_port();
        auto server_client = rpc_context->getCnchServerClient(rpc_context->getClientInfo().current_address.host().toString(), request->rpc_port());
        rpc_context->setCurrentTransaction(std::make_shared<CnchWorkerTransaction>(rpc_context, server_client));

        rpc_context->initCnchServerResource(txn_id);
        rpc_context->setSetting("prefer_localhost_replica", false);
        rpc_context->setSetting("prefer_cnch_catalog", true);

        const auto & settings = getContext()->getSettingsRef();
        UInt64 max_running_task = settings.max_threads * getContext()->getRootConfig().max_ratio_of_cnch_tasks_to_threads;
        if (getContext()->getManipulationList().size() > max_running_task)
            throw Exception(ErrorCodes::TOO_MANY_SIMULTANEOUS_TASKS, "Too many simultaneous tasks. Maximum: {}", max_running_task);

        StoragePtr storage = createStorageFromQuery(request->create_table_query(), rpc_context);
        auto * data = dynamic_cast<StorageCloudMergeTree *>(storage.get());
        if (!data)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table {} is not StorageCloudMergeTree", storage->getStorageID().getNameForLogs());

        trySetVirtualWarehouseAndWorkerGroup(data->getSettings()->cnch_vw_default.value, rpc_context);

        auto params = ManipulationTaskParams(storage);
        params.type = ManipulationType::MvRefresh;
        params.task_id = request->task_id();
        params.rpc_port = static_cast<UInt16>(request->rpc_port());
        params.txn_id = txn_id;
        params.mv_refresh_param = std::make_shared <AsyncRefreshParam>();
        params.mv_refresh_param->drop_partition_query = request->drop_partition_query();
        params.mv_refresh_param->insert_select_query = request->insert_select_query();

        auto remote_address
            = addBracketsIfIpv6(rpc_context->getClientInfo().current_address.host().toString()) + ':' + toString(params.rpc_port);

        LOG_DEBUG(log, "Received mv refresh task from {} :{}", remote_address, params.toDebugString());

        auto mv_storage_id = RPCHelpers::createStorageID(request->mv_storage_id());

        auto task = std::make_shared<StorageMaterializedViewRefreshTask>(*data, params, mv_storage_id, rpc_context, server_client);
        auto event = std::make_shared<Poco::Event>();

        ThreadFromGlobalPool([task = std::move(task),  event]() mutable {
            try
            {
                task->setManipulationEntry();
                event->set();
                task->execute();
            }
            catch (...)
            {
                event->set();
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }).detach();

        /// Waiting for manipulation task to be added to the ManipulationList
        event->wait(3 * 1000);
    })
}


void CnchWorkerServiceImpl::submitManipulationTask(
    google::protobuf::RpcController * cntl,
    const Protos::SubmitManipulationTaskReq * request,
    Protos::SubmitManipulationTaskResp * response,
    google::protobuf::Closure * done)
{
    SUBMIT_THREADPOOL({
        if (request->task_id().empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Require non-empty task_id");

        auto txn_id = TxnTimestamp(request->txn_id());
        auto rpc_context = RPCHelpers::createSessionContextForRPC(getContext(), *cntl);
        rpc_context->setCurrentQueryId(request->task_id());
        rpc_context->getClientInfo().rpc_port = request->rpc_port();
        auto server_client = rpc_context->getCnchServerClient(rpc_context->getClientInfo().current_address.host().toString(), request->rpc_port());
        rpc_context->setCurrentTransaction(std::make_shared<CnchWorkerTransaction>(rpc_context, txn_id, server_client));

        const auto & settings = getContext()->getSettingsRef();
        UInt64 max_running_task = settings.max_threads * getContext()->getRootConfig().max_ratio_of_cnch_tasks_to_threads;
        if (getContext()->getManipulationList().size() > max_running_task)
            throw Exception(ErrorCodes::TOO_MANY_SIMULTANEOUS_TASKS, "Too many simultaneous tasks. Maximum: {}", max_running_task);

        StoragePtr storage = createStorageFromQuery(request->create_table_query(), rpc_context);
        auto * data = dynamic_cast<StorageCloudMergeTree *>(storage.get());
        if (!data)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table {} is not CloudMergeTree", storage->getStorageID().getNameForLogs());
        if (request->has_dynamic_object_column_schema())
        {
            LOG_TRACE(
                log,
                "Received table:{}.{} with dynamic object column schema:{}.",
                data->getCnchDatabase(),
                data->getCnchTable(),
                request->dynamic_object_column_schema());
            data->resetObjectColumns(ColumnsDescription::parse(request->dynamic_object_column_schema()));
        }

        auto params = ManipulationTaskParams(storage);
        params.type = static_cast<ManipulationType>(request->type());
        params.task_id = request->task_id();
        params.rpc_port = static_cast<UInt16>(request->rpc_port());
        params.txn_id = txn_id;
        params.columns_commit_time = request->columns_commit_time();
        params.is_bucket_table = request->is_bucket_table();
        params.parts_preload_level = request->parts_preload_level();

        if (params.type == ManipulationType::Mutate || params.type == ManipulationType::Clustering)
        {
            params.mutation_commit_time = request->mutation_commit_time();
            auto read_buf = ReadBufferFromString(request->mutate_commands());
            params.mutation_commands = std::make_shared<MutationCommands>();
            params.mutation_commands->readText(read_buf);

            /// TODO: (zuochuang.zema) send mutation_entry but not mutation_commands in RPC.
            /// Data part need to load the mutation entry to do the column conversion.
            CnchMergeTreeMutationEntry mutation_entry;
            mutation_entry.commands = *params.mutation_commands;
            mutation_entry.txn_id = request->txn_id();
            mutation_entry.commit_time = params.mutation_commit_time;
            mutation_entry.columns_commit_time = params.columns_commit_time;
            data->addMutationEntry(mutation_entry);

            /// Always use FAST_DELETE mode for CnchMergeTree.
            for (auto & command : *params.mutation_commands)
            {
                if (command.type == MutationCommand::DELETE)
                    command.type = MutationCommand::FAST_DELETE;
            }

            rpc_context->initCnchServerResource(txn_id);
            rpc_context->setSetting("prefer_localhost_replica", false);
            rpc_context->setSetting("prefer_cnch_catalog", true);
            rpc_context->setSetting("max_execution_time", 3600);
            trySetVirtualWarehouseAndWorkerGroup(data->getSettings()->cnch_vw_default.value, rpc_context);
        }

        auto remote_address
            = addBracketsIfIpv6(rpc_context->getClientInfo().current_address.host().toString()) + ':' + toString(params.rpc_port);
        auto all_parts = createPartVectorFromModelsForSend<IMutableMergeTreeDataPartPtr>(*data, request->source_parts());
        params.assignParts(all_parts, [&]() {return rpc_context->getTimestamp(); });

        LOG_DEBUG(log, "Received manipulation from {} :{}", remote_address, params.toDebugString());

        auto task = data->manipulate(params, rpc_context);
        auto event = std::make_shared<Poco::Event>();

        // TODO(shiyuze): limit running tasks by soft limit (using canEnqueueBackgroundTask)
        ThreadFromGlobalPool([task = std::move(task), all_parts = std::move(all_parts), event]() mutable {
            try
            {
                task->setManipulationEntry();
                event->set();
                executeManipulationTask(std::move(task), std::move(all_parts));
            }
            catch (...)
            {
                event->set();
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }).detach();

        /// Waiting for manipulation task to be added to the ManipulationList
        event->wait(3 * 1000);
    })
}

void CnchWorkerServiceImpl::shutdownManipulationTasks(
    google::protobuf::RpcController *,
    const Protos::ShutdownManipulationTasksReq * request,
    Protos::ShutdownManipulationTasksResp * response,
    google::protobuf::Closure * done)
{
    SUBMIT_THREADPOOL({
        auto uuid = RPCHelpers::createUUID(request->table_uuid());
        std::unordered_set<String> task_ids(request->task_ids().begin(), request->task_ids().end());

        getContext()->getManipulationList().apply([&](std::list<ManipulationListElement> & container) {
            for (auto & e : container)
            {
                if (uuid != e.storage_id.uuid)
                    continue;

                if (!task_ids.empty() && !task_ids.contains(e.task_id))
                    continue;

                e.is_cancelled.store(true, std::memory_order_relaxed);
            }
        });
    })
}

void CnchWorkerServiceImpl::touchManipulationTasks(
    google::protobuf::RpcController *,
    const Protos::TouchManipulationTasksReq * request,
    Protos::TouchManipulationTasksResp * response,
    google::protobuf::Closure * done)
{
    SUBMIT_THREADPOOL({
        auto uuid = RPCHelpers::createUUID(request->table_uuid());
        std::unordered_set<String> request_tasks(request->tasks_id().begin(), request->tasks_id().end());

        getContext()->getManipulationList().apply([&](std::list<ManipulationListElement> & container) {
            auto now = time(nullptr);
            for (auto & e : container)
            {
                if (uuid != e.storage_id.uuid)
                    continue;

                if (e.type == ManipulationType::MvRefresh)
                    continue;

                if (request_tasks.count(e.task_id))
                {
                    e.last_touch_time.store(now, std::memory_order_relaxed);
                }
                else
                {
                    /// TBD: cancel tasks?
                }

                response->add_tasks_id(e.task_id);
            }
        });
    })
}

void CnchWorkerServiceImpl::getManipulationTasksStatus(
    google::protobuf::RpcController *,
    const Protos::GetManipulationTasksStatusReq *,
    Protos::GetManipulationTasksStatusResp * response,
    google::protobuf::Closure * done)
{
    SUBMIT_THREADPOOL({
        getContext()->getManipulationList().apply([&](std::list<ManipulationListElement> & container) {
            for (auto & e : container)
            {
                auto * task_info = response->add_tasks();

                task_info->set_task_id(e.task_id);
                task_info->set_type(UInt32(e.type));
                RPCHelpers::fillStorageID(e.storage_id, *task_info->mutable_storage_id());
                task_info->set_elapsed(e.watch.elapsedSeconds());
                task_info->set_num_parts(e.num_parts);
                for (auto & source_part_name : e.source_part_names)
                    task_info->add_source_part_names(source_part_name);
                for (auto & result_part_name : e.result_part_names)
                    task_info->add_result_part_names(result_part_name);
                task_info->set_partition_id(e.partition_id);
                task_info->set_total_size_bytes_compressed(e.total_size_bytes_compressed);
                task_info->set_total_size_marks(e.total_size_marks);
                task_info->set_total_rows_count(e.total_rows_count);
                task_info->set_progress(e.progress.load(std::memory_order_relaxed));
                task_info->set_bytes_read_uncompressed(e.bytes_read_uncompressed.load(std::memory_order_relaxed));
                task_info->set_bytes_written_uncompressed(e.bytes_written_uncompressed.load(std::memory_order_relaxed));
                task_info->set_rows_read(e.rows_read.load(std::memory_order_relaxed));
                task_info->set_rows_written(e.rows_written.load(std::memory_order_relaxed));
                task_info->set_columns_written(e.columns_written.load(std::memory_order_relaxed));
                task_info->set_memory_usage(e.getMemoryTracker().get());
                task_info->set_thread_id(e.thread_id);
            }
        });
    })
}

void CnchWorkerServiceImpl::sendCreateQuery(
    google::protobuf::RpcController * cntl,
    const Protos::SendCreateQueryReq * request,
    Protos::SendCreateQueryResp * response,
    google::protobuf::Closure * done)
{
    SUBMIT_THREADPOOL({
        LOG_TRACE(log, "Receiving create queries for Session: {}", request->txn_id());
        /// set client_info.
        auto rpc_context = RPCHelpers::createSessionContextForRPC(getContext(), *cntl);

        auto session = rpc_context->acquireNamedCnchSession(request->txn_id(), request->timeout(), false);
        auto & query_context = session->context;
        // session->context->setTemporaryTransaction(request->txn_id(), request->primary_txn_id());

        /// executeQuery may change the settings, so we copy a new context.
        auto create_context = Context::createCopy(query_context);
        auto worker_resource = query_context->getCnchWorkerResource();
        for (const auto & create_query : request->create_queries())
        {
            /// store cloud tables in cnch_session_resource.
            worker_resource->executeCreateQuery(create_context, create_query, true);
        }

        /// when create query with cnch table (support mv with multiple tables like subqery or join)
        if (!request->cnch_table_create_queries().empty())
            query_context->initCnchServerResource(request->txn_id());
        for (const auto & cnch_table_create_query : request->cnch_table_create_queries())
        {
            StoragePtr storage = createStorageFromQuery(cnch_table_create_query, query_context);
            if (auto data = dynamic_cast<MergeTreeMetaBase *>(storage.get()))
            {
                trySetVirtualWarehouseAndWorkerGroup(data->getSettings()->cnch_vw_default.value, query_context);
                worker_resource->cnch_tables.emplace(storage->getStorageID().database_name, storage->getStorageID().table_name);
            }
        }

        LOG_TRACE(log, "Successfully create {} queries for Session: {}", request->create_queries_size(), request->txn_id());
    })
}

void CnchWorkerServiceImpl::checkDataParts(
    google::protobuf::RpcController * cntl,
    const Protos::CheckDataPartsReq * request,
    Protos::CheckDataPartsResp * response,
    google::protobuf::Closure * done)
{
    SUBMIT_THREADPOOL({
        /// set client_info
        auto rpc_context = RPCHelpers::createSessionContextForRPC(getContext(), *cntl);

        auto session = rpc_context->acquireNamedCnchSession(request->txn_id(), {}, false);
        auto & query_context = session->context;

        auto worker_resource = query_context->getCnchWorkerResource();
        worker_resource->executeCreateQuery(query_context, request->create_query());

        auto storage = DatabaseCatalog::instance().getTable({request->database_name(), request->table_name()}, query_context);
        auto & cloud_merge_tree = dynamic_cast<StorageCloudMergeTree &>(*storage);

        LOG_DEBUG(log, "Receiving parts for table {} to check.", cloud_merge_tree.getStorageID().getNameForLogs());

        auto data_parts = createPartVectorFromModelsForSend<MutableMergeTreeDataPartCNCHPtr>(cloud_merge_tree, request->parts());

        ThreadPool check_pool(std::min<UInt64>(query_context->getSettingsRef().parts_preallocate_pool_size, data_parts.size()));
        std::mutex mutex;

        for (auto & part : data_parts)
        {
            check_pool.scheduleOrThrowOnError([&, part]() {
                bool is_passed = false;
                String message;
                try
                {
                    // TODO: checkDataParts(part);
                    dynamic_cast<MergeTreeDataPartCNCH*>(part.get())->loadFromFileSystem();
                    is_passed = true;
                    message.clear();
                }
                catch (const Exception & e)
                {
                    is_passed = false;
                    message = e.message();
                }
                std::lock_guard lock(mutex);
                *response->mutable_part_path()->Add() = part->name;
                *response->mutable_is_passed()->Add() = is_passed;
                *response->mutable_message()->Add() = message;
            });
        }

        check_pool.wait();

        session->release();
        LOG_DEBUG(log, "Send check results back for {} parts.", data_parts.size());
    })
}

void CnchWorkerServiceImpl::preloadDataParts(
    [[maybe_unused]] google::protobuf::RpcController * cntl,
    const Protos::PreloadDataPartsReq * request,
    Protos::PreloadDataPartsResp * response,
    google::protobuf::Closure * done)
{
    SUBMIT_THREADPOOL({
        Stopwatch watch;
        auto rpc_context = RPCHelpers::createSessionContextForRPC(getContext(), *cntl);
        StoragePtr storage = createStorageFromQuery(request->create_table_query(), rpc_context);
        auto & cloud_merge_tree = dynamic_cast<StorageCloudMergeTree &>(*storage);
        auto data_parts = createPartVectorFromModelsForSend<MutableMergeTreeDataPartCNCHPtr>(cloud_merge_tree, request->parts());

        LOG_TRACE(
            log,
            "Receiving preload parts task level = {}, sync = {}, current table preload setting: parts_preload_level = {}, "
            "enable_preload_parts = {}, enable_parts_sync_preload = {}, enable_local_disk_cache = {}, enable_nexus_fs = {}",
            request->preload_level(),
            request->sync(),
            cloud_merge_tree.getSettings()->parts_preload_level.value,
            cloud_merge_tree.getSettings()->enable_preload_parts.value,
            cloud_merge_tree.getSettings()->enable_parts_sync_preload,
            cloud_merge_tree.getSettings()->enable_local_disk_cache,
            cloud_merge_tree.getSettings()->enable_nexus_fs);

        if (!request->preload_level()
            || (!cloud_merge_tree.getSettings()->parts_preload_level && !cloud_merge_tree.getSettings()->enable_preload_parts))
            return;

        auto preload_level = request->preload_level();
        auto submit_ts = request->submit_ts();
        auto read_injection = request->read_injection();

        if (request->sync())
        {
            auto & settings = getContext()->getSettingsRef();
            auto pool = std::make_unique<ThreadPool>(std::min(data_parts.size(), settings.cnch_parallel_preloading.value));
            for (const auto & part : data_parts)
            {
                pool->scheduleOrThrowOnError([part, preload_level, submit_ts, read_injection, storage] {
                    part->remote_fs_read_failed_injection = read_injection;
                    part->disk_cache_mode = DiskCacheMode::SKIP_DISK_CACHE;// avoid getCheckum & getIndex re-cache
                    part->preload(preload_level, submit_ts);
                });
            }
            pool->wait();
            LOG_DEBUG(
                log,
                "Finish preload tasks in {} ms, level: {}, sync: {}, size: {}",
                watch.elapsedMilliseconds(),
                preload_level,
                sync,
                data_parts.size());
        }
        else
        {
            ThreadPool * preload_thread_pool = &(IDiskCache::getPreloadPool());
            for (const auto & part : data_parts)
            {
                preload_thread_pool->scheduleOrThrowOnError([part, preload_level, submit_ts, read_injection, storage] {
                    part->remote_fs_read_failed_injection = read_injection;
                    part->disk_cache_mode = DiskCacheMode::SKIP_DISK_CACHE;// avoid getCheckum & getIndex re-cache
                    part->preload(preload_level, submit_ts);
                });
            }
        }
    })
}

void CnchWorkerServiceImpl::dropPartDiskCache(
    [[maybe_unused]] google::protobuf::RpcController * cntl,
    const Protos::DropPartDiskCacheReq * request,
    Protos::DropPartDiskCacheResp * response,
    google::protobuf::Closure * done)
{
    SUBMIT_THREADPOOL({
        auto rpc_context = RPCHelpers::createSessionContextForRPC(getContext(), *cntl);
        StoragePtr storage = createStorageFromQuery(request->create_table_query(), rpc_context);
        auto & cloud_merge_tree = dynamic_cast<StorageCloudMergeTree &>(*storage);
        auto data_parts = createPartVectorFromModelsForSend<MutableMergeTreeDataPartCNCHPtr>(cloud_merge_tree, request->parts());

        std::unique_ptr<ThreadPool> pool;
        ThreadPool * pool_ptr;
        if (request->sync())
        {
            pool = std::make_unique<ThreadPool>(std::min(data_parts.size(), 16UL));
            pool_ptr = pool.get();
        }
        else
            pool_ptr = &(IDiskCache::getThreadPool());


        for (const auto & part : data_parts)
        {
            part->dropDiskCache(*pool_ptr, request->drop_vw_disk_cache());
            // Just need one part drop all disk cache if drop_vw_disk_cache = true
            if (request->drop_vw_disk_cache())
            {
                LOG_WARNING(log, "You now drop all vw {} cache!", cloud_merge_tree.getCnchStorageID().server_vw_name);
                break;
            }
        }

        if (pool)
            pool->wait();
    })
}

void CnchWorkerServiceImpl::dropManifestDiskCache(
    google::protobuf::RpcController * cntl,
    const Protos::DropManifestDiskCacheReq * request,
    Protos::DropManifestDiskCacheResp * response,
    google::protobuf::Closure * done)
{
    SUBMIT_THREADPOOL({
        auto rpc_context = RPCHelpers::createSessionContextForRPC(getContext(), *cntl);
        auto table_uuid = RPCHelpers::createUUID(request->storage_id());

        UInt64 version = request->version();

        auto storage_data_manager = rpc_context->getGlobalDataManager()->getStorageDataManager(table_uuid);
        if (!storage_data_manager)
            return;

        std::unique_ptr<ThreadPool> pool;
        ThreadPool * pool_ptr;
        if (request->sync())
        {
            // adjust pool size according to version number
            pool = std::make_unique<ThreadPool>(version>0 ? 1 : 4);
            pool_ptr = pool.get();
        }
        else
            pool_ptr = &(IDiskCache::getThreadPool());

        storage_data_manager->dropTableVersion(*pool_ptr, version);

        if (pool)
            pool->wait();
    })
}

void CnchWorkerServiceImpl::sendOffloading(
    google::protobuf::RpcController *,
    const Protos::SendOffloadingReq *,
    Protos::SendOffloadingResp * response,
    google::protobuf::Closure * done)
{
    SUBMIT_THREADPOOL({
        /// TODO
    })
}

void CnchWorkerServiceImpl::sendResources(
    google::protobuf::RpcController * cntl,
    const Protos::SendResourcesReq * request,
    Protos::SendResourcesResp * response,
    google::protobuf::Closure * done)
{
    SUBMIT_THREADPOOL({
        LOG_TRACE(log, "Receiving resources for Session: {}", request->txn_id());
        Stopwatch watch;
        auto rpc_context = RPCHelpers::createSessionContextForRPC(getContext(), *cntl);

        auto session = rpc_context->acquireNamedCnchSession(request->txn_id(), request->timeout(), false);
        auto query_context = session->context;
        query_context->setTemporaryTransaction(request->txn_id(), request->primary_txn_id());
        if (request->has_session_timezone())
            query_context->setSetting("session_timezone", request->session_timezone());

        CurrentThread::QueryScope query_scope(query_context);
        auto worker_resource = query_context->getCnchWorkerResource();

        /// store cloud tables in cnch_session_resource.
        {
            Stopwatch create_timer;
            /// create a copy of session_context to avoid modify settings in SessionResource
            auto context_for_create = Context::createCopy(query_context);
            for (int i = 0; i < request->create_queries_size(); i++)
            {
                auto create_query = request->create_queries().at(i);
                auto object_columns = request->dynamic_object_column_schema().at(i);

                worker_resource->executeCreateQuery(context_for_create, create_query, false, ColumnsDescription::parse(object_columns));
            }
            for (int i = 0; i < request->cacheable_create_queries_size(); i++)
            {
                auto & item = request->cacheable_create_queries().at(i);
                ColumnsDescription object_columns;
                if (item.has_dynamic_object_column_schema())
                    object_columns = ColumnsDescription::parse(item.dynamic_object_column_schema());
                worker_resource->executeCacheableCreateQuery(
                    context_for_create,
                    RPCHelpers::createStorageID(item.storage_id()),
                    item.definition(),
                    item.local_table_name(),
                    static_cast<WorkerEngineType>(item.local_engine_type()),
                    item.local_underlying_dictionary_tables(),
                    object_columns);
            }
            create_timer.stop();
            LOG_INFO(log, "Prepared {} tables for session {} in {} us", request->create_queries_size() + request->cacheable_create_queries_size(), request->txn_id(), create_timer.elapsedMicroseconds());
            ProfileEvents::increment(ProfileEvents::QueryCreateTablesMicroseconds, create_timer.elapsedMicroseconds());
        }

        for (const auto & data : request->data_parts())
        {
            auto storage = DatabaseCatalog::instance().getTable({data.database(), data.table()}, query_context);

            if (auto * cloud_merge_tree = dynamic_cast<StorageCloudMergeTree *>(storage.get()))
            {
                if (data.has_table_version())
                {
                    WGWorkerInfoPtr worker_info = RPCHelpers::createWorkerInfo(request->worker_info());
                    UInt64 version = data.table_version();
                    cloud_merge_tree->setDataDescription(std::move(worker_info), version);
                    LOG_DEBUG(log, "Received table {} with data version {}",
                        cloud_merge_tree->getStorageID().getNameForLogs(),
                        version);
                }
                else if (!data.server_parts().empty())
                {
                    MergeTreeMutableDataPartsVector server_parts;
                    if (cloud_merge_tree->getInMemoryMetadataPtr()->hasUniqueKey())
                        server_parts = createBasePartAndDeleteBitmapFromModelsForSend<IMergeTreeMutableDataPartPtr>(
                            *cloud_merge_tree, data.server_parts(), data.server_part_bitmaps());
                    else
                        server_parts
                            = createPartVectorFromModelsForSend<IMergeTreeMutableDataPartPtr>(*cloud_merge_tree, data.server_parts());

                    auto server_parts_size = server_parts.size();

                    if (request->has_disk_cache_mode())
                    {
                        auto disk_cache_mode = SettingFieldDiskCacheModeTraits::fromString(request->disk_cache_mode());
                        if (disk_cache_mode != DiskCacheMode::AUTO)
                        {
                            for (auto & part : server_parts)
                                part->disk_cache_mode = disk_cache_mode;
                        }
                    }

                    /// `loadDataParts` is an expensive action as it may involve remote read.
                    /// The worker rpc thread pool may be blocked when there are many `sendResources` requests.
                    /// Here we just pass the server_parts to storage. And it will do `loadDataParts` later (before reading).
                    /// One exception is StorageDictCloudMergeTree as it use a different read logic rather than StorageCloudMergeTree::read.
                    bool is_dict = false;
                    if (auto * cloud_dict = dynamic_cast<StorageDictCloudMergeTree *>(storage.get()))
                    {
                        cloud_dict->loadDataParts(server_parts);
                        is_dict = true;
                    }
                    else
                        cloud_merge_tree->receiveDataParts(std::move(server_parts));

                    LOG_DEBUG(
                        log,
                        "Received {} parts for table {}(txn_id: {}), disk_cache_mode {}, is_dict: {}",
                        server_parts_size, cloud_merge_tree->getStorageID().getNameForLogs(),
                        request->txn_id(), request->disk_cache_mode(), is_dict);
                }

                if (!data.virtual_parts().empty())
                {
                    MergeTreeMutableDataPartsVector virtual_parts;
                    if (cloud_merge_tree->getInMemoryMetadataPtr()->hasUniqueKey())
                        virtual_parts = createBasePartAndDeleteBitmapFromModelsForSend<IMergeTreeMutableDataPartPtr>(
                            *cloud_merge_tree, data.virtual_parts(), data.virtual_part_bitmaps());
                    else
                        virtual_parts
                            = createPartVectorFromModelsForSend<IMergeTreeMutableDataPartPtr>(*cloud_merge_tree, data.virtual_parts());

                    auto virtual_parts_size = virtual_parts.size();

                    if (request->has_disk_cache_mode())
                    {
                        auto disk_cache_mode = SettingFieldDiskCacheModeTraits::fromString(request->disk_cache_mode());
                        if (disk_cache_mode != DiskCacheMode::AUTO)
                        {
                            for (auto & part : virtual_parts)
                                part->disk_cache_mode = disk_cache_mode;
                        }
                    }

                    bool is_dict = false;
                    if (auto * cloud_dict = dynamic_cast<StorageDictCloudMergeTree *>(storage.get()))
                    {
                        cloud_dict->loadDataParts(virtual_parts);
                        is_dict = true;
                    }
                    else
                        cloud_merge_tree->receiveVirtualDataParts(std::move(virtual_parts));

                    LOG_DEBUG(
                        log,
                        "Received {} virtual parts for table {}(txn_id: {}), disk_cache_mode {}, is_dict: {}",
                        virtual_parts_size, cloud_merge_tree->getStorageID().getNameForLogs(),
                        request->txn_id(), request->disk_cache_mode(), is_dict);
                }

                std::set<Int64> required_bucket_numbers;
                for (const auto & bucket_number : data.bucket_numbers())
                    required_bucket_numbers.insert(bucket_number);

                cloud_merge_tree->setRequiredBucketNumbers(required_bucket_numbers);

                for (const auto & mutation_str : data.cnch_mutation_entries())
                {
                    auto mutation_entry = CnchMergeTreeMutationEntry::parse(mutation_str);
                    cloud_merge_tree->addMutationEntry(mutation_entry);
                }
            }
            else if (auto * hive_table = dynamic_cast<StorageCloudHive *>(storage.get()))
            {
                auto settings = hive_table->getSettings();
                auto files = RPCHelpers::deserialize(data.hive_parts(), query_context, storage->getInMemoryMetadataPtr(), *settings);
                hive_table->loadHiveFiles(files);
            }
            else if (auto * cloud_file_table = dynamic_cast<IStorageCloudFile *>(storage.get()))
            {
                auto data_parts = createCnchFileDataParts(getContext(), data.file_parts());
                cloud_file_table->loadDataParts(data_parts);

                LOG_DEBUG(
                    log,
                    "Received and loaded {}  cloud file parts for table {}",
                    data_parts.size(),
                    cloud_file_table->getStorageID().getNameForLogs());
            }
            else
                throw Exception("Unknown table engine: " + storage->getName(), ErrorCodes::UNKNOWN_TABLE);
        }

        watch.stop();
        LOG_INFO(log, "Received all resources for session {} in {} us.", request->txn_id(), watch.elapsedMicroseconds());
        ProfileEvents::increment(ProfileEvents::QuerySendResourcesMicroseconds, watch.elapsedMicroseconds());
    })
}

void CnchWorkerServiceImpl::removeWorkerResource(
    google::protobuf::RpcController *,
    const Protos::RemoveWorkerResourceReq * request,
    Protos::RemoveWorkerResourceResp * response,
    google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    try
    {
        auto session = getContext()->acquireNamedCnchSession(request->txn_id(), {}, true, true);
        if (!session)
        {
            // Return success if can't find session.
            return;
        }
        /// remove resource in worker
        session->release();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

#if defined(__clang__)
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wunused-parameter"
#else
#    pragma GCC diagnostic push
#    pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

void CnchWorkerServiceImpl::GetPreallocatedStatus(
    google::protobuf::RpcController *,
    const Protos::GetPreallocatedStatusReq * request,
    Protos::GetPreallocatedStatusResp * response,
    google::protobuf::Closure * done)
{
}

void CnchWorkerServiceImpl::SetQueryIntent(
    google::protobuf::RpcController *,
    const Protos::SetQueryIntentReq * request,
    Protos::SetQueryIntentResp * response,
    google::protobuf::Closure * done)
{
}

void CnchWorkerServiceImpl::SubmitSyncTask(
    google::protobuf::RpcController *,
    const Protos::SubmitSyncTaskReq * request,
    Protos::SubmitSyncTaskResp * response,
    google::protobuf::Closure * done)
{
}

void CnchWorkerServiceImpl::ResetQueryIntent(
    google::protobuf::RpcController *,
    const Protos::ResetQueryIntentReq * request,
    Protos::ResetQueryIntentResp * response,
    google::protobuf::Closure * done)
{
}

void CnchWorkerServiceImpl::SubmitScaleTask(
    google::protobuf::RpcController *,
    const Protos::SubmitScaleTaskReq * request,
    Protos::SubmitScaleTaskResp * response,
    google::protobuf::Closure * done)
{
}

void CnchWorkerServiceImpl::ClearPreallocatedDataParts(
    google::protobuf::RpcController *,
    const Protos::ClearPreallocatedDataPartsReq * request,
    Protos::ClearPreallocatedDataPartsResp * response,
    google::protobuf::Closure * done)
{
}

void CnchWorkerServiceImpl::createDedupWorker(
    google::protobuf::RpcController * cntl,
    const Protos::CreateDedupWorkerReq * request,
    Protos::CreateDedupWorkerResp * response,
    google::protobuf::Closure * done)
{
    SUBMIT_THREADPOOL({
        auto storage_id = RPCHelpers::createStorageID(request->table());
        const auto & query = request->create_table_query();
        auto host_ports = RPCHelpers::createHostWithPorts(request->host_ports());
        size_t deduper_index = request->deduper_index();

        auto rpc_context = RPCHelpers::createSessionContextForRPC(getContext(), *cntl);
        rpc_context->setSessionContext(rpc_context);
        rpc_context->setCurrentQueryId(toString(UUIDHelpers::generateV4()));

        /// XXX: We modify asynchronous creation to synchronous one because possible assignHighPriorityDedupPartition/assignRepairGran rpc relies on it
        try
        {
            // CurrentThread::attachQueryContext(*context);
            rpc_context->setSetting("default_database_engine", String("Memory"));
            executeQuery(query, rpc_context, true);
            LOG_INFO(log, "Created local table {}", storage_id.getFullTableName());

            auto storage = DatabaseCatalog::instance().getTable(storage_id, rpc_context);
            auto * cloud_table = dynamic_cast<StorageCloudMergeTree *>(storage.get());
            if (!cloud_table)
                throw Exception(
                    "convert to StorageCloudMergeTree from table failed: " + storage_id.getFullTableName(), ErrorCodes::LOGICAL_ERROR);

            auto * deduper = cloud_table->getDedupWorker();
            deduper->setServerIndexAndHostPorts(deduper_index, host_ports);
            LOG_DEBUG(log, "Success to create deduper table: {}", storage->getStorageID().getNameForLogs());
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    })
}

void CnchWorkerServiceImpl::assignHighPriorityDedupPartition(
    [[maybe_unused]] google::protobuf::RpcController * ,
    [[maybe_unused]] const Protos::AssignHighPriorityDedupPartitionReq * request,
    [[maybe_unused]] Protos::AssignHighPriorityDedupPartitionResp * response,
    [[maybe_unused]] google::protobuf::Closure * done)
{
    SUBMIT_THREADPOOL({
        auto storage_id = RPCHelpers::createStorageID(request->table());
        auto storage = DatabaseCatalog::instance().getTable(storage_id, getContext());

        auto * cloud_table = dynamic_cast<StorageCloudMergeTree *>(storage.get());
        if (!cloud_table)
            throw Exception("convert to StorageCloudMergeTree from table failed: " + storage_id.getFullTableName(), ErrorCodes::LOGICAL_ERROR);

        auto * deduper = cloud_table->getDedupWorker();
        NameSet high_priority_dedup_partition;
        for (const auto & entry : request->partition_id())
            high_priority_dedup_partition.emplace(entry);
        deduper->assignHighPriorityDedupPartition(high_priority_dedup_partition);
    })
}

void CnchWorkerServiceImpl::assignRepairGran(
    [[maybe_unused]] google::protobuf::RpcController * ,
    [[maybe_unused]] const Protos::AssignRepairGranReq * request,
    [[maybe_unused]] Protos::AssignRepairGranResp * response,
    [[maybe_unused]] google::protobuf::Closure * done)
{
    SUBMIT_THREADPOOL({
        auto storage_id = RPCHelpers::createStorageID(request->table());
        auto storage = DatabaseCatalog::instance().getTable(storage_id, getContext());

        auto * cloud_table = dynamic_cast<StorageCloudMergeTree *>(storage.get());
        if (!cloud_table)
            throw Exception("convert to StorageCloudMergeTree from table failed: " + storage_id.getFullTableName(), ErrorCodes::LOGICAL_ERROR);

        auto * deduper = cloud_table->getDedupWorker();
        deduper->assignRepairGran(request->partition_id(), request->bucket_number(), request->max_event_time());
    })
}

void CnchWorkerServiceImpl::dropDedupWorker(
    google::protobuf::RpcController * cntl,
    const Protos::DropDedupWorkerReq * request,
    Protos::DropDedupWorkerResp * response,
    google::protobuf::Closure * done)
{
    SUBMIT_THREADPOOL({
        auto storage_id = RPCHelpers::createStorageID(request->table());
        auto rpc_context = RPCHelpers::createSessionContextForRPC(getContext(), *cntl);

        ASTPtr query = std::make_shared<ASTDropQuery>();
        auto * drop_query = query->as<ASTDropQuery>();
        drop_query->kind = ASTDropQuery::Drop;
        drop_query->if_exists = true;
        drop_query->database = storage_id.database_name;
        drop_query->table = storage_id.table_name;

        ThreadFromGlobalPool([log = this->log, storage_id, q = std::move(query), c = std::move(rpc_context)] {
            LOG_DEBUG(log, "Dropping table: {}", storage_id.getNameForLogs());
            // CurrentThread::attachQueryContext(*c);
            InterpreterDropQuery(q, c).execute();
            LOG_DEBUG(log, "Dropped table: {}", storage_id.getNameForLogs());
        }).detach();
    })
}

void CnchWorkerServiceImpl::sendBackupCopyTask(
    google::protobuf::RpcController *,
    const Protos::SendBackupCopyTaskReq * request,
    Protos::SendBackupCopyTaskResp * response,
    google::protobuf::Closure * done)
{
    // backup copy task will hang for a while, so we use separate thread pool to execute it
    {
        std::lock_guard lock(backup_lock);
        if (!backup_rpc_pool)
            backup_rpc_pool = std::make_unique<ThreadPool>(8, 8, 16, false);
    }
    try
    {
        backup_rpc_pool->scheduleOrThrowOnError([=, this]() {
            brpc::ClosureGuard done_guard(done);
            bool finished = false;
            const auto & backup_tasks = request->backup_task();
            try
            {
                // Check if the backup task is aborted before and after copying data
                checkBackupTaskNotAborted(request->id(), getContext());
                ThreadPool copy_pool(std::min(16, backup_tasks.size()));
                for (const auto & backup_task : backup_tasks)
                {
                    copy_pool.scheduleOrThrowOnError([&, backup_task]() {
                        setThreadName("BackupCopyThr");
                        DiskPtr source_disk = getContext()->getDisk(backup_task.source_disk());
                        DiskPtr destination_disk = getContext()->getDisk(backup_task.destination_disk());
                        auto read_buffer = source_disk->readFile(backup_task.source_path());
                        auto write_buffer = destination_disk->writeFile(backup_task.destination_path());
                        copyData(*read_buffer, *write_buffer);
                        write_buffer->finalize();
                    });
                }
                copy_pool.wait();
                finished = true;
                checkBackupTaskNotAborted(request->id(), getContext());
            }
            catch (...)
            {
                if (finished)
                {
                    for (const auto & backup_task : backup_tasks)
                    {
                        DiskPtr destination_disk = getContext()->getDisk(backup_task.destination_disk());
                        destination_disk->removeFileIfExists(backup_task.destination_path());
                    }
                }
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
                RPCHelpers::handleException(response->mutable_exception());
            }
        });
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
        done->Run();
    }
}

void CnchWorkerServiceImpl::getDedupWorkerStatus(
    google::protobuf::RpcController *,
    const Protos::GetDedupWorkerStatusReq * request,
    Protos::GetDedupWorkerStatusResp * response,
    google::protobuf::Closure * done)
{
    SUBMIT_THREADPOOL({
        response->set_is_active(false);

        auto storage_id = RPCHelpers::createStorageID(request->table());
        auto storage = DatabaseCatalog::instance().getTable(storage_id, getContext());

        auto * cloud_table = dynamic_cast<StorageCloudMergeTree *>(storage.get());
        if (!cloud_table)
            throw Exception(
                "convert to StorageCloudMergeTree from table failed: " + storage_id.getFullTableName(), ErrorCodes::LOGICAL_ERROR);

        auto * deduper = cloud_table->getDedupWorker();
        DedupWorkerStatus status = deduper->getDedupWorkerStatus();
        response->set_is_active(deduper->isActive());
        if (response->is_active())
        {
            response->set_create_time(status.create_time);
            response->set_total_schedule_cnt(status.total_schedule_cnt);
            response->set_total_dedup_cnt(status.total_dedup_cnt);
            response->set_last_schedule_wait_ms(status.last_schedule_wait_ms);
            response->set_last_task_total_cost_ms(status.last_task_total_cost_ms);
            response->set_last_task_dedup_cost_ms(status.last_task_dedup_cost_ms);
            response->set_last_task_publish_cost_ms(status.last_task_publish_cost_ms);
            response->set_last_task_staged_part_cnt(status.last_task_staged_part_cnt);
            response->set_last_task_visible_part_cnt(status.last_task_visible_part_cnt);
            response->set_last_task_staged_part_total_rows(status.last_task_staged_part_total_rows);
            response->set_last_task_visible_part_total_rows(status.last_task_visible_part_total_rows);
            for (const auto & task_progress : status.dedup_tasks_progress)
                response->add_dedup_tasks_progress(task_progress);
            response->set_last_exception(status.last_exception);
            response->set_last_exception_time(status.last_exception_time);
        }
    })
}

void CnchWorkerServiceImpl::executeDedupTask(
    google::protobuf::RpcController * cntl,
    const Protos::ExecuteDedupTaskReq * request,
    Protos::ExecuteDedupTaskResp * response,
    google::protobuf::Closure * done)
{
    SUBMIT_THREADPOOL({
        auto txn_id = TxnTimestamp(request->txn_id());
        auto rpc_context = RPCHelpers::createSessionContextForRPC(getContext(), *cntl);
        rpc_context->getClientInfo().rpc_port = request->rpc_port();
        auto server_client
            = rpc_context->getCnchServerClient(rpc_context->getClientInfo().current_address.host().toString(), request->rpc_port());
        auto worker_txn = std::make_shared<CnchWorkerTransaction>(rpc_context, txn_id, server_client);
        /// This stage is in commit process, we can not finish transaction here.
        worker_txn->setIsInitiator(false);
        rpc_context->setCurrentTransaction(worker_txn);

        auto catalog = getContext()->getCnchCatalog();
        TxnTimestamp ts = getContext()->getTimestamp();
        auto table_uuid_str = UUIDHelpers::UUIDToString(RPCHelpers::createUUID(request->table_uuid()));
        auto table = catalog->tryGetTableByUUID(*getContext(), table_uuid_str, ts);
        if (!table)
            throw Exception(ErrorCodes::ABORTED, "Table {} has been dropped", table_uuid_str);
        auto cnch_table = dynamic_pointer_cast<StorageCnchMergeTree>(table);
        if (!cnch_table)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Table {} is not cnch merge tree", table_uuid_str);

        auto new_parts = createPartVectorFromModels<MutableMergeTreeDataPartCNCHPtr>(*cnch_table, request->new_parts(), &request->new_parts_paths());
        DeleteBitmapMetaPtrVector delete_bitmaps_for_new_parts;
        delete_bitmaps_for_new_parts.reserve(request->delete_bitmaps_for_new_parts_size());
        for (const auto & bitmap_model : request->delete_bitmaps_for_new_parts())
            delete_bitmaps_for_new_parts.emplace_back(createFromModel(*cnch_table, bitmap_model));

        auto staged_parts = createPartVectorFromModels<MutableMergeTreeDataPartCNCHPtr>(*cnch_table, request->staged_parts(), &request->staged_parts_paths());
        DeleteBitmapMetaPtrVector delete_bitmaps_for_staged_parts;
        delete_bitmaps_for_staged_parts.reserve(request->delete_bitmaps_for_staged_parts_size());
        for (const auto & bitmap_model : request->delete_bitmaps_for_staged_parts())
            delete_bitmaps_for_staged_parts.emplace_back(createFromModel(*cnch_table, bitmap_model));

        auto visible_parts = createPartVectorFromModels<MutableMergeTreeDataPartCNCHPtr>(*cnch_table, request->visible_parts(), &request->visible_parts_paths());
        DeleteBitmapMetaPtrVector delete_bitmaps_for_visible_parts;
        delete_bitmaps_for_visible_parts.reserve(request->delete_bitmaps_for_visible_parts_size());
        for (const auto & bitmap_model : request->delete_bitmaps_for_visible_parts())
            delete_bitmaps_for_visible_parts.emplace_back(createFromModel(*cnch_table, bitmap_model));

        auto dedup_mode = static_cast<CnchDedupHelper::DedupMode>(request->dedup_mode());
        auto dedup_task = std::make_shared<CnchDedupHelper::DedupTask>(dedup_mode, cnch_table->getCnchStorageID());
        dedup_task->new_parts = std::move(new_parts);
        dedup_task->delete_bitmaps_for_new_parts = std::move(delete_bitmaps_for_new_parts);
        dedup_task->staged_parts = std::move(staged_parts);
        dedup_task->delete_bitmaps_for_staged_parts = std::move(delete_bitmaps_for_staged_parts);
        dedup_task->visible_parts = std::move(visible_parts);
        dedup_task->delete_bitmaps_for_visible_parts = std::move(delete_bitmaps_for_visible_parts);

        CnchDedupHelper::executeDedupTask(*cnch_table, *dedup_task, txn_id, rpc_context);
    })
}

#if USE_RDKAFKA
void CnchWorkerServiceImpl::submitKafkaConsumeTask(
    google::protobuf::RpcController * cntl,
    const Protos::SubmitKafkaConsumeTaskReq * request,
    Protos::SubmitKafkaConsumeTaskResp * response,
    google::protobuf::Closure * done)
{
    SUBMIT_THREADPOOL({
        /// parse command params passed by brpc
        auto command = std::make_shared<KafkaTaskCommand>();
        command->type = static_cast<KafkaTaskCommand::Type>(request->type());
        if (request->task_id().empty())
            throw Exception("Require non-empty task_id", ErrorCodes::BAD_ARGUMENTS);
        command->task_id = request->task_id();
        command->rpc_port = static_cast<UInt16>(request->rpc_port());

        command->cnch_storage_id = RPCHelpers::createStorageID(request->cnch_storage_id());
        if (command->cnch_storage_id.empty())
            throw Exception("cnch_storage_id is required while starting consumer", ErrorCodes::BAD_ARGUMENTS);

        command->local_database_name = request->database();
        command->local_table_name = request->table();

        if (command->type == KafkaTaskCommand::START_CONSUME)
        {
            command->assigned_consumer = request->assigned_consumer();

            if (request->create_table_command_size() < 2)
                throw Exception(
                    "The number of tables to be created should be larger than 2, but provided with "
                        + toString(request->create_table_command_size()),
                    ErrorCodes::BAD_ARGUMENTS);
            for (const auto & cmd : request->create_table_command())
                command->create_table_commands.push_back(cmd);

            if (request->tpl_size() == 0)
                throw Exception("TopicPartitionList is required for starting consume", ErrorCodes::BAD_ARGUMENTS);
            for (const auto & tpl : request->tpl())
                command->tpl.emplace_back(tpl.topic(), tpl.partition(), tpl.offset());
            for (const auto & tpl: request->sample_partitions())
                command->sample_partitions.emplace(tpl.topic(), tpl.partition());
        }

        auto rpc_context = RPCHelpers::createSessionContextForRPC(getContext(), *cntl);
        rpc_context->setCurrentQueryId(request->task_id());
        rpc_context->getClientInfo().rpc_port = static_cast<UInt16>(request->rpc_port());
        ///rpc_context->setQueryContext(*rpc_context);

        LOG_TRACE(log, "Successfully to parse kafka-consumer command: {}", KafkaTaskCommand::typeToString(command->type));

        /// create thread to execute kafka-consume-task
        ThreadFromGlobalPool([p = std::move(command), c = std::move(rpc_context)] {
            ///CurrentThread::attachQueryContext(*c);
            DB::executeKafkaConsumeTask(*p, c);
        }).detach();
    })
}

void CnchWorkerServiceImpl::getConsumerStatus(
    google::protobuf::RpcController * cntl,
    const Protos::GetConsumerStatusReq * request,
    Protos::GetConsumerStatusResp * response,
    google::protobuf::Closure * done)
{
    SUBMIT_THREADPOOL({
        auto rpc_context = RPCHelpers::createSessionContextForRPC(getContext(), *cntl);
        rpc_context->makeQueryContext();

        auto storage_id = RPCHelpers::createStorageID(request->table());
        auto kafka_table = DatabaseCatalog::instance().getTable(storage_id, rpc_context);

        auto * storage = dynamic_cast<StorageCloudKafka *>(kafka_table.get());
        if (!storage)
            throw Exception("StorageCloudKafka expected but get " + kafka_table->getTableName(), ErrorCodes::BAD_ARGUMENTS);

        CnchConsumerStatus status;
        storage->getConsumersStatus(status);

        response->set_cluster(status.cluster);
        for (auto & topic : status.topics)
            response->add_topics(topic);
        for (auto & tpl : status.assignment)
            response->add_assignments(tpl);
        response->set_consumer_num(status.assigned_consumers);
        response->set_last_exception(status.last_exception);
    })
}
#endif

#if USE_MYSQL
void CnchWorkerServiceImpl::submitMySQLSyncThreadTask(
    [[maybe_unused]] google::protobuf::RpcController * cntl,
    [[maybe_unused]] const Protos::SubmitMySQLSyncThreadTaskReq * request,
    [[maybe_unused]] Protos::SubmitMySQLSyncThreadTaskResp * response,
    [[maybe_unused]] google::protobuf::Closure * done)
{
    SUBMIT_THREADPOOL({
        if (request->database_name().empty() || request->sync_thread_key().empty())
            throw Exception("MySQLSyncThread task requires database name [" + request->database_name()
                            + "] and thread key [" + request->sync_thread_key() +"] should not be empty", ErrorCodes::BAD_ARGUMENTS);

        auto command = std::make_shared<MySQLSyncThreadCommand>();
        command->type = MySQLSyncThreadCommand::CommandType(request->type());
        command->database_name = request->database_name();
        command->sync_thread_key = request->sync_thread_key();
        command->table = request->table();

        if (command->type == MySQLSyncThreadCommand::START_SYNC)
        {
            if (request->create_sqls().empty())
                throw Exception("Try to START SyncThread but has no create sqls", ErrorCodes::BAD_ARGUMENTS);
            if (request->binlog_file().empty())
                throw Exception("Try to START SyncThread but has no binlog file", ErrorCodes::BAD_ARGUMENTS);

            for (const auto & create_sql : request->create_sqls())
                command->create_sqls.emplace_back(create_sql);

            command->binlog.binlog_file = request->binlog_file();
            command->binlog.binlog_position = request->binlog_position();
            command->binlog.executed_gtid_set = request->executed_gtid_set();
            command->binlog.meta_version = request->meta_version();
        }

        auto rpc_context = RPCHelpers::createSessionContextForRPC(getContext(), *cntl);
        rpc_context->getClientInfo().rpc_port = static_cast<UInt16>(request->rpc_port());

        LOG_TRACE(log, "Successfully to parse SyncThread task command: {}", MySQLSyncThreadCommand::toString(command->type));

        /// create thread to execute command
        ThreadFromGlobalPool([p = std::move(command), c = std::move(rpc_context)] {
            // CurrentThread::attachQueryContext(*c);
            DB::executeSyncThreadTaskCommand(*p, c);
        }).detach();
    })
}

void CnchWorkerServiceImpl::checkMySQLSyncThreadStatus(
    [[maybe_unused]] google::protobuf::RpcController * cntl,
    [[maybe_unused]] const Protos::CheckMySQLSyncThreadStatusReq * request,
    [[maybe_unused]] Protos::CheckMySQLSyncThreadStatusResp * response,
    [[maybe_unused]] google::protobuf::Closure * done)
{
    SUBMIT_THREADPOOL({
        auto database = DatabaseCatalog::instance().getDatabase(request->database_name(), getContext());
        if (!database)
            throw Exception("Database " + request->database_name() + " doesn't exist", ErrorCodes::LOGICAL_ERROR);

        auto * materialized_mysql = dynamic_cast<DatabaseCloudMaterializedMySQL*>(database.get());
        if (!materialized_mysql)
            throw Exception("DatabaseCloudMaterializedMySQL is expected, but got " + database->getEngineName(), ErrorCodes::LOGICAL_ERROR);

        response->set_is_running(materialized_mysql->syncThreadIsRunning(request->sync_thread_key()));
    })
}
#endif

void CnchWorkerServiceImpl::preloadChecksumsAndPrimaryIndex(
    google::protobuf::RpcController * cntl,
    const Protos::PreloadChecksumsAndPrimaryIndexReq * request,
    Protos::PreloadChecksumsAndPrimaryIndexResp * response,
    google::protobuf::Closure * done)
{
}

void CnchWorkerServiceImpl::getCloudMergeTreeStatus(
    google::protobuf::RpcController * cntl,
    const Protos::GetCloudMergeTreeStatusReq * request,
    Protos::GetCloudMergeTreeStatusResp * response,
    google::protobuf::Closure * done)
{
}

void CnchWorkerServiceImpl::broadcastManifest(
    [[maybe_unused]] google::protobuf::RpcController * cntl,
    [[maybe_unused]] const Protos::BroadcastManifestReq * request,
    [[maybe_unused]] Protos::BroadcastManifestResp * response,
    [[maybe_unused]] google::protobuf::Closure * done
)
{
    SUBMIT_THREADPOOL({
        const UUID storage_uuid = RPCHelpers::createUUID(request->table_uuid());
        const UInt64 txn_id = request->txn_id();
        //WGWorkerInfoPtr worker_info = RPCHelpers::createWorkerInfo(request->worker_info());
        DataModelPartPtrVector part_models;
        DataModelDeleteBitmapPtrVector delete_bitmap_models;

        for (const auto & part : request->parts())
        {
            DataModelPartPtr part_model = std::make_shared<Protos::DataModelPart>(part);
            part_models.push_back(part_model);
        }

        for (const auto & dbm : request->delete_bitmaps())
        {
            DataModelDeleteBitmapPtr dbm_model = std::make_shared<DataModelDeleteBitmap>(dbm);
            delete_bitmap_models.push_back(dbm_model);
        }

        getContext()->getManifestCache()->addManifest(storage_uuid, txn_id, std::move(part_models), std::move(delete_bitmap_models));
    })
}

#if defined(__clang__)
#    pragma clang diagnostic pop
#else
#    pragma GCC diagnostic pop
#endif

}
