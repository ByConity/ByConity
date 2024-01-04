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

#include <CloudServices/CnchWorkerClient.h>

#include <CloudServices/CnchServerResource.h>
#include <CloudServices/DedupWorkerStatus.h>
#include <Interpreters/Context.h>
#include <Protos/DataModelHelpers.h>
#include <Protos/RPCHelpers.h>
#include <Protos/cnch_worker_rpc.pb.h>
#include <Storages/IStorage.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Transaction/ICnchTransaction.h>
#include <WorkerTasks/ManipulationList.h>
#include <WorkerTasks/ManipulationTaskParams.h>
#include "Storages/Hive/HiveFile/IHiveFile.h"

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <common/logger_useful.h>
#include <Storages/MergeTree/MarkRange.h>
namespace DB
{
CnchWorkerClient::CnchWorkerClient(String host_port_)
    : RpcClientBase(getName(), std::move(host_port_)), stub(std::make_unique<Protos::CnchWorkerService_Stub>(&getChannel()))
{
}

CnchWorkerClient::CnchWorkerClient(HostWithPorts host_ports_)
    : RpcClientBase(getName(), std::move(host_ports_)), stub(std::make_unique<Protos::CnchWorkerService_Stub>(&getChannel()))
{
}

CnchWorkerClient::~CnchWorkerClient() = default;

void CnchWorkerClient::submitManipulationTask(
    const MergeTreeMetaBase & storage, const ManipulationTaskParams & params, TxnTimestamp txn_id)
{
    if (!params.rpc_port)
        throw Exception("Rpc port is not set in ManipulationTaskParams", ErrorCodes::LOGICAL_ERROR);

    brpc::Controller cntl;
    Protos::SubmitManipulationTaskReq request;
    Protos::SubmitManipulationTaskResp response;

    request.set_txn_id(txn_id);
    request.set_timestamp(0); /// NOTE: do not remove this as `timestamp` is a required field.
    request.set_type(static_cast<UInt32>(params.type));
    request.set_task_id(params.task_id);
    request.set_rpc_port(params.rpc_port);
    request.set_columns_commit_time(params.columns_commit_time);
    request.set_is_bucket_table(params.is_bucket_table);
    if (!params.create_table_query.empty())
        request.set_create_table_query(params.create_table_query);
    fillPartsModelForSend(storage, params.source_parts, *request.mutable_source_parts());

    if (params.type == ManipulationType::Mutate || params.type == ManipulationType::Clustering)
    {
        request.set_mutation_commit_time(params.mutation_commit_time);
        WriteBufferFromString write_buf(*request.mutable_mutate_commands());
        params.mutation_commands->writeText(write_buf);
    }

    stub->submitManipulationTask(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

void CnchWorkerClient::shutdownManipulationTasks(const UUID & table_uuid, const Strings & task_ids)
{
    brpc::Controller cntl;
    Protos::ShutdownManipulationTasksReq request;
    Protos::ShutdownManipulationTasksResp response;

    RPCHelpers::fillUUID(table_uuid, *request.mutable_table_uuid());
    if (!task_ids.empty())
    {
        std::for_each(task_ids.begin(), task_ids.end(), [&request](const String & task_id) { request.add_task_ids(task_id); });
    }

    stub->shutdownManipulationTasks(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

std::unordered_set<std::string> CnchWorkerClient::touchManipulationTasks(const UUID & table_uuid, const Strings & tasks_id)
{
    brpc::Controller cntl;
    Protos::TouchManipulationTasksReq request;
    Protos::TouchManipulationTasksResp response;

    RPCHelpers::fillUUID(table_uuid, *request.mutable_table_uuid());

    for (const auto & t : tasks_id)
        request.add_tasks_id(t);

    stub->touchManipulationTasks(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);

    return {response.tasks_id().begin(), response.tasks_id().end()};
}

std::vector<ManipulationInfo> CnchWorkerClient::getManipulationTasksStatus()
{
    brpc::Controller cntl;
    Protos::GetManipulationTasksStatusReq request;
    Protos::GetManipulationTasksStatusResp response;

    stub->getManipulationTasksStatus(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);

    std::vector<ManipulationInfo> res;
    res.reserve(response.tasks_size());

    for (const auto & task : response.tasks())
    {
        ManipulationInfo info(RPCHelpers::createStorageID(task.storage_id()));
        info.type = ManipulationType(task.type());
        info.related_node = this->getRPCAddress();
        info.elapsed = task.elapsed();
        info.num_parts = task.num_parts();
        for (const auto & source_part_name : task.source_part_names())
            info.source_part_names.emplace_back(source_part_name);
        for (const auto & result_part_name : task.result_part_names())
            info.result_part_names.emplace_back(result_part_name);
        info.partition_id = task.partition_id();
        info.total_size_bytes_compressed = task.total_size_bytes_compressed();
        info.total_size_marks = task.total_size_marks();
        info.total_rows_count = task.total_rows_count();
        info.progress = task.progress();
        info.bytes_read_uncompressed = task.bytes_read_uncompressed();
        info.bytes_written_uncompressed = task.bytes_written_uncompressed();
        info.rows_read = task.rows_read();
        info.rows_written = task.rows_written();
        info.columns_written = task.columns_written();
        info.memory_usage = task.memory_usage();
        info.thread_id = task.thread_id();
        res.emplace_back(info);
    }

    return res;
}

void CnchWorkerClient::sendCreateQueries(const ContextPtr & context, const std::vector<String> & create_queries)
{
    brpc::Controller cntl;
    Protos::SendCreateQueryReq request;
    Protos::SendCreateQueryResp response;

    const auto & settings = context->getSettingsRef();
    auto timeout = settings.max_execution_time.value.totalSeconds();

    request.set_txn_id(context->getCurrentTransactionID());
    request.set_primary_txn_id(context->getCurrentTransaction()->getPrimaryTransactionID()); /// Why?
    request.set_timeout(timeout ? timeout : 3600); // clean session resource if there exists Exception after 3600s

    for (const auto & create_query : create_queries)
        *request.mutable_create_queries()->Add() = create_query;

    stub->sendCreateQuery(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

brpc::CallId CnchWorkerClient::preloadDataParts(
    const ContextPtr & context,
    const TxnTimestamp & txn_id,
    const IStorage & storage,
    const String & create_local_table_query,
    const ServerDataPartsVector & parts,
    const ExceptionHandlerPtr & handler,
    bool enable_parts_sync_preload,
    UInt64 parts_preload_level,
    UInt64 submit_ts
   )
{
    Protos::PreloadDataPartsReq request;
    request.set_txn_id(txn_id);
    request.set_create_table_query(create_local_table_query);
    request.set_sync(enable_parts_sync_preload);
    request.set_preload_level(parts_preload_level);
    request.set_submit_ts(submit_ts);
    fillPartsModelForSend(storage, parts, *request.mutable_parts());

    auto * cntl = new brpc::Controller();
    auto * response = new Protos::PreloadDataPartsResp();
    /// adjust the timeout to prevent timeout if there are too many parts to send,
    const auto & settings = context->getSettingsRef();
    auto send_timeout = std::max(settings.max_execution_time.value.totalMilliseconds() >> 1, settings.brpc_data_parts_timeout_ms.totalMilliseconds());
    cntl->set_timeout_ms(send_timeout);

    auto call_id = cntl->call_id();
    stub->preloadDataParts(cntl, &request, response, brpc::NewCallback(RPCHelpers::onAsyncCallDone, response, cntl, handler));
    return call_id;
}

brpc::CallId CnchWorkerClient::dropPartDiskCache(
    const ContextPtr & context,
    const TxnTimestamp & txn_id,
    const IStorage & storage,
    const String & create_local_table_query,
    const ServerDataPartsVector & parts,
    bool sync,
    bool drop_vw_disk_cache)
{
    brpc::Controller cntl;
    Protos::DropPartDiskCacheReq request;
    Protos::DropPartDiskCacheResp response;

    const auto & settings = context->getSettingsRef();
    auto send_timeout = std::max(settings.max_execution_time.value.totalMilliseconds() >> 1, settings.brpc_data_parts_timeout_ms.totalMilliseconds());
    cntl.set_timeout_ms(send_timeout);

    request.set_txn_id(txn_id);
    request.set_create_table_query(create_local_table_query);
    request.set_sync(sync);
    request.set_drop_vw_disk_cache(drop_vw_disk_cache);

    fillPartsModelForSend(storage, parts, *request.mutable_parts());
    stub->dropPartDiskCache(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
    return cntl.call_id();
}

brpc::CallId CnchWorkerClient::sendQueryDataParts(
    const ContextPtr & context,
    const StoragePtr & storage,
    const String & local_table_name,
    const ServerDataPartsVector & data_parts,
    const std::set<Int64> & required_bucket_numbers,
    const ExceptionHandlerWithFailedInfoPtr & handler,
    const WorkerId & worker_id)
{
    Protos::SendDataPartsReq request;
    request.set_txn_id(context->getCurrentTransactionID());
    request.set_database_name(storage->getDatabaseName());
    request.set_table_name(local_table_name);
    request.set_disk_cache_mode(context->getSettingsRef().disk_cache_mode.toString());

    fillBasePartAndDeleteBitmapModels(*storage, data_parts, *request.mutable_parts(), *request.mutable_bitmaps());
    for (const auto & bucket_num : required_bucket_numbers)
        *request.mutable_bucket_numbers()->Add() = bucket_num;

    // TODO:
    // auto udf_info = context.getNonSqlUdfVersionMap();
    // for (const auto & [name, version]: udf_info)
    // {
    //     auto & new_info = *request.mutable_udf_infos()->Add();
    //     new_info.set_function_name(name);
    //     new_info.set_version(version);
    // }


    auto * cntl = new brpc::Controller();
    auto * response = new Protos::SendDataPartsResp();
    /// adjust the timeout to prevent timeout if there are too many parts to send,
    const auto & settings = context->getSettingsRef();
    auto send_timeout = std::max(settings.max_execution_time.value.totalMilliseconds() >> 1, settings.brpc_data_parts_timeout_ms.totalMilliseconds());
    cntl->set_timeout_ms(send_timeout);

    auto call_id = cntl->call_id();
    stub->sendQueryDataParts(
        cntl, &request, response, brpc::NewCallback(RPCHelpers::onAsyncCallDoneWithFailedInfo, response, cntl, handler, worker_id));

    return call_id;
}

brpc::CallId CnchWorkerClient::sendOffloadingInfo( // NOLINT
    [[maybe_unused]] const ContextPtr & context,
    [[maybe_unused]] const HostWithPortsVec & read_workers,
    [[maybe_unused]] const std::vector<std::pair<StorageID, String>> & worker_table_names,
    [[maybe_unused]] const std::vector<HostWithPortsVec> & buffer_workers_vec,
    [[maybe_unused]] const ExceptionHandlerPtr & handler)
{
    /// TODO:
    return {};
}

brpc::CallId CnchWorkerClient::sendResources(
    const ContextPtr & context,
    const std::vector<AssignedResource> & resources_to_send,
    const ExceptionHandlerWithFailedInfoPtr & handler,
    const WorkerId & worker_id,
    bool with_mutations)
{
    Protos::SendResourcesReq request;

    const auto & settings = context->getSettingsRef();
    auto max_execution_time = settings.max_execution_time.value.totalSeconds();

    request.set_txn_id(context->getCurrentTransactionID());
    request.set_primary_txn_id(context->getCurrentTransaction()->getPrimaryTransactionID());
    /// recycle_timeout refers to the time when the session is recycled under abnormal case,
    /// so it should be larger than max_execution_time to make sure the session is not to be destroyed in advance.
    auto recycle_timeout = max_execution_time ? max_execution_time + 60 : 3600;
    request.set_timeout(recycle_timeout);

    for (const auto & resource : resources_to_send)
    {
        if (!resource.sent_create_query)
            request.add_create_queries(resource.create_table_query);

        /// parts
        auto & table_data_parts = *request.mutable_data_parts()->Add();

        /// Send storage's mutations to worker if needed.
        if (with_mutations)
        {
            auto * cnch_merge_tree = dynamic_cast<StorageCnchMergeTree *>(resource.storage.get());
            if (cnch_merge_tree)
            {
                for (auto const & mutation_str : cnch_merge_tree->getPlainMutationEntries())
                {
                    LOG_TRACE(&Poco::Logger::get(__func__), "Send mutations to worker: {}", mutation_str);
                    table_data_parts.add_cnch_mutation_entries(mutation_str);
                }
            }
        }

        table_data_parts.set_database(resource.storage->getDatabaseName());
        table_data_parts.set_table(resource.worker_table_name);

        if (!resource.server_parts.empty())
        {
            // todo(jiashuo): bitmap need handler?
            fillBasePartAndDeleteBitmapModels(
                *resource.storage,
                resource.server_parts,
                *table_data_parts.mutable_server_parts(),
                *table_data_parts.mutable_server_part_bitmaps());
        }

        if (!resource.hive_parts.empty())
        {
            auto * mutable_hive_parts = table_data_parts.mutable_hive_parts();
            RPCHelpers::serialize(*mutable_hive_parts, resource.hive_parts);
        }

        if (!resource.file_parts.empty())
        {
            fillCnchFilePartsModel(resource.file_parts, *table_data_parts.mutable_file_parts());
        }

        /// bucket numbers
        for (const auto & bucket_num : resource.bucket_numbers)
            *table_data_parts.mutable_bucket_numbers()->Add() = bucket_num;
    }

    request.set_disk_cache_mode(context->getSettingsRef().disk_cache_mode.toString());

    brpc::Controller * cntl = new brpc::Controller;
    /// send_timeout refers to the time to send resource to worker
    /// If max_execution_time is not set, the send_timeout will be set to brpc_data_parts_timeout_ms
    auto send_timeout_ms = max_execution_time ? max_execution_time * 1000L : settings.brpc_data_parts_timeout_ms.totalMilliseconds();
    cntl->set_timeout_ms(send_timeout_ms);
    const auto call_id = cntl->call_id();
    auto * response = new Protos::SendResourcesResp();
    LOG_INFO(&Poco::Logger::get(__func__), request.DebugString());
    stub->sendResources(cntl, &request, response, brpc::NewCallback(RPCHelpers::onAsyncCallDoneWithFailedInfo, response, cntl, handler, worker_id));

    return call_id;
}

void CnchWorkerClient::removeWorkerResource(TxnTimestamp txn_id)
{
    brpc::Controller cntl;
    Protos::RemoveWorkerResourceReq request;
    Protos::RemoveWorkerResourceResp response;

    request.set_txn_id(txn_id);
    stub->removeWorkerResource(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

void CnchWorkerClient::createDedupWorker(const StorageID & storage_id, const String & create_table_query, const HostWithPorts & host_ports_)
{
    brpc::Controller cntl;
    Protos::CreateDedupWorkerReq request;
    Protos::CreateDedupWorkerResp response;

    RPCHelpers::fillStorageID(storage_id, *request.mutable_table());
    request.set_create_table_query(create_table_query);
    RPCHelpers::fillHostWithPorts(host_ports_, *request.mutable_host_ports());

    stub->createDedupWorker(&cntl, &request, &response, nullptr);
    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

void CnchWorkerClient::dropDedupWorker(const StorageID & storage_id)
{
    brpc::Controller cntl;
    Protos::DropDedupWorkerReq request;
    Protos::DropDedupWorkerResp response;

    RPCHelpers::fillStorageID(storage_id, *request.mutable_table());

    stub->dropDedupWorker(&cntl, &request, &response, nullptr);
    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

DedupWorkerStatus CnchWorkerClient::getDedupWorkerStatus(const StorageID & storage_id)
{
    brpc::Controller cntl;
    Protos::GetDedupWorkerStatusReq request;
    Protos::GetDedupWorkerStatusResp response;
    RPCHelpers::fillStorageID(storage_id, *request.mutable_table());

    stub->getDedupWorkerStatus(&cntl, &request, &response, nullptr);
    assertController(cntl);
    RPCHelpers::checkResponse(response);

    DedupWorkerStatus status;
    status.is_active = response.is_active();
    if (status.is_active)
    {
        status.create_time = response.create_time();
        status.total_schedule_cnt = response.total_schedule_cnt();
        status.total_dedup_cnt = response.total_dedup_cnt();
        status.last_schedule_wait_ms = response.last_schedule_wait_ms();
        status.last_task_total_cost_ms = response.last_task_total_cost_ms();
        status.last_task_dedup_cost_ms = response.last_task_dedup_cost_ms();
        status.last_task_publish_cost_ms = response.last_task_publish_cost_ms();
        status.last_task_staged_part_cnt = response.last_task_staged_part_cnt();
        status.last_task_visible_part_cnt = response.last_task_visible_part_cnt();
        status.last_task_staged_part_total_rows = response.last_task_staged_part_total_rows();
        status.last_task_visible_part_total_rows = response.last_task_visible_part_total_rows();
        status.last_exception = response.last_exception();
        status.last_exception_time = response.last_exception_time();
    }
    return status;
}

#if USE_RDKAFKA
CnchConsumerStatus CnchWorkerClient::getConsumerStatus(const StorageID & storage_id)
{
    brpc::Controller cntl;
    Protos::GetConsumerStatusReq request;
    Protos::GetConsumerStatusResp response;
    RPCHelpers::fillStorageID(storage_id, *request.mutable_table());

    stub->getConsumerStatus(&cntl, &request, &response, nullptr);
    assertController(cntl);
    RPCHelpers::checkResponse(response);

    CnchConsumerStatus status;
    status.cluster = response.cluster();
    for (const auto & topic : response.topics())
        status.topics.emplace_back(topic);
    for (const auto & tpl : response.assignments())
        status.assignment.emplace_back(tpl);
    status.assigned_consumers = response.consumer_num();
    status.last_exception = response.last_exception();

    return status;
}

void CnchWorkerClient::submitKafkaConsumeTask(const KafkaTaskCommand & command)
{
    if (!command.rpc_port)
        throw Exception("Rpc port is not set in KafkaTaskCommand", ErrorCodes::LOGICAL_ERROR);

    brpc::Controller cntl;
    Protos::SubmitKafkaConsumeTaskReq request;
    Protos::SubmitKafkaConsumeTaskResp response;

    request.set_type(command.type);
    request.set_task_id(command.task_id);
    request.set_rpc_port(command.rpc_port);
    RPCHelpers::fillStorageID(command.cnch_storage_id, *request.mutable_cnch_storage_id());
    request.set_database(command.local_database_name);
    request.set_table(command.local_table_name);
    request.set_assigned_consumer(command.assigned_consumer);
    for (const auto & cmd : command.create_table_commands)
    {
        request.add_create_table_command(cmd);
    }
    for (const auto & tpl : command.tpl)
    {
        auto * cur_tpl = request.add_tpl();
        cur_tpl->set_topic(toString(tpl.get_topic()));
        cur_tpl->set_partition(tpl.get_partition());
        cur_tpl->set_offset(tpl.get_offset());
    }

    stub->submitKafkaConsumeTask(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
}
#endif

#if USE_MYSQL
void CnchWorkerClient::submitMySQLSyncThreadTask(const MySQLSyncThreadCommand & command)
{
    brpc::Controller cntl;
    Protos::SubmitMySQLSyncThreadTaskReq request;
    Protos::SubmitMySQLSyncThreadTaskResp response;

    request.set_type(command.type);
    request.set_database_name(command.database_name);
    request.set_sync_thread_key(command.sync_thread_key);
    request.set_rpc_port(command.rpc_port);
    request.set_table(command.table);

    if (command.type == MySQLSyncThreadCommand::START_SYNC)
    {
        for (const auto & create_sql : command.create_sqls)
            request.add_create_sqls(create_sql);

        request.set_binlog_file(command.binlog.binlog_file);
        request.set_binlog_position(command.binlog.binlog_position);
        request.set_executed_gtid_set(command.binlog.executed_gtid_set);
        request.set_meta_version(command.binlog.meta_version);
    }

    stub->submitMySQLSyncThreadTask(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

bool CnchWorkerClient::checkMySQLSyncThreadStatus(const String & database_name, const String & sync_thread)
{
    brpc::Controller cntl;
    Protos::CheckMySQLSyncThreadStatusReq request;
    Protos::CheckMySQLSyncThreadStatusResp response;

    request.set_database_name(database_name);
    request.set_sync_thread_key(sync_thread);

    stub->checkMySQLSyncThreadStatus(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);

    return response.is_running();
}
#endif

}
