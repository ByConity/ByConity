#include <CloudServices/CnchWorkerClient.h>

#include <Protos/cnch_worker_rpc.pb.h>
#include <Protos/RPCHelpers.h>
#include <Protos/DataModelHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <WorkerTasks/ManipulationTaskParams.h>
#include <WorkerTasks/ManipulationList.h>

#include <brpc/channel.h>
#include <brpc/controller.h>

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
    const MergeTreeMetaBase & storage,
    const ManipulationTaskParams & params,
    TxnTimestamp txn_id,
    TxnTimestamp begin_ts)
{
    if (!params.rpc_port)
        throw Exception("Rpc port is not set in ManipulationTaskParams", ErrorCodes::LOGICAL_ERROR);

    brpc::Controller cntl;
    Protos::SubmitManipulationTaskReq request;
    Protos::SubmitManipulationTaskResp response;

    request.set_txn_id(txn_id);
    request.set_timestamp(begin_ts);
    request.set_type(static_cast<UInt32>(params.type));
    request.set_task_id(params.task_id);
    request.set_rpc_port(params.rpc_port);
    request.set_columns_commit_time(params.columns_commit_time);
    request.set_is_bucket_table(params.is_bucket_table);
    if (!params.create_table_query.empty())
        request.set_create_table_query(params.create_table_query);
    fillPartsModelForSend(storage, params.source_parts, *request.mutable_source_parts());

    if (params.type == ManipulationType::Mutate)
    {
        request.set_mutation_commit_time(params.mutation_commit_time);
        WriteBufferFromString write_buf(*request.mutable_mutate_commands());
        params.mutation_commands->writeText(write_buf);
    }

    stub->submitManipulationTask(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

void CnchWorkerClient::shutdownManipulationTasks(const UUID & table_uuid)
{
    brpc::Controller cntl;
    Protos::ShutdownManipulationTasksReq request;
    Protos::ShutdownManipulationTasksResp response;

    RPCHelpers::fillUUID(table_uuid, *request.mutable_table_uuid());
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
    for (const auto & task: response.tasks())
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
    auto timeout = settings.max_execution_time.value.seconds();

    /// TODO:
    request.set_txn_id(0);
    request.set_primary_txn_id(0);
    request.set_timeout(timeout ? timeout : 3600);  // clean session resource if there exists Exception after 3600s

    for (const auto & create_query: create_queries)
        *request.mutable_create_queries()->Add() = create_query;

    stub->sendCreateQuery(&cntl, &request, &response, nullptr);
    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

void CnchWorkerClient::sendQueryDataParts(
    const ContextPtr & context,
    const StoragePtr & storage,
    const String & local_table_name,
    const ServerDataPartsVector & data_parts,
    const std::set<Int64> & required_bucket_numbers)
{
    brpc::Controller cntl;
    Protos::SendDataPartsReq request;
    Protos::SendDataPartsResp response;
    request.set_txn_id(0);
    request.set_database_name(storage->getDatabaseName());
    request.set_table_name(local_table_name);

    fillBasePartAndDeleteBitmapModels(*storage, data_parts, *request.mutable_parts(), *request.mutable_bitmaps());
    for (const auto & bucket_num: required_bucket_numbers)
        *request.mutable_bucket_numbers()->Add() = bucket_num;

    // TODO:
    // auto udf_info = context.getNonSqlUdfVersionMap();
    // for (const auto & [name, version]: udf_info)
    // {
    //     auto & new_info = *request.mutable_udf_infos()->Add();
    //     new_info.set_function_name(name);
    //     new_info.set_version(version);
    // }

    /// adjust the timeout to prevent timeout if there are too many parts to send,
    const auto & settings = context->getSettingsRef();
    auto send_timeout = std::max(settings.max_execution_time.value.totalMilliseconds() >> 1, 30 * 1000L);
    cntl.set_timeout_ms(send_timeout);

    stub->sendQueryDataParts(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

void CnchWorkerClient::sendOffloadingInfo(
    [[maybe_unused]]const ContextPtr & context,
    [[maybe_unused]]const HostWithPortsVec & read_workers,
    [[maybe_unused]]const std::vector<std::pair<StorageID, String>> & worker_table_names,
    [[maybe_unused]]const std::vector<HostWithPortsVec> & buffer_workers_vec)
{
    /// TODO:
}

void CnchWorkerClient::sendFinishTask(TxnTimestamp txn_id, bool only_clean)
{
    brpc::Controller cntl;
    Protos::SendFinishTaskReq request;
    Protos::SendFinishTaskResp response;

    request.set_txn_id(txn_id);
    request.set_only_clean(only_clean);
    stub->sendFinishTask(&cntl, &request, &response, nullptr);
    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

}
