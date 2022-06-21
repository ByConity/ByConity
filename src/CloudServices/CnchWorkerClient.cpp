#include <CloudServices/CnchWorkerClient.h>

#include <Protos/cnch_worker_rpc.pb.h>
#include <Protos/RPCHelpers.h>
#include <Protos/DataModelHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>

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
    for (auto & bucket_num: required_bucket_numbers)
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
