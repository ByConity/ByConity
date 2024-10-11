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

#include <ResourceManagement/ResourceManagerClient.h>

#include <Catalog/Catalog.h>
#include <Common/Configurations.h>
#include <Common/StorageElection/KvStorage.h>
#include <Protos/RPCHelpers.h>
#include <Protos/data_models.pb.h>
#include <Protos/resource_manager_rpc.pb.h>
#include <ResourceManagement/CommonData.h>
#include <ResourceManagement/WorkerNode.h>
#include <ServiceDiscovery/IServiceDiscovery.h>

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <common/logger_useful.h>
#include <memory>


namespace DB::ErrorCodes
{
extern const int RESOURCE_MANAGER_ILLEGAL_CONFIG;
extern const int RESOURCE_MANAGER_NO_AVAILABLE_WORKER;
}

namespace DB::ResourceManagement
{

ResourceManagerClient::ResourceManagerClient(ContextPtr global_context_)
    : WithContext(global_context_)
    , RpcLeaderClientBase(getName(), "127.0.0.1:18989")
{
    auto prefix = getContext()->getRootConfig().service_discovery_kv.election_prefix.value;
    auto election_path = prefix + getContext()->getRootConfig().service_discovery_kv.resource_manager_host_path.value;
    auto metastore_ptr = getContext()->getCnchCatalog()->getMetastore();
    election_reader = std::make_unique<ElectionReader>(
        std::make_shared<ResourceManagerKvStorage>(metastore_ptr),
        election_path);
    auto addr = fetchRMAddress();

    /// Have to init stub after initialzing election_reader.
    stub = std::make_unique<Stub>(&updateChannel(addr));
}

String fetchRMAddressByPSM(ContextPtr context)
{
    auto sd = context->getServiceDiscoveryClient();
    if (!sd)
    {
        throw Exception("Can't initialise RM client in PSM mode as the SD client is null.", ErrorCodes::RESOURCE_MANAGER_ILLEGAL_CONFIG);
    }
    auto psm = context->getRootConfig().service_discovery.resource_manager_psm.value;
    auto addresses = sd->lookup(psm, ComponentType::RESOURCE_MANAGER);
    if (addresses.empty())
    {
        throw Exception("No RM instance found with psm: " + psm, ErrorCodes::RESOURCE_MANAGER_ILLEGAL_CONFIG);
    }
    if (addresses.size() > 1)
    {
        std::stringstream ss;
        ss << psm << ": ";
        for (const auto & address : addresses)
        {
            ss << address.getRPCAddress() << " ";
        }
        throw Exception("Only one instance is allowed in PSM mode, but multiple instances found: " + ss.str(), ErrorCodes::RESOURCE_MANAGER_ILLEGAL_CONFIG);
    }
    return addresses[0].getRPCAddress();
}

/// @brief Fetch RM address from leader election result or service discovery.
/// @return the RM's ip:port .
/// @throw Exception if can not fetch address from either election result or PSM mode.
String ResourceManagerClient::fetchRMAddress() const
{
    auto context = getContext();
    try
    {
        election_reader->refresh();
        auto leader_info = election_reader->tryGetLeaderInfo();
        if (leader_info.has_value())
        {
            LOG_TRACE(log, "Fetched leader info: {}", leader_info->toDebugString());
            return leader_info->getRPCAddress();
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    auto addr = fetchRMAddressByPSM(context);
    LOG_TRACE(log, "Fetched RM address by PSM: {}", addr);
    return addr;
}

ResourceManagerClient::~ResourceManagerClient()
{
}

void ResourceManagerClient::getVirtualWarehouse(const std::string & vw_name, VirtualWarehouseData & vw_data)
{
    brpc::Controller cntl;
    Protos::GetVirtualWarehouseReq request;
    Protos::GetVirtualWarehouseResp response;
    auto rpc_func = [this, &cntl, &request, &response, &vw_name](std::unique_ptr<Stub> & stub_) {
        request.set_vw_name(vw_name);
        stub_->getVirtualWarehouse(&cntl, &request, &response, nullptr);
        assertController(cntl);
        RPCHelpers::checkResponse(response);
    };

    auto process_response = [&vw_data](Protos::GetVirtualWarehouseResp & response_)
    {
        vw_data.parseFromProto(response_.vw_data());
    };

    callToLeaderWrapper(response, rpc_func, process_response);
}

void ResourceManagerClient::createVirtualWarehouse(
    const std::string & vw_name, const VirtualWarehouseSettings & vw_settings, bool if_not_exists)
{
    brpc::Controller cntl;
    Protos::CreateVirtualWarehouseReq request;
    Protos::CreateVirtualWarehouseResp response;
    auto rpc_func = [this, &cntl, &request, &response, &vw_name, &vw_settings, if_not_exists](std::unique_ptr<Stub> & stub_)
	{
        request.set_vw_name(vw_name);
        vw_settings.fillProto(*request.mutable_vw_settings());
        request.set_if_not_exists(if_not_exists);
        stub_->createVirtualWarehouse(&cntl, &request, &response, nullptr);

        assertController(cntl);
        RPCHelpers::checkResponse(response);
    };

    callToLeaderWrapper(response, rpc_func);
}

void ResourceManagerClient::updateVirtualWarehouse(const std::string & vw_name, const VirtualWarehouseAlterSettings & alter_settings)
{
    brpc::Controller cntl;
    Protos::UpdateVirtualWarehouseReq request;
    Protos::UpdateVirtualWarehouseResp response;
    auto rpc_func = [this, &cntl, &request, &response, &vw_name, &alter_settings](std::unique_ptr<Stub> & stub_)
	{
        request.set_vw_name(vw_name);
        alter_settings.fillProto(*request.mutable_vw_settings());
        LOG_TRACE(log, "update warehouse request : {}", request.ShortDebugString());

        stub_->updateVirtualWarehouse(&cntl, &request, &response, nullptr);

        assertController(cntl);
        RPCHelpers::checkResponse(response);
    };

    callToLeaderWrapper(response, rpc_func);
}

void ResourceManagerClient::dropVirtualWarehouse(const std::string & vw_name, const bool if_exists)
{
    brpc::Controller cntl;
    Protos::DropVirtualWarehouseReq request;
    Protos::DropVirtualWarehouseResp response;
    auto rpc_func = [this, &cntl, &request, &response, &vw_name, if_exists](std::unique_ptr<Stub> & stub_)
	{
        request.set_vw_name(vw_name);
        request.set_if_exists(if_exists);

        stub_->dropVirtualWarehouse(&cntl, &request, &response, nullptr);

        assertController(cntl);
        RPCHelpers::checkResponse(response);
    };

    callToLeaderWrapper(response, rpc_func);
}

void ResourceManagerClient::getAllVirtualWarehouses(std::vector<VirtualWarehouseData> & vw_data_list)
{
    brpc::Controller cntl;
    Protos::GetAllVirtualWarehousesReq request;
    Protos::GetAllVirtualWarehousesResp response;
    auto rpc_func = [this, &cntl, &request, &response](std::unique_ptr<Stub> & stub_)
	{
        stub_->getAllVirtualWarehouses(&cntl, &request, &response, nullptr);

        assertController(cntl);
        RPCHelpers::checkResponse(response);
    };

    auto process_response = [&vw_data_list] (Protos::GetAllVirtualWarehousesResp & response_)
    {
        for (auto & vw_data : response_.vw_data())
        {
            vw_data_list.emplace_back();
            vw_data_list.back().parseFromProto(vw_data);
        }
    };

    callToLeaderWrapper(response, rpc_func, process_response);
}

void ResourceManagerClient::createWorkerGroup(
    [[maybe_unused]] const String & worker_group_id, const String & vw_name, const WorkerGroupData & worker_group_data)
{
    brpc::Controller cntl;
    Protos::CreateWorkerGroupReq request;
    Protos::CreateWorkerGroupResp response;
    auto rpc_func = [this, &cntl, &request, &response, &vw_name, &worker_group_data](std::unique_ptr<Stub> & stub_)
	{
        request.set_if_not_exists(false);
        worker_group_data.fillProto(*request.mutable_worker_group_data(), false, false);
        request.set_vw_name(vw_name);

        stub_->createWorkerGroup(&cntl, &request, &response, nullptr);

        assertController(cntl);
        RPCHelpers::checkResponse(response);
    };

    callToLeaderWrapper(response, rpc_func);
}

void ResourceManagerClient::dropWorkerGroup(const String & worker_group_id, bool if_exists)
{
    brpc::Controller cntl;
    Protos::DropWorkerGroupReq request;
    Protos::DropWorkerGroupResp response;
    auto rpc_func = [this, &cntl, &request, &response, &worker_group_id, if_exists](std::unique_ptr<Stub> & stub_)
	{
        request.set_if_exists(if_exists);
        request.set_worker_group_id(worker_group_id);
        stub_->dropWorkerGroup(&cntl, &request, &response, nullptr);

        assertController(cntl);
        RPCHelpers::checkResponse(response);
    };

    callToLeaderWrapper(response, rpc_func);
}

void ResourceManagerClient::getWorkerGroups(
    const std::string & vw_name,
    std::vector<WorkerGroupData> & groups_data,
    std::optional<VirtualWarehouseSettings> & settings,
    std::atomic<UInt64> & last_settings_timestamp)
{
    brpc::Controller cntl;
    Protos::GetWorkerGroupsReq request;
    Protos::GetWorkerGroupsResp response;
    auto rpc_func = [this, &cntl, &request, &response, &vw_name, &last_settings_timestamp](std::unique_ptr<Stub> & stub_)
	{
        request.set_vw_name(vw_name);
        request.set_last_settings_timestamp(last_settings_timestamp.load());
        stub_->getWorkerGroups(&cntl, &request, &response, nullptr);
        LOG_TRACE(
            getLogger("adaptiveScheduler"),
            "getWorkerGroups response: {}", response.ShortDebugString().c_str());
        assertController(cntl);
        RPCHelpers::checkResponse(response);
    };

    auto process_response = [&groups_data, &settings, &last_settings_timestamp] (Protos::GetWorkerGroupsResp & response_)
    {
        for (auto & worker_group_data : response_.worker_group_data())
        {
            groups_data.emplace_back(WorkerGroupData::createFromProto(worker_group_data));
        }
        if (response_.has_vw_settings())
        {
            settings = VirtualWarehouseSettings{};
            (*settings).parseFromProto(response_.vw_settings());
            last_settings_timestamp = response_.last_settings_timestamp();
        }
    };

    callToLeaderWrapper(response, rpc_func, process_response);
}

std::vector<WorkerGroupData> ResourceManagerClient::getAllWorkerGroups(bool with_metrics)
{
    std::vector<WorkerGroupData> worker_group_data_list;
    brpc::Controller cntl;
    Protos::GetAllWorkerGroupsReq request;
    Protos::GetAllWorkerGroupsResp response;
    auto rpc_func = [this, &cntl, &request, &response, with_metrics](std::unique_ptr<Stub> & stub_)
	{
        request.set_with_metrics(with_metrics);
        stub_->getAllWorkerGroups(&cntl, &request, &response, nullptr);

        assertController(cntl);
        RPCHelpers::checkResponse(response);
    };
    auto process_response = [&worker_group_data_list] (Protos::GetAllWorkerGroupsResp & response_)
    {
        for (auto & worker_group_data : response_.worker_group_data())
        {
            worker_group_data_list.emplace_back();
            worker_group_data_list.back().parseFromProto(worker_group_data);
        }
    };

    callToLeaderWrapper(response, rpc_func, process_response);
    return worker_group_data_list;
}

void ResourceManagerClient::getAllWorkers(std::vector<WorkerNodeResourceData> & data)
{
    brpc::Controller cntl;
    Protos::GetAllWorkersReq request;
    Protos::GetAllWorkersResp response;
    auto rpc_func = [this, &cntl, &request, &response](std::unique_ptr<Stub> & stub_)
	{
        stub_->getAllWorkers(&cntl, &request, &response, nullptr);

        assertController(cntl);
        RPCHelpers::checkResponse(response);
    };
    auto process_response = [&data] (Protos::GetAllWorkersResp response_)
    {
        for (auto & work_data : response_.worker_data())
        {
            data.emplace_back(WorkerNodeResourceData::createFromProto(work_data));
        }
    };

    callToLeaderWrapper(response, rpc_func, process_response);
}

bool ResourceManagerClient::reportResourceUsage(const WorkerNodeResourceData & data)
{
    bool res{false};
    brpc::Controller cntl;
    Protos::SyncResourceInfoReq request;
    Protos::SyncResourceInfoResp response;
    auto rpc_func = [this, &cntl, &request, &response, &data](std::unique_ptr<Stub> & stub_)
	{
        data.fillProto(*request.mutable_resource_data());

        stub_->syncResourceUsage(&cntl, &request, &response, nullptr);

        assertController(cntl);
        RPCHelpers::checkResponse(response);
    };

    auto process_response = [&res] (Protos::SyncResourceInfoResp & response_)
    {
        res = response_.success();
    };

    callToLeaderWrapper(response, rpc_func, process_response);
    return res;
}

void ResourceManagerClient::registerWorker(const WorkerNodeResourceData & data)
{
    brpc::Controller cntl;
    Protos::RegisterWorkerNodeReq request;
    Protos::RegisterWorkerNodeResp response;
    auto rpc_func = [this, &cntl, &request, &response, &data](std::unique_ptr<Stub> & stub_)
	{
        data.fillProto(*request.mutable_resource_data());

        stub_->registerWorkerNode(&cntl, &request, &response, nullptr);

        assertController(cntl);
        RPCHelpers::checkResponse(response);
    };

    callToLeaderWrapper(response, rpc_func);
}

void ResourceManagerClient::removeWorker(const String & worker_id, const String & vw_name, const String & worker_group_id)
{
    brpc::Controller cntl;
    Protos::RemoveWorkerNodeReq request;
    Protos::RemoveWorkerNodeResp response;
    auto rpc_func = [this, &cntl, &request, &response, &worker_id, &vw_name, &worker_group_id](std::unique_ptr<Stub> & stub_)
	{
        request.set_worker_id(worker_id);
        request.set_vw_name(vw_name);
        request.set_worker_group_id(worker_group_id);

        stub_->removeWorkerNode(&cntl, &request, &response, nullptr);

        assertController(cntl);
        RPCHelpers::checkResponse(response);
    };

    callToLeaderWrapper(response, rpc_func);
}

AggQueryQueueMap ResourceManagerClient::syncQueueDetails(VWQueryQueueMap vw_query_queue_map
                                                         , std::vector<String> * deleted_vw_list)
{
    AggQueryQueueMap res;
    brpc::Controller cntl;
    Protos::SyncQueueDetailsReq request;
    Protos::SyncQueueDetailsResp response;
    auto rpc_func = [this, &cntl, &request, &response, &vw_query_queue_map](const std::unique_ptr<Stub> & stub_)
    {
        for (const auto & [key, server_query_queue_info] : vw_query_queue_map)
        {
            Protos::QueryQueueInfo protobuf_entry;
            server_query_queue_info.fillProto(protobuf_entry);
            (*request.mutable_server_query_queue_map())[key] = protobuf_entry;
        }
        stub_->syncQueueDetails(&cntl, &request, &response, nullptr);
        assertController(cntl);
        RPCHelpers::checkResponse(response);
    };
    auto process_response = [&deleted_vw_list, &res] (Protos::SyncQueueDetailsResp & response_)
    {
        deleted_vw_list->insert(deleted_vw_list->end(), response_.deleted_vws().begin(), response_.deleted_vws().end());
        for (const auto & [key, proto_agg_query_queue_info] : response_.agg_query_queue_map())
        {
            QueryQueueInfo agg_query_queue_info;
            agg_query_queue_info.parseFromProto(proto_agg_query_queue_info);
            res[key] = agg_query_queue_info;
        }
    };
    callToLeaderWrapper(response, rpc_func, process_response);
    return res;
}

void ResourceManagerClient::sendResourceRequest(const Protos::SendResourceRequestReq & request)
{
    brpc::Controller cntl;
    Protos::SendResourceRequestResp response;
    auto rpc_func = [this, &cntl, &request, &response](std::unique_ptr<Stub> & stub_) {
        stub_->sendResourceRequest(&cntl, &request, &response, nullptr);

        assertController(cntl);
        RPCHelpers::checkResponse(response);
    };

    callToLeaderWrapper(response, rpc_func);
}
}
