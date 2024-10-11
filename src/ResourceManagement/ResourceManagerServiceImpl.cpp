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

#include <ResourceManagement/ResourceManagerServiceImpl.h>

#include <Protos/RPCHelpers.h>
#include <ResourceManagement/ResourceManagerController.h>
#include <ResourceManagement/ResourceTracker.h>
#include <ResourceManagement/VirtualWarehouseManager.h>
#include <ResourceManagement/WorkerGroupManager.h>
#include <ResourceManagement/ResourceManagerController.h>
#include <ResourceManagement/ElectionController.h>

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <ResourceManagement/CommonData.h>


namespace DB::ErrorCodes
{
    extern const int WORKER_GROUP_NOT_FOUND;
    extern const int RESOURCE_MANAGER_NO_AVAILABLE_WORKER;
    extern const int RESOURCE_MANAGER_NO_AVAILABLE_WORKER_GROUP;
}

namespace DB::ResourceManagement
{
ResourceManagerServiceImpl::ResourceManagerServiceImpl(ResourceManagerController & rm_controller_)
    : rm_controller(rm_controller_)
    , vw_manager(rm_controller.getVirtualWarehouseManager())
    , group_manager(rm_controller.getWorkerGroupManager())
{
}

template <typename T>
bool ResourceManagerServiceImpl::checkForLeader(T & response)
{
    auto & election_controller = rm_controller.getElectionController();
    auto is_leader = election_controller.isLeader();
    response->set_is_leader(is_leader);
    return is_leader;
}

void ResourceManagerServiceImpl::syncResourceUsage(
    [[maybe_unused]] ::google::protobuf::RpcController * controller,
    [[maybe_unused]] const ::DB::Protos::SyncResourceInfoReq * request,
    [[maybe_unused]] ::DB::Protos::SyncResourceInfoResp * response,
    [[maybe_unused]] ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);

    try
    {
        if (!checkForLeader(response))
            return;

        auto entry = WorkerNodeResourceData::createFromProto(request->resource_data());

        LOG_TRACE(log, "Worker resource report: {}", entry.toDebugString());

        auto & resource_tracker = rm_controller.getResourceTracker();
        auto success = resource_tracker.updateNode(entry);
        response->set_success(success);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}


void ResourceManagerServiceImpl::registerWorkerNode(
    [[maybe_unused]] ::google::protobuf::RpcController * controller,
    const ::DB::Protos::RegisterWorkerNodeReq * request,
    ::DB::Protos::RegisterWorkerNodeResp * response,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);

    try
    {
        if (!checkForLeader(response))
            return;

        auto data = WorkerNodeResourceData::createFromProto(request->resource_data());
        LOG_DEBUG(log, "Register worker {} - {}", data.host_ports.toDebugString(), data.toDebugString());
        rm_controller.registerWorkerNode(data);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

void ResourceManagerServiceImpl::removeWorkerNode(
    [[maybe_unused]] ::google::protobuf::RpcController * controller,
    const ::DB::Protos::RemoveWorkerNodeReq * request,
    ::DB::Protos::RemoveWorkerNodeResp * response,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);

    try
    {
        auto worker_id = request->worker_id();

        if (!checkForLeader(response))
            return;

        LOG_DEBUG(log, "Remove worker {}", worker_id);
        const auto & vw_name = request->vw_name();
        const auto & worker_group_id = request->worker_group_id();
        rm_controller.removeWorkerNode(worker_id, vw_name, worker_group_id);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

void ResourceManagerServiceImpl::createVirtualWarehouse(
    [[maybe_unused]] ::google::protobuf::RpcController * controller,
    [[maybe_unused]] const ::DB::Protos::CreateVirtualWarehouseReq * request,
    [[maybe_unused]] ::DB::Protos::CreateVirtualWarehouseResp * response,
    [[maybe_unused]] ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);

    try
    {
        if (!checkForLeader(response))
            return;

        const auto & vw_name = request->vw_name();
        auto vw_settings = VirtualWarehouseSettings::createFromProto(request->vw_settings());

        vw_manager.createVirtualWarehouse(vw_name, vw_settings, request->if_not_exists());
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

void ResourceManagerServiceImpl::updateVirtualWarehouse(
    [[maybe_unused]] ::google::protobuf::RpcController * controller,
    [[maybe_unused]] const ::DB::Protos::UpdateVirtualWarehouseReq * request,
    [[maybe_unused]] ::DB::Protos::UpdateVirtualWarehouseResp * response,
    [[maybe_unused]] ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);

    try
    {
        if (!checkForLeader(response))
            return;
        LOG_TRACE(log, "get update req : {}", request->ShortDebugString());
        const auto & vw_name = request->vw_name();
        auto vw_settings = VirtualWarehouseAlterSettings::createFromProto(request->vw_settings());

        vw_manager.alterVirtualWarehouse(vw_name, vw_settings);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

void ResourceManagerServiceImpl::getVirtualWarehouse(
    [[maybe_unused]] ::google::protobuf::RpcController * controller,
    [[maybe_unused]] const ::DB::Protos::GetVirtualWarehouseReq * request,
    [[maybe_unused]] ::DB::Protos::GetVirtualWarehouseResp * response,
    [[maybe_unused]] ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);

    try
    {
        if (!checkForLeader(response))
            return;

        auto vw = vw_manager.getVirtualWarehouse(request->vw_name());
        auto vw_data = vw->getData();
        vw_data.fillProto(*response->mutable_vw_data());
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

void ResourceManagerServiceImpl::dropVirtualWarehouse(
    [[maybe_unused]] ::google::protobuf::RpcController * controller,
    [[maybe_unused]] const ::DB::Protos::DropVirtualWarehouseReq * request,
    [[maybe_unused]] ::DB::Protos::DropVirtualWarehouseResp * response,
    [[maybe_unused]] ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);

    try
    {
        if (!checkForLeader(response))
            return;

        vw_manager.dropVirtualWarehouse(request->vw_name(), request->if_exists());
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

void ResourceManagerServiceImpl::getAllVirtualWarehouses(
    [[maybe_unused]] ::google::protobuf::RpcController * controller,
    [[maybe_unused]] const ::DB::Protos::GetAllVirtualWarehousesReq * request,
    [[maybe_unused]] ::DB::Protos::GetAllVirtualWarehousesResp * response,
    [[maybe_unused]] ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);

    try
    {
        if (!checkForLeader(response))
            return;

        auto vws = vw_manager.getAllVirtualWarehouses();
        for (auto & [name, vw] : vws)
            vw->getData().fillProto(*response->add_vw_data());
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

void ResourceManagerServiceImpl::getAllWorkers(
    [[maybe_unused]] ::google::protobuf::RpcController * controller,
    [[maybe_unused]] const ::DB::Protos::GetAllWorkersReq * request,
    [[maybe_unused]] ::DB::Protos::GetAllWorkersResp * response,
    [[maybe_unused]] ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);

    try
    {
        if (!checkForLeader(response))
            return;

        auto & resource_tracker = rm_controller.getResourceTracker();
        auto workers = resource_tracker.getAllWorkers();
        for (auto & [name, node] : workers)
        {
            node->fillProto(*response->add_worker_data());
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

void ResourceManagerServiceImpl::createWorkerGroup(
    [[maybe_unused]] ::google::protobuf::RpcController * controller,
    [[maybe_unused]] const ::DB::Protos::CreateWorkerGroupReq * request,
    [[maybe_unused]] ::DB::Protos::CreateWorkerGroupResp * response,
    [[maybe_unused]] ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);

    try
    {
        if (!checkForLeader(response))
            return;

        auto worker_group_data = WorkerGroupData::createFromProto(request->worker_group_data());

        rm_controller.createWorkerGroup(worker_group_data.id, request->vw_name(), worker_group_data);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

void ResourceManagerServiceImpl::dropWorkerGroup(
    [[maybe_unused]] ::google::protobuf::RpcController * controller,
    [[maybe_unused]] const ::DB::Protos::DropWorkerGroupReq * request,
    [[maybe_unused]] ::DB::Protos::DropWorkerGroupResp * response,
    [[maybe_unused]] ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);

    try
    {
        if (!checkForLeader(response))
            return;

        LOG_TRACE(log, "Drop worker group: {}", request->worker_group_id());
        rm_controller.dropWorkerGroup(request->worker_group_id(), request->if_exists());
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

void ResourceManagerServiceImpl::getWorkerGroups(
    [[maybe_unused]] ::google::protobuf::RpcController * controller,
    [[maybe_unused]] const ::DB::Protos::GetWorkerGroupsReq * request,
    [[maybe_unused]] ::DB::Protos::GetWorkerGroupsResp * response,
    [[maybe_unused]] ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);

    try
    {
        if (!checkForLeader(response))
            return;

        auto vw = vw_manager.getVirtualWarehouse(request->vw_name());
        auto groups = vw->getAllWorkerGroups();
        LOG_TRACE(log, "Got {} worker groups of {}", groups.size(), request->vw_name());
        for (const auto & group : groups)
            group->getData(/*with_metrics*/true, /*only_running_state*/true)
                .fillProto(*response->add_worker_group_data(), /*with_host_ports*/true, /*with_metrics*/true);

        auto timestamp = vw->getLastSettingsTimestamp();
        if (timestamp != request->last_settings_timestamp())
        {
            vw->getSettings().fillProto(*response->mutable_vw_settings());
            response->set_last_settings_timestamp(timestamp);
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

void ResourceManagerServiceImpl::getAllWorkerGroups(
    [[maybe_unused]] ::google::protobuf::RpcController * controller,
    [[maybe_unused]] const ::DB::Protos::GetAllWorkerGroupsReq * request,
    [[maybe_unused]] ::DB::Protos::GetAllWorkerGroupsResp * response,
    [[maybe_unused]] ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);

    try
    {
        if (!checkForLeader(response))
            return;

        auto groups = group_manager.getAllWorkerGroups();
        auto with_metrics = request->with_metrics();
        for (auto & [_, group] : groups)
        {
            group->getData(/*with_metrics*/with_metrics, /*only_running_state*/true)
                .fillProto(*response->add_worker_group_data(), /*with_host_ports*/false, with_metrics);
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

void ResourceManagerServiceImpl::syncQueueDetails(
    ::google::protobuf::RpcController * controller,
    const ::DB::Protos::SyncQueueDetailsReq * request,
    ::DB::Protos::SyncQueueDetailsResp * response,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    try
    {
        if (!checkForLeader(response))
            return;
        brpc::Controller * cntl =  static_cast<brpc::Controller*>(controller);
        String server_hostport = butil::endpoint2str(cntl->remote_side()).c_str();
        auto proto_server_query_queue_map = request->server_query_queue_map();
        ServerQueryQueueMap server_query_queue_map;
        UInt64 time_now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()
                                                                            .time_since_epoch()).count();
        for (const auto & [name, proto] : proto_server_query_queue_map)
        {
            QueryQueueInfo entry;
            entry.parseFromProto(proto);
            entry.last_sync = time_now;
            server_query_queue_map[name] = entry;
        }
        LOG_DEBUG(log, "Received vw queue update from {}", server_hostport);
        std::vector<String> deleted_vw_list;
        vw_manager.updateQueryQueueMap(server_hostport, server_query_queue_map, deleted_vw_list);
        auto agg_query_queue_map = vw_manager.getAggQueryQueueMap();
        for (const auto & [key, agg_query_queue_info] : agg_query_queue_map)
        {
            Protos::QueryQueueInfo protobuf_entry;
            agg_query_queue_info.fillProto(protobuf_entry);
            (*response->mutable_agg_query_queue_map())[key] = protobuf_entry;
        }
        *response->mutable_deleted_vws() = {deleted_vw_list.begin(), deleted_vw_list.end()};
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

void ResourceManagerServiceImpl::sendResourceRequest(
    [[maybe_unused]] ::google::protobuf::RpcController * controller,
    const ::DB::Protos::SendResourceRequestReq * request,
    ::DB::Protos::SendResourceRequestResp * response,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    try
    {
        if (!checkForLeader(response))
            return;
        if (!rm_controller.getResourceScheduler().queue.push(*request))
            response->set_exception("enqueue failed");
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}
}
