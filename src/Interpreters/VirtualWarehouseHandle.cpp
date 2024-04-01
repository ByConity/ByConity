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

#include <Catalog/Catalog.h>
#include <Interpreters/VirtualWarehouseHandle.h>
#include <Interpreters/WorkerGroupHandle.h>
#include <Interpreters/WorkerStatusManager.h>
#include <ResourceManagement/ResourceManagerClient.h>
#include <ServiceDiscovery/IServiceDiscovery.h>
#include <Common/Exception.h>
#include <Common/thread_local_rng.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int VIRTUAL_WAREHOUSE_NOT_INITIALIZED;
    extern const int RESOURCE_MANAGER_LOCAL_NO_AVAILABLE_WORKER_GROUP;
    extern const int RESOURCE_MANAGER_WRONG_VW_SCHEDULE_ALGO;
}

VirtualWarehouseHandleImpl::VirtualWarehouseHandleImpl(
    VirtualWarehouseHandleSource source_,
    std::string name_,
    UUID uuid_,
    const ContextPtr global_context_,
    const VirtualWarehouseSettings & settings_)
    : WithContext(global_context_)
    , source(source_)
    , name(std::move(name_))
    , uuid(uuid_)
    , settings(settings_)
    , log(&Poco::Logger::get(name + " (VirtualWarehouseHandle)"))
{
    tryUpdateWorkerGroups(ForceUpdate);
}

VirtualWarehouseHandleImpl::VirtualWarehouseHandleImpl(
    VirtualWarehouseHandleSource source_, const VirtualWarehouseData & vw_data, const ContextPtr global_context_)
    : VirtualWarehouseHandleImpl(source_, vw_data.name, vw_data.uuid, global_context_, vw_data.settings)
{
    LOG_DEBUG(log, "create virtual warehouse {}", name);
    queue_manager.updateQueue(vw_data.settings.queue_datas);
}

bool VirtualWarehouseHandleImpl::empty(UpdateMode update_mode)
{
    tryUpdateWorkerGroups(update_mode);

    std::lock_guard lock(state_mutex);
    return worker_groups.empty();
}

VirtualWarehouseHandleImpl::Container VirtualWarehouseHandleImpl::getAll(UpdateMode update_mode)
{
    tryUpdateWorkerGroups(update_mode);

    std::lock_guard lock(state_mutex);
    return worker_groups;
}

size_t VirtualWarehouseHandleImpl::getNumWorkers(UpdateMode update_mode)
{
    tryUpdateWorkerGroups(update_mode);

    size_t res = 0;
    std::lock_guard lock(state_mutex);
    for (auto const & [_, worker_group] : worker_groups)
        res += worker_group->size();
    return res;
}

WorkerGroupHandle VirtualWarehouseHandleImpl::getWorkerGroup(const String & worker_group_id, UpdateMode update_mode)
{
    tryUpdateWorkerGroups(update_mode);

    std::lock_guard lock(state_mutex);

    auto it = worker_groups.find(worker_group_id);
    if (it == worker_groups.end())
        throw Exception("There is no worker group with id " + worker_group_id + " in VW " + name, ErrorCodes::LOGICAL_ERROR);
    return it->second;
}

void VirtualWarehouseHandleImpl::updateWorkerStatusFromRM(const std::vector<WorkerGroupData> & groups_data)
{
    auto worker_status_manager = getContext()->getWorkerStatusManager();
    auto cannot_update_workers = worker_status_manager->getWorkersCannotUpdateFromRM();
    if (log->trace())
    {
        LOG_TRACE(log, "adaptive updateWorkerStatusFromRM");
        for (const auto & [id, _] : cannot_update_workers)
            LOG_TRACE(log, "adaptive cannot update worker : {}", id.ToString());
    }
    for (const auto & group_data : groups_data)
    {
        for (const auto & worker_resource_data : group_data.worker_node_resource_vec)
        {
            auto worker_id = WorkerStatusManager::getWorkerId(group_data.vw_name, group_data.id, worker_resource_data.id);
            if (!cannot_update_workers.count(worker_id))
            {
                LOG_TRACE(log, "update worker {} from rm", worker_id.ToString());
                Protos::WorkerNodeResourceData resource_info;
                worker_resource_data.fillProto(resource_info);
                worker_status_manager->updateWorkerNode(resource_info, WorkerStatusManager::UpdateSource::ComeFromRM);
            }
        }
    }
}

WorkerGroupHandle VirtualWarehouseHandleImpl::pickWorkerGroup(
    [[maybe_unused]] VWScheduleAlgo algo, [[maybe_unused]] const Requirement & requirement, [[maybe_unused]] UpdateMode update_mode)
{
    return randomWorkerGroup();
}

void VirtualWarehouseHandleImpl::tryUpdateWorkerGroups(UpdateMode update_mode)
{
    if (update_mode == NoUpdate)
        return;

    UInt64 current_ns = clock_gettime_ns(CLOCK_MONOTONIC_COARSE);
    UInt64 the_last_update_time_ns = last_update_time_ns.load();

    if (current_ns >= the_last_update_time_ns + force_update_interval_ns)
    {
        LOG_TRACE(log, "The worker_groups is not updated for {} ms. Will use ForceUpdate mode.", force_update_interval_ns / 1000000);
        update_mode = ForceUpdate;
    }

    /// TryUpdate mode - one of the query thread will do updateWorkerGroups.
    if (update_mode != ForceUpdate)
    {
        if (current_ns < the_last_update_time_ns + try_update_interval_ns)
            return;
        if (!last_update_time_ns.compare_exchange_strong(the_last_update_time_ns, current_ns))
            return;
    }

    // Try to retrieve worker group info from RM first
    auto success = updateWorkerGroupsFromRM();

    if (!success)
        success = updateWorkerGroupsFromPSM();

    if (!success || worker_groups.empty())
        LOG_WARNING(log, "Failed to update worker groups for VW:{}", name);

    last_update_time_ns.store(current_ns);
}


bool VirtualWarehouseHandleImpl::addWorkerGroupImpl(const WorkerGroupHandle & worker_group, const std::lock_guard<std::mutex> & /*lock*/)
{
    auto [_, success] = worker_groups.try_emplace(worker_group->getID(), worker_group);
    if (!success)
        LOG_DEBUG(log, "Worker group {} already exists in {}.", worker_group->getQualifiedName(), name);
    return success;
}

bool VirtualWarehouseHandleImpl::addWorkerGroup(const WorkerGroupHandle & worker_group)
{
    std::lock_guard lock(state_mutex);
    return addWorkerGroupImpl(worker_group, lock);
}

bool VirtualWarehouseHandleImpl::updateWorkerGroupsFromRM()
{
    try
    {
        auto rm_client = getContext()->getResourceManagerClient();
        if (!rm_client)
        {
            LOG_TRACE(log, "The client of ResourceManagement is not initialized");
            return false;
        }

        std::vector<WorkerGroupData> groups_data;
        std::optional<VirtualWarehouseSettings> new_settings;
        rm_client->getWorkerGroups(name, groups_data, new_settings, last_settings_timestamp);
        if (groups_data.empty())
        {
            LOG_DEBUG(log, "No worker group found in VW {} from RM", name);
            return false;
        }

        if (new_settings)
            updateSettings(*new_settings);
        updateWorkerStatusFromRM(groups_data);
        std::lock_guard lock(state_mutex);

        Container old_groups;
        old_groups.swap(worker_groups);

        for (auto & group_data : groups_data)
        {
            if (group_data.num_workers == 0)
            {
                LOG_WARNING(log, "Get an empty worker group from RM:{}", group_data.id);
                continue;
            }

            if (auto it = old_groups.find(group_data.id); it == old_groups.end()) /// new worker group
                worker_groups.try_emplace(group_data.id, std::make_shared<WorkerGroupHandleImpl>(group_data, getContext()));
            else if (!it->second->isSame(group_data)) /// replace with the new one because of diff
                worker_groups.try_emplace(group_data.id, std::make_shared<WorkerGroupHandleImpl>(group_data, getContext()));
            else /// reuse the old worker group handle and update metrics.
            {
                it->second->setMetrics(group_data.metrics);
                worker_groups.try_emplace(group_data.id, it->second);
            }
        }

        if (worker_groups.empty())
        {
            LOG_WARNING(log, "Get an empty VW from RM:{}", name);
            return false;
        }

        return true;
    }
    catch (...)
    {
        tryLogDebugCurrentException(__PRETTY_FUNCTION__);
        return false;
    }
}

void VirtualWarehouseHandleImpl::updateSettings(const VirtualWarehouseSettings & new_settings)
{
    getQueueManager().updateQueue(new_settings.queue_datas);
}

void VirtualWarehouseHandleImpl::updateWorkerStatusFromPSM(
    const IServiceDiscovery::WorkerGroupMap & groups_data, const std::string & vw_name)
{
    auto worker_status_manager = getContext()->getWorkerStatusManager();
    auto cannot_update_workers = worker_status_manager->getWorkersCannotUpdateFromRM();
    if (log->trace())
    {
        for (const auto & [id, _] : cannot_update_workers)
            LOG_TRACE(log, "adaptive cannot update unhealth worker : {}", id.ToString());
    }

    for (const auto & [wg_name, host_ports_vec] : groups_data)
    {
        for (const auto & host_ports : host_ports_vec)
        {
            auto worker_id = WorkerStatusManager::getWorkerId(vw_name, wg_name, host_ports.id);
            if (!cannot_update_workers.count(worker_id))
            {
                worker_status_manager->restoreWorkerNode(worker_id);
            }
        }
    }
}

bool VirtualWarehouseHandleImpl::updateWorkerGroupsFromPSM()
{
    try
    {
        auto sd = getContext()->getServiceDiscoveryClient();
        auto psm = getContext()->getVirtualWarehousePSM();

        auto groups_map = sd->lookupWorkerGroupsInVW(psm, name);
        if (groups_map.empty())
        {
            LOG_DEBUG(log, "No worker group found in VW {} from PSM {}", name, psm);
            return false;
        }
        updateWorkerStatusFromPSM(groups_map, name);
        std::lock_guard lock(state_mutex);

        Container old_groups;
        old_groups.swap(worker_groups);

        for (auto & [group_tag, hosts] : groups_map)
        {
            auto group_id = group_tag + PSM_WORKER_GROUP_SUFFIX;
            if (auto it = old_groups.find(group_id); it == old_groups.end()) /// new worker group
                worker_groups.try_emplace(
                    group_id, std::make_shared<WorkerGroupHandleImpl>(group_id, WorkerGroupHandleSource::PSM, name, hosts, getContext()));
            else if (!HostWithPorts::isExactlySameVec(
                         it->second->getHostWithPortsVec(), hosts)) /// replace with the new one because of diff
                worker_groups.try_emplace(
                    group_id, std::make_shared<WorkerGroupHandleImpl>(group_id, WorkerGroupHandleSource::PSM, name, hosts, getContext()));
            else /// reuse the old one
                worker_groups.try_emplace(group_id, it->second);
        }

        return true;
    }
    catch (...)
    {
        tryLogDebugCurrentException(__PRETTY_FUNCTION__);
        return false;
    }
}

/// pickLocally stage 1: filter worker groups by requirement.
void VirtualWarehouseHandleImpl::filterGroup(const Requirement & requirement, std::vector<WorkerGroupAndMetrics> & res)
{
    /// TODO: (zuochuang.zema) read-write lock.
    std::lock_guard lock(state_mutex);
    for (const auto & [_, group] : worker_groups)
    {
        if (!group->empty())
        {
            if (auto metrics = group->getMetrics(); metrics.available(requirement))
                res.emplace_back(group, metrics);
        }
    }
}

/// pickLocally stage 2: select one group by algo.
WorkerGroupHandle VirtualWarehouseHandleImpl::selectGroup(
    [[maybe_unused]] const VWScheduleAlgo & algo, [[maybe_unused]] std::vector<WorkerGroupAndMetrics> & available_groups)
{
    return available_groups[0].first;

    // if (available_groups.size() == 1)
    //     return available_groups[0].first;

    // auto comparator = cmp_group_cpu;
    // switch (algo)
    // {
    //     case VWScheduleAlgo::Unknown:
    //     case VWScheduleAlgo::Random:
    //     {
    //         std::uniform_int_distribution dist;
    //         auto index = dist(thread_local_rng) % available_groups.size();
    //         return available_groups[index].first;
    //     }

    //     case VWScheduleAlgo::LocalRoundRobin:
    //     {
    //         size_t index = pick_group_sequence.fetch_add(1, std::memory_order_relaxed) % available_groups.size();
    //         return available_groups[index].first;
    //     }
    //     case VWScheduleAlgo::LocalLowCpu:
    //         break;
    //     case VWScheduleAlgo::LocalLowMem:
    //         comparator = cmp_group_mem;
    //         break;
    //     default:
    //         const auto & s = std::string(ResourceManagement::toString(algo));
    //         throw Exception("Wrong vw_schedule_algo for local scheduler: " + s, ErrorCodes::RESOURCE_MANAGER_WRONG_VW_SCHEDULE_ALGO);
    // }

    // /// Choose from the two lowest CPU worker groups but not the lowest one as the metrics on server may be outdated.
    // if (available_groups.size() >= 3)
    // {
    //     std::partial_sort(available_groups.begin(), available_groups.begin() + 2, available_groups.end(), comparator);
    //     std::uniform_int_distribution dist;
    //     auto index = (dist(thread_local_rng) % 3) >> 1; /// prefer index 0 than 1.
    //     return available_groups[index].first;
    // }
    // else
    // {
    //     auto it = std::min_element(available_groups.begin(), available_groups.end(), comparator);
    //     return it->first;
    // }
}

/// Picking a worker group from local cache. Two stages:
/// - 1. filter worker groups by requirement.
/// - 2. select one group by algo.
WorkerGroupHandle
VirtualWarehouseHandleImpl::pickLocally([[maybe_unused]] const VWScheduleAlgo & algo, [[maybe_unused]] const Requirement & requirement)
{
    return randomWorkerGroup();
    /// TODO: (zuochuang.zema) refactor after multi worker groups is enabled.

    //     std::vector<WorkerGroupAndMetrics> available_groups;
    //     filterGroup(requirement, available_groups);
    //     if (available_groups.empty())
    //     {
    //         LOG_WARNING(log, "No available worker groups for requirement: {}, choose one randomly.", requirement.toDebugString());
    //         return randomWorkerGroup();
    //     }
    //     return selectGroup(algo, available_groups);
}

/// Get a worker from the VW using a random strategy.
CnchWorkerClientPtr VirtualWarehouseHandleImpl::getWorker()
{
    auto wg_handle = randomWorkerGroup();
    return wg_handle->getWorkerClient();
}

CnchWorkerClientPtr VirtualWarehouseHandleImpl::getWorkerByHash(const String & key)
{
    tryUpdateWorkerGroups(TryUpdate);
    /// TODO: Should we expand the worker list first?
    UInt64 val = std::hash<String>{}(key);
    std::lock_guard lock(state_mutex);
    auto wg_index = val % worker_groups.size();
    auto & group = std::next(worker_groups.begin(), wg_index)->second;
    return group->getWorkerClient(val, false).second;
}

CnchWorkerClientPtr VirtualWarehouseHandleImpl::getWorkerByHostWithPorts(const HostWithPorts & host_ports)
{
    auto wg_handle = randomWorkerGroup();
    return wg_handle->getWorkerClient(host_ports);
}

std::vector<CnchWorkerClientPtr> VirtualWarehouseHandleImpl::getAllWorkers()
{
    tryUpdateWorkerGroups(UpdateMode::TryUpdate);

    std::vector<CnchWorkerClientPtr> res;
    std::lock_guard lock(state_mutex);
    for (const auto & [_, wg_handle] : worker_groups)
    {
        const auto & workers = wg_handle->getWorkerClients();
        res.insert(res.end(), workers.begin(), workers.end());
    }
    return res;
}

WorkerGroupHandle VirtualWarehouseHandleImpl::randomWorkerGroup(UpdateMode mode)
{
    tryUpdateWorkerGroups(mode);

    std::uniform_int_distribution dist;

    {
        std::lock_guard lock(state_mutex);
        if (auto size = worker_groups.size())
        {
            auto begin = dist(thread_local_rng) % size;
            for (size_t i = 0; i < size; i++)
            {
                auto index = (begin + i) % size;
                const auto & group = std::next(worker_groups.begin(), index)->second;
                if (!group->getHostWithPortsVec().empty())
                    return group;
            }
        }
    }

    throw Exception("No local available worker group for " + name, ErrorCodes::RESOURCE_MANAGER_LOCAL_NO_AVAILABLE_WORKER_GROUP);
}

std::optional<HostWithPorts> VirtualWarehouseHandleImpl::tryPickWorkerFromRM(VWScheduleAlgo algo, const Requirement & requirement)
{
    if (auto rm_client = getContext()->getResourceManagerClient())
    {
        try
        {
            return rm_client->pickWorker(name, algo, requirement);
        }
        catch (const Exception & e)
        {
            LOG_ERROR(log, "Failed to pick a worker in vw: {} from RM: {}", name, e.displayText());
        }
    }
    return {};
}

CnchWorkerClientPtr VirtualWarehouseHandleImpl::pickWorker(const String & worker_group_id, bool skip_busy_worker)
{
    if (!worker_group_id.empty())
        return getWorkerGroup(worker_group_id)->getWorkerClient(skip_busy_worker);

    return randomWorkerGroup()->getWorkerClient(skip_busy_worker);
}

std::pair<UInt64, CnchWorkerClientPtr>
VirtualWarehouseHandleImpl::pickWorker(const String & worker_group_id, UInt64 sequence, bool skip_busy_worker)
{
    if (!worker_group_id.empty())
        return getWorkerGroup(worker_group_id)->getWorkerClient(sequence, skip_busy_worker);

    return randomWorkerGroup()->getWorkerClient(sequence, skip_busy_worker);
}

}
