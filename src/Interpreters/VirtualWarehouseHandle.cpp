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

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>
#include <string>
#include <algorithm>
#include <Catalog/Catalog.h>
#include <Interpreters/VirtualWarehouseHandle.h>
#include <Interpreters/WorkerGroupHandle.h>
#include <Interpreters/WorkerStatusManager.h>
#include <ResourceManagement/ResourceManagerClient.h>
#include <ServiceDiscovery/IServiceDiscovery.h>
#include <Common/Exception.h>
#include <Common/thread_local_rng.h>
#include <Core/SettingsEnums.h>
#include <Interpreters/Context.h>
#include <ResourceManagement/CommonData.h>
#include <boost/lexical_cast.hpp>
#include <boost/lexical_cast/bad_lexical_cast.hpp>
#include <consistent-hashing/consistent_hashing.h>

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
    , log(getLogger(name + " (VirtualWarehouseHandle)"))
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
    for (const auto & group_data : groups_data)
    {
        if (!worker_status_managers.contains(group_data.id))
        {
            worker_status_managers[group_data.id] = std::make_shared<WorkerStatusManager>(getContext());
        }

        auto current_wg_status = worker_status_managers[group_data.id];
        current_wg_status->recommended_concurrent_query_limit.store(settings.recommended_concurrent_query_limit, std::memory_order_relaxed);
        current_wg_status->health_worker_cpu_usage_threshold.store(settings.health_worker_cpu_usage_threshold, std::memory_order_relaxed);
        current_wg_status->circuit_breaker_open_to_halfopen_wait_seconds.store(settings.circuit_breaker_open_to_halfopen_wait_seconds, std::memory_order_relaxed);
        current_wg_status->unhealth_worker_recheck_wait_seconds.store(settings.unhealth_worker_recheck_wait_seconds, std::memory_order_relaxed);
        current_wg_status->circuit_breaker_open_error_threshold.store(settings.circuit_breaker_open_error_threshold, std::memory_order_relaxed);
        for (const auto & worker_resource_data : group_data.worker_node_resource_vec)
        {
            auto worker_id = WorkerStatusManager::getWorkerId(group_data.vw_name, group_data.id, worker_resource_data.id);
            LOG_TRACE(log, "update worker {} from rm", worker_id.toString());
            Protos::WorkerNodeResourceData resource_info;
            worker_resource_data.fillProto(resource_info);
            current_wg_status->updateWorkerNode(resource_info, WorkerStatusManager::UpdateSource::ComeFromRM);
        }
    }
}

WorkerGroupHandle VirtualWarehouseHandleImpl::pickWorkerGroup(
    [[maybe_unused]] VWScheduleAlgo algo,
    bool use_router,
    VWLoadBalancing load_balance,
    [[maybe_unused]] const Requirement & requirement,
    [[maybe_unused]] UpdateMode update_mode)
{
    LOG_DEBUG(log, "pick worker group, use router [{}], load_balance[{}]", use_router, load_balance);
    if (use_router)
    {
        tryUpdateWorkerGroups(update_mode);
        std::unique_lock<std::mutex> lock(state_mutex);

        if (!priority_groups.empty() && (load_balance == VWLoadBalancing::IN_ORDER || load_balance == VWLoadBalancing::REVERSE_ORDER))
        {
            static std::atomic<uint64_t> round_robin_count{0};

            size_t target_index = load_balance == VWLoadBalancing::IN_ORDER ? 0 : priority_groups.size() - 1;
            if (!priority_groups[target_index].second.empty())
                return priority_groups[target_index].second[round_robin_count++ % priority_groups[target_index].second.size()];
        }
    }

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
        std::lock_guard lock(state_mutex);

        updateWorkerStatusFromRM(groups_data);
        Container old_groups;
        old_groups.swap(worker_groups);

        std::optional<size_t> composite_index;

        for (size_t i = 0; i < groups_data.size(); i++)
        {
            // Only use first Composite WG to complete
            const auto & group_data = groups_data[i];
            if (group_data.num_workers > 0 && group_data.type == WorkerGroupType::Composite && !composite_index)
                composite_index = i;
        }

        for (auto & group_data : groups_data)
        {
            if (group_data.num_workers == 0 || group_data.type == WorkerGroupType::Composite)
            {
                LOG_WARNING(log, "Get an empty worker group from RM:{}", group_data.id);
                continue;
            }

            // Complete common WG with complement WG .
            if (composite_index && group_data.type == WorkerGroupType::Physical)
            {
                LOG_DEBUG(log, "Use WG {} to complete WG {}", groups_data[*composite_index].id, group_data.id);
                auto worker_complement = complementPhysicalWorkerGroup(group_data, groups_data[*composite_index]);
                if (worker_complement)
                {
                    for (const auto & complement_info : worker_complement->complement_infos)
                    {
                        LOG_DEBUG(
                            log,
                            "Use host {} of WG {} to complete WG {} for index {}",
                            complement_info.host_ports.toDebugString(),
                            groups_data[*composite_index].id,
                            group_data.id,
                            complement_info.target_index);
                        group_data.host_ports_vec.push_back(complement_info.host_ports);
                        group_data.metrics.worker_metrics_vec.push_back(complement_info.worker_metrics);
                        std::stable_sort(group_data.host_ports_vec.begin(), group_data.host_ports_vec.end());
                    }
                }
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

        updatePriorityGroups();

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

std::optional<WorkerComplement> VirtualWarehouseHandleImpl::complementPhysicalWorkerGroup(WorkerGroupData & common_group, const WorkerGroupData & completion_group)
{
    WorkerComplement worker_complement;
    String worker_prefix;
    auto total_number = settings.num_workers;
    std::unordered_set<UInt64> worker_id_suffixs;
    UInt64 worker_idx = 0;
    UInt64 max_worker_idx = 0;
    bool failed = false;
    // Find missing workers with suffix number of the worker_id
    for (const auto & host_ports : common_group.host_ports_vec)
    {
        auto dash_idx = host_ports.id.rfind('-');
        if (dash_idx == std::string::npos)
        {
            failed = true;
            break;
        }
        if (worker_prefix.empty())
            worker_prefix = host_ports.id.substr(0, dash_idx);
        auto worker_suffix = host_ports.id.substr(dash_idx + 1);
        try
        {
            worker_idx = boost::lexical_cast<UInt64>(worker_suffix);
            if (worker_idx > max_worker_idx)
                max_worker_idx = worker_idx;
        }
        catch (boost::bad_lexical_cast &)
        {
            failed = true;
            break;
        }
        worker_id_suffixs.insert(worker_idx);
    }
    if (failed)
    {
        LOG_ERROR(log, "parse worker id failed");
        return std::nullopt;
    }

    for (UInt64 idx = 0; idx < std::max(total_number, max_worker_idx + 1); idx++)
    {
        if (!worker_id_suffixs.contains(idx))
            worker_complement.complement_infos.emplace_back(idx);
    }

    if (worker_complement.complement_infos.empty() || worker_complement.complement_infos.size() > completion_group.host_ports_vec.size())
    {
        LOG_DEBUG(log, "Don't need complete wg or too many missing workers : {}", worker_complement.complement_infos.size());
        return std::nullopt;
    }

    worker_complement.candidates.resize(completion_group.host_ports_vec.size());

    // first round selection use cnsistent hash.
    for (auto & completion_info: worker_complement.complement_infos)
    {
        auto hash_index = ConsistentHashing(completion_info.target_index, completion_group.host_ports_vec.size());
        if (!worker_complement.candidates[hash_index])
        {
            completion_info.source_index = hash_index;
            worker_complement.candidates[hash_index] = true;
        }
        else
        {
            completion_info.candidate_index = hash_index;
        }
    }
    // second round selection, Sequentialy select valid worker
    size_t candidate_size = worker_complement.candidates.size();
    for (auto & completion_info : worker_complement.complement_infos)
    {
        if (!completion_info.source_index)
        {
            for (size_t step = 1; step < completion_group.host_ports_vec.size(); step++)
            {
                if (!worker_complement.candidates[(step + completion_info.candidate_index) % candidate_size])
                {
                    //get result
                    completion_info.source_index = (step + completion_info.candidate_index) % candidate_size;
                    worker_complement.candidates[*completion_info.source_index] = true;
                    break;
                }
            }
        }
        if (!completion_info.source_index)
        {
            LOG_ERROR(log, "Can't find candidate worker");
            return std::nullopt;
        }
    }
    // Prepare HostWithPorts for completion worker.
    for (auto & completion_info : worker_complement.complement_infos)
    {
        completion_info.host_ports = completion_group.host_ports_vec.at(*completion_info.source_index);
        completion_info.worker_metrics = completion_group.metrics.worker_metrics_vec.at(*completion_info.source_index);
        completion_info.worker_metrics.id = completion_info.host_ports.id;
        completion_info.host_ports.real_id = completion_info.host_ports.id;
        completion_info.host_ports.replaceId(worker_prefix + "-" + std::to_string(completion_info.target_index));
    }
    return worker_complement;
}

void VirtualWarehouseHandleImpl::updateSettings(const VirtualWarehouseSettings & new_settings)
{
    std::lock_guard lock(state_mutex);
    getQueueManager().updateQueue(new_settings.queue_datas);
    settings = new_settings;
}

void VirtualWarehouseHandleImpl::updateWorkerStatusFromPSM(
    const IServiceDiscovery::WorkerGroupMap & groups_data, const std::string & vw_name)
{
    for (const auto & [wg_name, host_ports_vec] : groups_data)
    {
        if (!worker_status_managers.contains(wg_name))
        {
            worker_status_managers[wg_name] = std::make_shared<WorkerStatusManager>(getContext());
        }
        auto current_wg_status = worker_status_managers[wg_name];

        for (const auto & host_ports : host_ports_vec)
        {
            auto worker_id = WorkerStatusManager::getWorkerId(vw_name, wg_name, host_ports.id);
            current_wg_status->restoreWorkerNode(worker_id);
        }
    }
}
void VirtualWarehouseHandleImpl::updatePriorityGroups()
{
    priority_groups.clear();
    std::unordered_map<Int64, std::vector<WorkerGroupHandle>> priority_group_maps;
    for (const auto & [_, group_handle] : worker_groups)
        priority_group_maps[group_handle->getPriority()].push_back(group_handle);
    for (auto & priority_group : priority_group_maps)
        priority_groups.push_back(std::move(priority_group));
    std::stable_sort(priority_groups.begin(), priority_groups.end());
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
        std::lock_guard lock(state_mutex);
        updateWorkerStatusFromPSM(groups_map, name);

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

        updatePriorityGroups();

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
}

/// Picking a worker group from local cache. Two stages:
/// - 1. filter worker groups by requirement.
/// - 2. select one group by algo.
WorkerGroupHandle
VirtualWarehouseHandleImpl::pickLocally([[maybe_unused]] const VWScheduleAlgo & algo, [[maybe_unused]] const Requirement & requirement)
{
    return randomWorkerGroup();
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
