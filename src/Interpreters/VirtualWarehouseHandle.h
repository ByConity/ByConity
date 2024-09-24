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

#pragma once

#include <Common/Logger.h>
#include <CloudServices/CnchWorkerClient.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/VirtualWarehouseQueue.h>
#include <ResourceManagement/CommonData.h>
#include <ResourceManagement/VWScheduleAlgo.h>
#include <ServiceDiscovery/IServiceDiscovery.h>
#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Interpreters/WorkerStatusManager.h>
#include <Core/SettingsEnums.h>

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <boost/noncopyable.hpp>

namespace Poco
{
class Logger;
}

namespace DB
{
class Context;
class CnchWorkerClient;
using CnchWorkerClientPtr = std::shared_ptr<CnchWorkerClient>;
class WorkerGroupHandleImpl;
using WorkerGroupHandle = std::shared_ptr<WorkerGroupHandleImpl>;

struct ComplementResult
{
    explicit ComplementResult(size_t target_index_) : target_index(target_index_) {}
    size_t target_index {0};
    std::optional<size_t> source_index;
    size_t candidate_index {0};
    HostWithPorts host_ports;
    WorkerMetrics worker_metrics;
};

struct WorkerComplement
{
    std::vector<bool> candidates;
    std::vector<ComplementResult> complement_infos;
};

enum class VirtualWarehouseHandleSource
{
    RM,
    PSM,
};

constexpr auto toString(VirtualWarehouseHandleSource s)
{
    switch (s)
    {
        case VirtualWarehouseHandleSource::RM:
            return "RM";
        case VirtualWarehouseHandleSource::PSM:
            return "PSM";
    }
}

/**
 * NOTE:
 * 1. VirtualWarehouseHandleImpl is a mutable class which is protected by mutex.
 * 2. There is only one VirtualWarehouseHandle object in server for a unique VW (identified by uuid).
 * 3. The content in handle might be outdated because the handle would be only updated periodically.
 * 4. After updating, the outdated WorkerGroupHandle(s) will be replaced by new ones.
 */
class VirtualWarehouseHandleImpl : protected WithContext, private boost::noncopyable
{
private:
    friend class VirtualWarehousePool; /// could only be created by VirtualWarehousePool

    VirtualWarehouseHandleImpl(
        VirtualWarehouseHandleSource source,
        String name,
        UUID uuid,
        const ContextPtr global_context_,
        const VirtualWarehouseSettings & settings = {});

    VirtualWarehouseHandleImpl(VirtualWarehouseHandleSource source, const VirtualWarehouseData & vw_data, const ContextPtr global_context_);

public:
    using Container = std::map<String, WorkerGroupHandle>;
    using VirtualWarehouseHandle = std::shared_ptr<VirtualWarehouseHandleImpl>;
    using CnchWorkerClientPtr = std::shared_ptr<CnchWorkerClient>;
    using VWScheduleAlgo = ResourceManagement::VWScheduleAlgo;
    using Requirement = ResourceManagement::ResourceRequirement;
    using WorkerGroupMetrics = ResourceManagement::WorkerGroupMetrics;

    enum UpdateMode
    {
        NoUpdate,
        TryUpdate,
        ForceUpdate,
    };

    auto getSource() const { return source; }
    auto & getName() const { return name; }
    auto getUUID() const { return uuid; }
    auto & getSettingsRef() const { return settings; }

    bool empty(UpdateMode mode = NoUpdate);
    Container getAll(UpdateMode mode = NoUpdate);
    size_t getNumWorkers(UpdateMode mode = NoUpdate);

    WorkerGroupHandle getWorkerGroup(const String & worker_group_id, UpdateMode mode = TryUpdate);
    WorkerGroupHandle pickWorkerGroup(
        VWScheduleAlgo query_algo,
        bool use_router = false,
        VWLoadBalancing load_balance = VWLoadBalancing::RANDOM,
        const Requirement & requirement = {},
        UpdateMode mode = TryUpdate
        );
    WorkerGroupHandle pickLocally(const VWScheduleAlgo & algo, const Requirement & requirement = {});
    WorkerGroupHandle randomWorkerGroup(UpdateMode mode = TryUpdate);
    void updateWorkerStatusFromRM(const std::vector<WorkerGroupData> & groups_data);
    void updateWorkerStatusFromPSM(const IServiceDiscovery::WorkerGroupMap & groups_data, const std::string & vw_name);

    bool addWorkerGroup(const WorkerGroupHandle & worker_group);

    /// Caller should already know the worker group id when picking a single worker.
    /// So VW handle just forward the request to the target WG handle, or forward to a random WG if it's not specified.
    CnchWorkerClientPtr pickWorker(const String & worker_group_id, bool skip_busy_worker = true);

    void updateSettings(const VirtualWarehouseSettings & settings);
    std::pair<UInt64, CnchWorkerClientPtr> pickWorker(const String & worker_group_id, UInt64 sequence, bool skip_busy_worker = true);
    CnchWorkerClientPtr getWorker();
    CnchWorkerClientPtr getWorkerByHash(const String & key);
    CnchWorkerClientPtr getWorkerByHostWithPorts(const HostWithPorts & host_ports);
    std::vector<CnchWorkerClientPtr> getAllWorkers();

    VWQueueResultStatus enqueue(VWQueueInfoPtr queue_info, UInt64 timeout_ms)
    {
        return queue_manager.enqueue(queue_info, timeout_ms);
    }
    VirtualWarehouseQueueManager & getQueueManager() { return queue_manager; }

    std::optional<WorkerComplement> complementPhysicalWorkerGroup(WorkerGroupData & common_group, const WorkerGroupData & completion_group);

    void updatePriorityGroups();

    WorkerStatusManagerPtr getWorkerStatusManager(const String & wg_id)
    {
        std::lock_guard lock(state_mutex);
        if (auto iter = worker_status_managers.find(wg_id); iter != worker_status_managers.end())
        {
            return iter->second;
        }
        return worker_status_managers.emplace(wg_id, std::make_shared<WorkerStatusManager>(getContext())).first->second;
    }

    void tryUpdateWorkerGroups(UpdateMode mode);

private:
    bool addWorkerGroupImpl(const WorkerGroupHandle & worker_group, const std::lock_guard<std::mutex> & lock);
    bool updateWorkerGroupsFromRM();
    bool updateWorkerGroupsFromPSM();

    using WorkerGroupAndMetrics = std::pair<WorkerGroupHandle, WorkerGroupMetrics>;
    void filterGroup(const Requirement & requirement, std::vector<WorkerGroupAndMetrics> & out_available_groups);
    WorkerGroupHandle selectGroup(const VWScheduleAlgo & algo, std::vector<WorkerGroupAndMetrics> & available_groups);

    static constexpr auto PSM_WORKER_GROUP_SUFFIX = "_psm";

    const VirtualWarehouseHandleSource source;
    const String name;
    const UUID uuid;
    VirtualWarehouseSettings settings;
    LoggerPtr log;

    /// In ByteHouse, a VW will be auto recycled (auto-suspend) if no new queries received for a period (5 minutes by default).
    /// And when user send queries to the VW again, ByteYard will make sure to send out the queries after workers are full ready.
    /// Even though workers are full ready, the VW handle may still hold the outdated data as the UpdateMode is always TryUpdate.
    /// This cause some query failures in auto-resume period. https:****
    /// To fix this issue, we do a ForceUpdate if the data is not updated for a long time (the timeout of auto-suspend).
    size_t force_update_interval_ns = 5ULL * 60 * 1000 * 1000 * 1000;
    size_t try_update_interval_ns = 500ULL * 1000 * 1000;
    std::atomic<UInt64> last_update_time_ns{0};
    std::atomic<UInt64> last_settings_timestamp{0};

    mutable std::mutex state_mutex;
    Container worker_groups;
    using PriorityGroups = std::pair<Int64, std::vector<WorkerGroupHandle>>;
    std::vector<PriorityGroups> priority_groups;
    std::atomic<size_t> pick_group_sequence = 0; /// round-robin index for pickWorkerGroup.
    VirtualWarehouseQueueManager queue_manager;
    std::unordered_map<String, WorkerStatusManagerPtr> worker_status_managers;
};

using VirtualWarehouseHandle = std::shared_ptr<VirtualWarehouseHandleImpl>;

}
