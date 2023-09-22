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

#include <ResourceManagement/PhysicalWorkerGroup.h>
#include <ResourceManagement/CommonData.h>
#include <ResourceManagement/WorkerNode.h>

#include <atomic>
#include <iterator>
#include <algorithm>
#include <random>

namespace DB::ErrorCodes
{
    extern const int RESOURCE_MANAGER_NO_AVAILABLE_WORKER;
}

namespace DB::ResourceManagement
{
size_t PhysicalWorkerGroup::getNumWorkers() const
{
    std::lock_guard lock(state_mutex);
    return workers.size();
}

std::map<String, WorkerNodePtr> PhysicalWorkerGroup::getWorkers() const
{
    std::lock_guard lock(state_mutex);
    return getWorkersImpl(lock);
}

std::map<String, WorkerNodePtr> PhysicalWorkerGroup::getWorkersImpl(std::lock_guard<bthread::RecursiveMutex> & /*lock*/) const
{
    return workers;
}

WorkerGroupData PhysicalWorkerGroup::getData(bool with_metrics, bool only_running_state) const
{
    WorkerGroupData data;
    data.id = getID();
    data.type = WorkerGroupType::Physical;
    data.vw_uuid = getVWUUID();
    data.vw_name = getVWName();
    data.psm = psm;

    for (const auto & [_, worker] : getWorkers())
    {
        if (only_running_state && worker->state.load(std::memory_order_relaxed) != WorkerState::Running)
            continue;
        data.host_ports_vec.push_back(worker->host);
        data.worker_node_resource_vec.push_back(worker->getResourceData());
        
        if (with_metrics)
            data.metrics.worker_metrics_vec.push_back(worker->getMetrics());
    }
    
    data.num_workers = data.host_ports_vec.size();

    if (with_metrics)
        data.metrics.id = id;
    return data;
}

WorkerGroupMetrics PhysicalWorkerGroup::getMetrics() const
{
    WorkerGroupMetrics metrics(id);
    auto all_workers = getWorkers();
    for (const auto & [_, worker] : all_workers)
        metrics.worker_metrics_vec.push_back(worker->getMetrics());
    return metrics;
}

void PhysicalWorkerGroup::registerNode(const WorkerNodePtr & node)
{
    std::lock_guard lock(state_mutex);
    /// replace the old Node if exists.
    workers[node->getID()] = node;
}

void PhysicalWorkerGroup::removeNode(const String & worker_id)
{
    std::lock_guard lock(state_mutex);
    workers.erase(worker_id);
}

void PhysicalWorkerGroup::addLentGroupDestID(const String & group_id)
{
    std::lock_guard lock(state_mutex);
    lent_groups_dest_ids.insert(group_id);
}

void PhysicalWorkerGroup::removeLentGroupDestID(const String & group_id)
{
    std::lock_guard lock(state_mutex);
    auto it = lent_groups_dest_ids.find(group_id);
    if (it == lent_groups_dest_ids.end())
        throw Exception("Auto linked group id not found", ErrorCodes::LOGICAL_ERROR);

    lent_groups_dest_ids.erase(it);
}

void PhysicalWorkerGroup::clearLentGroups()
{
    std::lock_guard lock(state_mutex);
    lent_groups_dest_ids.clear();
}

std::unordered_set<String> PhysicalWorkerGroup::getLentGroupsDestIDs() const
{
    std::lock_guard lock(state_mutex);
    return lent_groups_dest_ids;
}

std::vector<WorkerNodePtr> PhysicalWorkerGroup::randomWorkers(const size_t n, const std::unordered_set<String> & blocklist) const
{
    std::vector<WorkerNodePtr> candidates;

    {
        std::lock_guard lock(state_mutex);
        if (!workers.empty())
        {
            candidates.reserve(workers.size() - blocklist.size());
            for (const auto & [_, worker] : workers)
            {
                if (!blocklist.contains(worker->id))
                    candidates.push_back(worker);
            }
        }
    }

    if (candidates.empty())
        throw Exception("No available worker for " + id, ErrorCodes::RESOURCE_MANAGER_NO_AVAILABLE_WORKER);

    std::vector<WorkerNodePtr> res;
    res.reserve(n);
    std::sample(candidates.begin(), candidates.end(), std::back_inserter(res), n, std::mt19937{std::random_device{}()});
    return res;
}

}
