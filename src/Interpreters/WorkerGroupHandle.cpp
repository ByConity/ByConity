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

#include <cstddef>
#include <Interpreters/WorkerGroupHandle.h>

#include <ResourceManagement/CommonData.h>
#include "Common/Exception.h"
#include "Common/HostWithPorts.h"
#include "common/getFQDNOrHostName.h"
#include <Common/Configurations.h>
#include <Common/parseAddress.h>
#include <Interpreters/Context.h>
#include <IO/ConnectionTimeouts.h>
#include <CloudServices/CnchWorkerClientPools.h>
#include <fmt/core.h>
#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_SUCH_SERVICE;
    extern const int VIRTUAL_WAREHOUSE_NOT_FOUND;
    extern const int RESOURCE_MANAGER_NO_AVAILABLE_WORKER;
    extern const int WORKER_NODE_NOT_FOUND;
}

WorkerGroupHandle WorkerGroupHandleImpl::mockWorkerGroupHandle(const String & worker_id_prefix_, UInt64 worker_number_, const ContextPtr & context_)
{
    HostWithPortsVec hosts_vec;
    for (size_t i=0; i<worker_number_; i++)
    {
        String worker_id = worker_id_prefix_ + toString(i);
        HostWithPorts mock_host;
        mock_host.id = std::move(worker_id);
        hosts_vec.emplace_back(std::move(mock_host));
    }

    return std::make_shared<WorkerGroupHandleImpl>("", WorkerGroupHandleSource::RM, "", hosts_vec, context_);
}

void WorkerGroupHandleImpl::buildRing()
{
    UInt32 num_replicas = getContext()->getConfigRef().getInt("consistent_hash_ring.num_replicas", 16);
    UInt32 num_probes = getContext()->getConfigRef().getInt("consistent_hash_ring.num_probes", 21);
    double load_factor = getContext()->getConfigRef().getDouble("consistent_hash_ring.load_factor", 1.15);
    ring = std::make_unique<DB::ConsistentHashRing>(num_replicas, num_probes, load_factor);
    std::for_each(shards_info.begin(), shards_info.end(), [&](const ShardInfo & info) { ring->insert(info.worker_id); });
}

WorkerGroupHandleImpl::WorkerGroupHandleImpl(
    String id_,
    WorkerGroupHandleSource source_,
    String vw_name_,
    HostWithPortsVec hosts_,
    const ContextPtr & context_,
    const WorkerGroupMetrics & metrics_)
    : WithContext(context_->getGlobalContext())
    , id(std::move(id_))
    , source(std::move(source_))
    , vw_name(std::move(vw_name_))
    , hosts(std::move(hosts_))
    , metrics(metrics_)
    , worker_num(hosts.size())
{
    /// some allocation algorithm (such as jump consistent hash) work best when
    /// 1) the index of existing workers don't change
    /// 2) new workers are added to the end with larger index
    /// we achieve this by
    /// 1) let k8s assign sequential worker id "{WG_NAME}_{IDX}" to each worker
    /// 2) make sure workers are sorted in worker id's order
    /// TODO: sort in numeric order rather than lexicographic order
    std::sort(hosts.begin(), hosts.end());

    auto current_context = getContext();

    const auto & settings = current_context->getSettingsRef();
    const auto & config = current_context->getConfigRef();
    bool enable_ssl = current_context->isEnableSSL();

    UInt16 clickhouse_port = enable_ssl ? static_cast<UInt16>(config.getInt("tcp_port_secure", 0))
                                        : static_cast<UInt16>(config.getInt("tcp_port", 0));

    auto user_password = current_context->getCnchInterserverCredentials();
    auto default_database = config.getRawString("default_database", "default");

    for (size_t i = 0; i < hosts.size(); ++i)
    {
        auto & host = hosts[i];
        Address address(host.getTCPAddress(), user_password.first, user_password.second, clickhouse_port, enable_ssl);

        ShardInfo info;
        info.worker_id = host.id;
        info.shard_num = i + 1; /// shard_num begin from 1
        info.weight = 1;

        if (address.is_local)
            info.local_addresses.push_back(address);

        LOG_DEBUG(getLogger("WorkerGroupHandleImpl"), "Add address {}. is_local: {} id: {}", host.toDebugString(), address.is_local, host.id);

        ConnectionPoolPtr pool = std::make_shared<ConnectionPool>(
            settings.distributed_connections_pool_size,
            address.host_name, address.port,
            default_database, user_password.first, user_password.second,
            /*cluster_*/"",/*cluster_secret_*/"",
            "server", address.compression, address.secure, 1,
            host.exchange_port, host.exchange_status_port, host.rpc_port, host.id);

        info.pool = std::make_shared<ConnectionPoolWithFailover>(
            ConnectionPoolPtrs{pool}, settings.load_balancing, settings.connections_with_failover_max_tries);
        info.per_replica_pools = {std::move(pool)};

        shards_info.emplace_back(std::move(info));
    }

    buildRing();
    LOG_DEBUG(getLogger("WorkerGroupHandleImpl"), "Success built ring with {} nodes\n", ring->size());
}

WorkerGroupHandleImpl::WorkerGroupHandleImpl(const WorkerGroupData & data, const ContextPtr & context_)
    : WorkerGroupHandleImpl(data.id, WorkerGroupHandleSource::RM, data.vw_name, data.host_ports_vec, context_, data.metrics)
{
    type = data.type;
    worker_num = data.num_workers;
    priority = data.priority;
}

WorkerGroupHandleImpl::WorkerGroupHandleImpl(const WorkerGroupHandleImpl & from, [[maybe_unused]] const std::vector<size_t> & indices)
    : WithContext(from.getContext()), id(from.getID()), vw_name(from.getVWName())
{
    auto current_context = context.lock();
    if (!current_context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Context expired!");

    for (size_t index : indices)
    {
        hosts.emplace_back(from.getHostWithPortsVec().at(index));
        shards_info.emplace_back(from.getShardsInfo().at(index));
    }

    buildRing();

    worker_num = from.workerNum();
}

static std::unordered_set<UInt64> getBusyWorkerIndexes(double ratio, const WorkerGroupMetrics & metrics)
{
    if (metrics.worker_metrics_vec.empty())
        return {};
    auto size = metrics.worker_metrics_vec.size();

    /// Mapping from #task to worker index list.
    std::map<UInt64, std::unordered_set<UInt64>> order_by_tasks;
    double avg = 0;
    for (size_t i = 0; i < size; ++i)
    {
        const auto & worker_metrics = metrics.worker_metrics_vec[i];
        auto num_tasks = worker_metrics.numTasks();
        order_by_tasks[num_tasks].insert(i);
        avg += num_tasks;
    }

    if (order_by_tasks.size() <= 1)
        return {};

    avg /= size;

    std::unordered_set<UInt64> res;
    for (auto it = order_by_tasks.rbegin(); it != order_by_tasks.rend(); ++it)
    {
        if (it->first >= ratio * avg)
            res.insert(it->second.begin(), it->second.end());
        else
            break;
    }
    return res;
}

CnchWorkerClientPtr WorkerGroupHandleImpl::getWorkerClient(bool skip_busy_worker) const
{
    std::uniform_int_distribution dist;
    size_t sequence = dist(thread_local_rng);
    return getWorkerClient(sequence, skip_busy_worker).second;
}

std::pair<UInt64, CnchWorkerClientPtr> WorkerGroupHandleImpl::getWorkerClient(UInt64 sequence, bool skip_busy_worker) const
{
    if (hosts.empty())
        throw Exception("No available worker for " + id, ErrorCodes::RESOURCE_MANAGER_NO_AVAILABLE_WORKER);

    auto size = hosts.size();
    if (size == 1)
        return {0, doGetWorkerClient(hosts[0])};

    auto start_index = sequence % size;
    if (!skip_busy_worker)
        return {start_index, doGetWorkerClient(hosts[start_index])};

    auto ratio = getContext()->getRootConfig().vw_ratio_of_busy_worker.value;
    auto busy_worker_indexes = getBusyWorkerIndexes(ratio, getMetrics());
    for (size_t i = 0; i < size; ++i)
    {
        start_index %= size;
        if (busy_worker_indexes.contains(start_index))
        {
            start_index++;
            continue;
        }
        return {start_index, doGetWorkerClient(hosts[start_index])};
    }
    start_index %= size;
    return {start_index, doGetWorkerClient(hosts[start_index])};
}

CnchWorkerClientPtr WorkerGroupHandleImpl::getWorkerClient(const HostWithPorts & host_ports) const
{
    if (indexOf(host_ports).has_value())
    {
        return doGetWorkerClient(host_ports);
    }

    throw Exception("Can't get WorkerClient for host_ports: " + host_ports.toDebugString(), ErrorCodes::NO_SUCH_SERVICE);
}

std::vector<CnchWorkerClientPtr> WorkerGroupHandleImpl::getWorkerClients() const
{
    std::vector<CnchWorkerClientPtr> res;
    res.reserve(hosts.size());
    for (const auto & host : hosts)
        res.emplace_back(doGetWorkerClient(host));
    return res;
}

CnchWorkerClientPtr WorkerGroupHandleImpl::doGetWorkerClient(const HostWithPorts & host_ports) const
{
    /// Get a cached client, or create a new one when cache miss or client is unhealthy.
    return getContext()->getCnchWorkerClientPools().getWorker(host_ports);
}

size_t WorkerGroupHandleImpl::getWorkerIndex(const String & worker_id) const
{
    for (size_t i = 0; i < hosts.size(); ++i)
        if (worker_id == hosts[i].id)
            return i;
    throw Exception(ErrorCodes::WORKER_NODE_NOT_FOUND, "worker '{}' not found in worker group {}", worker_id, id);
}

bool WorkerGroupHandleImpl::isSame(const WorkerGroupData & data) const
{
    if (id != data.id || vw_name != data.vw_name)
        return false;
    if (!HostWithPorts::isExactlySameVec(hosts, data.host_ports_vec))
        return false;
    return true;
}

WorkerGroupHandle WorkerGroupHandleImpl::cloneAndRemoveShards(const std::vector<uint64_t> & remove_marks) const
{
    std::vector<size_t> cloned_hosts;
    for (size_t i = 0; i < hosts.size(); ++i)
    {
        if (!remove_marks[i])
            cloned_hosts.emplace_back(i);
    }

    if (cloned_hosts.empty())
        throw Exception("No worker available for after removing not available shards.", ErrorCodes::VIRTUAL_WAREHOUSE_NOT_FOUND);

    return std::make_shared<WorkerGroupHandleImpl>(*this, cloned_hosts);
}

Strings WorkerGroupHandleImpl::getWorkerTCPAddresses(const Settings & settings) const
{
    Strings res;
    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(settings);
    for (const auto & shard_info : shards_info)
    {
        auto entry = shard_info.pool->get(timeouts, &settings, true);
        Connection * conn = &(*entry);
        res.emplace_back(conn->getHost() + ":" + std::to_string(conn->getPort()));
    }
    return res;
}

Strings WorkerGroupHandleImpl::getWorkerIDVec() const
{
    Strings res;
    for (const auto & host : hosts)
        res.emplace_back(host.id);
    return res;
}

std::vector<std::pair<String, UInt16>> WorkerGroupHandleImpl::getReadWorkers() const
{
    std::vector<std::pair<String, UInt16>> res;
    const auto & settings = getContext()->getSettingsRef();
    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(settings);
    for (const auto & shard_info : shards_info)
    {
        auto entry = shard_info.pool->get(timeouts, &settings, true);
        Connection * conn = &(*entry);
        res.emplace_back(conn->getHost(), conn->getPort());
    }
    return res;
}

std::unordered_map<String, HostWithPorts> WorkerGroupHandleImpl::getIdHostPortsMap() const
{
    std::unordered_map<String, HostWithPorts> id_hosts;
    for (const auto & host : hosts)
    {
        id_hosts.emplace(host.id, host);
    }
    return id_hosts;
}

}
