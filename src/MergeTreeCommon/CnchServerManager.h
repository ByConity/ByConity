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

#include <Coordination/LeaderElectionBase.h>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context_fwd.h>
#include <MergeTreeCommon/CnchServerTopology.h>

namespace zkutil
{
    class LeaderElection;
    class ZooKeeper;
    using ZooKeeperPtr = std::shared_ptr<ZooKeeper>;
}

namespace DB
{

/***
 * CnchServerManager is used to synchronize topology of current cluster in metastore with that in consul.
 * It contains two kind of background tasks:
 * 1. Topology refresh task. This task periodically get current servers topology from consul.
 * 2. Lease renew task. This task is responsible for periodically update the topology to metastore.
 *
 * Leader election is required to make sure only one CnchServerManager can update server topology at a time.
 */
class CnchServerManager: public WithContext, public LeaderElectionBase
{
using Topology = CnchServerTopology;

public:
    explicit CnchServerManager(ContextPtr context_, const Poco::Util::AbstractConfiguration & config);

    ~CnchServerManager() override;

    bool isLeader() {return is_leader;}

    void shutDown();
    void partialShutdown();

    void updateServerVirtualWarehouses(const Poco::Util::AbstractConfiguration & config, const String & config_name = "server_virtual_warehouses");
private:
    void onLeader() override;
    void exitLeaderElection() override;
    void enterLeaderElection() override;

    void refreshTopology();
    void renewLease();
    void checkAsyncQueryStatus();

    /// set topology status when becoming leader. may runs in background tasks.
    void setLeaderStatus();

    Poco::Logger * log = &Poco::Logger::get("CnchServerManager");

    BackgroundSchedulePool::TaskHolder topology_refresh_task;
    BackgroundSchedulePool::TaskHolder lease_renew_task;
    BackgroundSchedulePool::TaskHolder async_query_status_check_task;

    std::optional<Topology> next_version_topology;
    std::list<Topology> cached_topologies;
    mutable std::mutex topology_mutex;

    std::atomic_bool need_stop{false};
    std::atomic_bool is_leader{false};
    std::atomic_bool leader_initialized{false};
    std::unordered_map<String, String> server_virtual_warehouses;
};

using CnchServerManagerPtr = std::shared_ptr<CnchServerManager>;

}
