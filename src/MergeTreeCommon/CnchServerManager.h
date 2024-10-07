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

#include <Catalog/IMetastore.h>
#include <Common/Logger.h>
#include <Common/StorageElection/KvStorage.h>
#include <Common/StorageElection/StorageElector.h>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context_fwd.h>
#include <MergeTreeCommon/CnchServerTopology.h>

namespace DB
{

/***
 * CnchServerManager is used to get the current topology from service discovery and synchronize it to metastore.
 * It contains two kind of background tasks:
 * 1. Topology refresh task. This task periodically get current servers topology from service discovery.
 * 2. Lease renew task. This task is responsible for periodically update the topology to metastore.
 *
 * Leader election is required to make sure only one CnchServerManager can update server topology at a time.
 */
class CnchServerManager: public WithContext
{
using Topology = CnchServerTopology;

public:
    explicit CnchServerManager(ContextPtr context_, const Poco::Util::AbstractConfiguration & config);

    ~CnchServerManager();

    bool isLeader() const;
    std::optional<HostWithPorts> getCurrentLeader() const;

    void shutDown();
    void partialShutdown();

    void dumpServerStatus() const;

    void updateServerVirtualWarehouses(const Poco::Util::AbstractConfiguration & config, const String & config_name = "server_virtual_warehouses");
private:
    bool onLeader();
    bool onFollower();

    bool refreshTopology();
    bool renewLease();
    bool checkAsyncQueryStatus();

    /// set topology status when becoming leader. may runs in background tasks.
    void initLeaderStatus();

    LoggerPtr log = getLogger("CnchServerManager");

    BackgroundSchedulePool::TaskHolder topology_refresh_task;
    BackgroundSchedulePool::TaskHolder lease_renew_task;
    BackgroundSchedulePool::TaskHolder async_query_status_check_task;

    std::optional<Topology> next_version_topology;
    std::list<Topology> cached_topologies;
    mutable std::timed_mutex topology_mutex;

    UInt64 term = 0;

    std::shared_ptr<StorageElector> elector;
    std::atomic_bool leader_initialized{false};
    std::atomic<UInt64> refresh_topology_time{0};
    std::atomic<UInt64> renew_lease_time{0};
    std::atomic<UInt64> async_query_status_check_time{0};
    std::unordered_map<String, String> server_virtual_warehouses;
};

using CnchServerManagerPtr = std::shared_ptr<CnchServerManager>;

}
