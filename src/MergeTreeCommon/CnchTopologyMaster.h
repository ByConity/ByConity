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
#include <MergeTreeCommon/CnchServerTopology.h>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context_fwd.h>
#include <Core/Settings.h>

namespace DB
{
/***
 * This class help to supply a unified view of servers' topology in cnch cluster.
 * A background task will periodically fetch topology from metastore to ensure the cached topology
 * keeps up to date.
 */
class CnchTopologyMaster: WithContext
{
public:
    explicit CnchTopologyMaster(ContextPtr context_);

    ~CnchTopologyMaster();

    std::list<CnchServerTopology> getCurrentTopology();

    std::pair<PairInt64, CnchServerTopology> getCurrentTopologyVersion();

    /// Get target server for table with current timestamp.
    HostWithPorts getTargetServer(const String & table_uuid, const String & server_vw_name, bool allow_empty_result, bool allow_tso_unavailable = false);
    /// Get target server with provided timestamp.
    HostWithPorts getTargetServer(const String & table_uuid, const String & server_vw_name, const UInt64 ts,  bool allow_empty_result, bool allow_tso_unavailable = false);

    void shutDown();

    void dumpStatus() const;
private:

    bool fetchTopologies();

    HostWithPorts getTargetServerImpl(
        const String & table_uuid,
        const String server_vw_name,
        std::list<CnchServerTopology> & current_topology,
        UInt64 current_ts,
        bool allow_empty_result,
        bool allow_tso_unavailable);

    LoggerPtr log = getLogger("CnchTopologyMaster");
    BackgroundSchedulePool::TaskHolder topology_fetcher;
    std::list<CnchServerTopology> topologies;
    const Settings settings;
    mutable std::timed_mutex mutex;

    std::atomic<UInt64> fetch_time{0};
};

using CnchTopologyMasterPtr = std::shared_ptr<CnchTopologyMaster>;

}
