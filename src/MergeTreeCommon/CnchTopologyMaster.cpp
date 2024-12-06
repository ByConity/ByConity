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

#include <MergeTreeCommon/CnchTopologyMaster.h>
#include <Catalog/Catalog.h>
#include <common/logger_useful.h>
#include <CloudServices/CnchServerClient.h>
#include <Interpreters/Context.h>
#include <Storages/PartCacheManager.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CNCH_NO_AVAILABLE_TOPOLOGY;
}

CnchTopologyMaster::CnchTopologyMaster(ContextPtr context_)
    : WithContext(context_)
    , settings{context_->getSettings()}
{
    topology_fetcher = getContext()->getTopologySchedulePool().createTask("TopologyFetcher", [&]() {
        bool success = fetchTopologies();
        fetch_time = time(nullptr);
        auto schedule_delay = success ? settings.topology_refresh_interval_ms.totalMilliseconds()
                                      : settings.topology_retry_interval_ms.totalMilliseconds();
        topology_fetcher->scheduleAfter(schedule_delay);
    });
    topology_fetcher->activateAndSchedule();
}

CnchTopologyMaster::~CnchTopologyMaster()
{
    try
    {
        shutDown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

bool CnchTopologyMaster::fetchTopologies()
{
    try
    {
        auto fetched = getContext()->getCnchCatalog()->getTopologies();
        if (!fetched.empty())
        {
            /// copy current topology and update it with new fetched one.
            auto last_topology = topologies;
            {
                std::lock_guard lock(mutex);
                topologies = fetched;
            }

            /// need to adjust part cache if the server topology change.
            if (getContext()->getServerType() == ServerType::cnch_server && !last_topology.empty())
            {
                /// reset cache if the server fails to sync topology for a while, prevent from ABA problem
                if (topologies.front().getExpiration()
                        > last_topology.front().getExpiration() + 2 * settings.topology_lease_life_ms.totalMilliseconds()
                    && topologies.front().getTerm() > last_topology.front().getTerm() + 1)
                {
                    LOG_WARNING(log, "Reset part and table cache because of topology change");
                    if (auto cache_manager = getContext()->getPartCacheManager(); cache_manager)
                        cache_manager->reset();
                }
                else if (!topologies.front().isSameTopologyWith(last_topology.front()))
                {
                    LOG_WARNING(log, "Invalid outdated part and table cache because of topology change");
                    if (getContext()->getPartCacheManager())
                        getContext()->getPartCacheManager()->invalidCacheWithNewTopology(topologies.front());
                }
            }
        }
        else
        {
            /// needed for the 1st time write to kv..
            LOG_TRACE(log, "Cannot fetch topology from remote.");
            return false;
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        return false;
    }

    return true;
}

std::list<CnchServerTopology> CnchTopologyMaster::getCurrentTopology()
{
    std::lock_guard lock(mutex);
    return topologies;
}

std::pair<PairInt64, CnchServerTopology> CnchTopologyMaster::getCurrentTopologyVersion()
{
    std::pair<PairInt64, CnchServerTopology> res = std::make_pair(PairInt64(0, 0), CnchServerTopology{});
    UInt64 current_ts = getContext()->tryGetTimestamp(__PRETTY_FUNCTION__);
    if (TxnTimestamp::fallbackTS() == current_ts)
        return res;
    UInt64 current_time = current_ts>>18;
    std::list<CnchServerTopology> current_topology = getCurrentTopology();
    UInt64 lease_life_time = getContext()->getSettingsRef().topology_lease_life_ms.totalMilliseconds();

    auto it = current_topology.begin();
    while (it != current_topology.end())
    {
        UInt64 lease_start_time = it->getInitialTime() ? it->getInitialTime() : it->getExpiration() - lease_life_time;

        if (current_time >= lease_start_time && current_time < it->getExpiration() - 6000)
        {
            res = std::make_pair(PairInt64{it->getInitialTime(), it->getTerm()}, *it);
            break;
        }
        it++;
    }
    return res;
}

HostWithPorts CnchTopologyMaster::getTargetServerImpl(
        const String & table_uuid,
        const String server_vw_name,
        std::list<CnchServerTopology> & current_topology,
        const UInt64 current_ts,
        bool allow_empty_result,
        bool allow_tso_unavailable)
{
    HostWithPorts target_server{};
    UInt64 lease_life_time = settings.topology_lease_life_ms.totalMilliseconds();
    bool tso_is_available = (current_ts != TxnTimestamp::fallbackTS());
    UInt64 commit_time_ms = current_ts >> 18;

    auto it = current_topology.begin();
    while(it != current_topology.end())
    {
        UInt64 lease_start_time = it->getInitialTime() ? it->getInitialTime() : it->getExpiration() - lease_life_time;
        bool commit_within_lease_life_time = commit_time_ms >= lease_start_time;

        if (!tso_is_available && allow_tso_unavailable)
        {
            target_server = it->getTargetServer(table_uuid, server_vw_name);
            if (!target_server.empty())
            {
                LOG_DEBUG(log, "Fallback to first possible target server due to TSO unavailability. servers vw name is {}", server_vw_name);
                break;
            }
        }

        /// currently, topology_lease_life_ms is 12000ms by default. we suppose bytekv MultiWrite timeout is 6000ms.
        if (commit_within_lease_life_time && commit_time_ms < it->getExpiration() - 6000)
        {
            target_server = it->getTargetServer(table_uuid, server_vw_name);
            target_server.topology_version = PairInt64(it->getInitialTime(), it->getTerm());
            break;
        }
        else if (commit_within_lease_life_time && commit_time_ms < it->getExpiration())
        {
            HostWithPorts server_in_old_topology = it->getTargetServer(table_uuid, server_vw_name);
            it++;
            if (it != current_topology.end())
            {
                HostWithPorts server_in_new_topology = it->getTargetServer(table_uuid, server_vw_name);
                if (server_in_new_topology.isSameEndpoint(server_in_old_topology))
                {
                    target_server = server_in_new_topology;
                    target_server.topology_version = PairInt64(it->getInitialTime(), it->getTerm());
                }
            }
            break;
        }

        it++;
    }

    if (target_server.empty())
    {
        if (!allow_empty_result)
            throw Exception("No available topology for server vw: " + server_vw_name
                + ", current commit time : " + std::to_string(commit_time_ms)
                + ". Available topology : " + dumpTopologies(current_topology)
                + ". Current time : " + std::to_string(time(nullptr))
                + ". Fetch time : " + std::to_string(fetch_time)
                , ErrorCodes::CNCH_NO_AVAILABLE_TOPOLOGY);
        else
            LOG_INFO(log, "No available topology for server vw: {}, current commit time : {}. Available topology : {}. Current time : {}. Fetch time : {}"
                , server_vw_name, std::to_string(commit_time_ms), dumpTopologies(current_topology), time(nullptr), fetch_time);
    }

    return target_server;
}


HostWithPorts CnchTopologyMaster::getTargetServer(const String & table_uuid, const String & server_vw_name, bool allow_empty_result, bool allow_tso_unavailable)
{
    /// Its important to get current topology before get current timestamp.
    std::list<CnchServerTopology> current_topology = getCurrentTopology();
    UInt64 ts = getContext()->tryGetTimestamp(__PRETTY_FUNCTION__);

    return getTargetServerImpl(table_uuid, server_vw_name, current_topology, ts, allow_empty_result, allow_tso_unavailable);
}

HostWithPorts CnchTopologyMaster::getTargetServer(const String & table_uuid, const String & server_vw_name, const UInt64 ts,  bool allow_empty_result, bool allow_tso_unavailable)
{
    std::list<CnchServerTopology> current_topology = getCurrentTopology();
    return getTargetServerImpl(table_uuid, server_vw_name, current_topology, ts, allow_empty_result, allow_tso_unavailable);
}


void CnchTopologyMaster::shutDown()
{
    if (topology_fetcher)
        topology_fetcher->deactivate();
}

void CnchTopologyMaster::dumpStatus() const
{
    std::stringstream ss;
    ss << "[tasks info] : \n"
       << "current_time : " << time(nullptr) << std::endl
       << "fetch_time : " << fetch_time << std::endl
       << "[current fetched topology] : \n";

    {
        std::unique_lock lock(mutex, std::defer_lock);
        if (lock.try_lock_for(std::chrono::milliseconds(1000)))
        {
            ss << DB::dumpTopologies(topologies);
        }
        else
        {
            ss << "Failed to accquire topology master lock";
        }
    }
    LOG_INFO(log, "Dump topology master status :\n{}", ss.str());
}

}
