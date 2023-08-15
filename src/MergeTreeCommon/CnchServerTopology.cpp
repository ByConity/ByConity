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

#include <MergeTreeCommon/CnchServerTopology.h>
#include <Common/ConsistentHashUtils/Hash.h>
#include <sstream>

namespace DB
{

CnchServerVwTopology::CnchServerVwTopology(const String & server_vw_name_)
    :server_vw_name(server_vw_name_)
{
}

void CnchServerVwTopology::addServer(const HostWithPorts & server)
{
    servers.push_back(server);
}

const HostWithPortsVec & CnchServerVwTopology::getServerList() const
{
    return servers;
}

String CnchServerVwTopology::getServerVwName() const
{
    return server_vw_name;
}

String CnchServerVwTopology::format() const
{
    std::stringstream ss;
    ss << "{server_vw_name: " << server_vw_name;
    ss << ", [";
    for (size_t i=0; i<servers.size(); i++)
    {
        if (i>0)
            ss << ", ";
        ss << servers[i].getHost();
    }
    ss << "]}";

    return ss.str();
}

bool CnchServerVwTopology::isSameToplogyWith(const CnchServerVwTopology & other_vw_topology) const
{
    return server_vw_name == other_vw_topology.server_vw_name
        && HostWithPorts::isExactlySameVec(servers, other_vw_topology.servers);
}

void CnchServerTopology::addServer(const HostWithPorts & server, const String & server_vw_name)
{
    auto it = vw_topologies.find(server_vw_name);
    if (it == vw_topologies.end())
        it = vw_topologies.emplace(server_vw_name, CnchServerVwTopology(server_vw_name)).first;
    it->second.addServer(server);
    servers.push_back(server);
}

std::map<String, CnchServerVwTopology> CnchServerTopology::getVwTopologies() const
{
    return vw_topologies;
}

HostWithPorts CnchServerTopology::getTargetServer(const String & uuid, const String & server_vw_name) const
{
    auto it = vw_topologies.find(server_vw_name);
    if (it == vw_topologies.end() || it->second.getServerList().empty())
        return {};
    const auto & servers_list = it->second.getServerList();
    auto hashed_index = consistentHashForString(uuid, servers_list.size());
    return servers_list[hashed_index];
}

HostWithPortsVec CnchServerTopology::getServerList() const
{
    return servers;
}

bool CnchServerTopology::empty() const
{
    return servers.empty();
}

size_t CnchServerTopology::getServerSize() const
{
    return servers.size();
}

void CnchServerTopology::setExpiration(const UInt64 & new_expiration)
{
    lease_expiration = new_expiration;
}

UInt64 CnchServerTopology::getExpiration() const
{
    return lease_expiration;
}

String CnchServerTopology::format() const
{
    std::stringstream ss;
    ss << "{expiration: " << lease_expiration;
    ss << ", [";
    for (auto it = vw_topologies.begin(); it != vw_topologies.end(); ++it)
    {
        if (it != vw_topologies.begin())
            ss << ", ";
        ss << it->second.format();
    }
    ss << "]}";

    return ss.str();
}

String dumpTopologies(const std::list<CnchServerTopology> & topologies)
{
    std::stringstream ss;
    ss << "[";
    for (auto d_it = topologies.begin(); d_it != topologies.end(); d_it++)
    {
        if (d_it != topologies.begin())
            ss << ", ";
        ss << d_it->format();
    }
    ss << "]";

    return ss.str();
}

bool CnchServerTopology::isSameTopologyWith(const CnchServerTopology & other_topology) const
{
    if (vw_topologies.size() != other_topology.vw_topologies.size())
        return false;
    auto it = vw_topologies.begin();
    auto other_it = other_topology.vw_topologies.begin();
    while (it != vw_topologies.end() && other_it != other_topology.vw_topologies.end())
    {
        if (!it->second.isSameToplogyWith(other_it->second))
            return false;

        ++it;
        ++other_it;
    }

    return true;
}

}
