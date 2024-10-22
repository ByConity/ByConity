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

#include <list>
#include <DataTypes/DataTypeUUID.h>
#include <Common/HostWithPorts.h>
#include <DataTypes/DataTypeString.h>

namespace DB
{

class CnchServerVwTopology
{
public:
    explicit CnchServerVwTopology(const String & vw_name_);

    bool operator<(const CnchServerVwTopology & other) const;
    void addServer(const HostWithPorts & server);
    const HostWithPortsVec & getServerList() const;

    String getServerVwName() const;

    String format() const;

    bool isSameToplogyWith(const CnchServerVwTopology & other_vw_topology) const;

private:
    String server_vw_name;
    HostWithPortsVec servers;
};

class CnchServerTopology
{

public:
    CnchServerTopology() = default;
    explicit CnchServerTopology(const String & leader_info_) : leader_info(leader_info_) { }

    void addServer(const HostWithPorts & server, const String & server_vw_name = DEFAULT_SERVER_VW_NAME);

    std::map<String, CnchServerVwTopology> getVwTopologies() const;

    HostWithPorts getTargetServer(const String & uuid, const String & server_vw_name) const;

    HostWithPortsVec getServerList() const;

    bool empty() const;
    size_t getServerSize() const;

    void setExpiration(const UInt64 & new_expiration);

    void setInitialTime(const UInt64 & initial_time);

    UInt64 getExpiration() const;

    UInt64 getInitialTime() const;

    UInt64 getTerm() const;

    void setTerm(UInt64 new_term);

    String format() const;

    bool isSameTopologyWith(const CnchServerTopology & other_topology) const;

    std::pair<std::map<String, CnchServerVwTopology>, std::map<String, CnchServerVwTopology>>
    diffWith(const CnchServerTopology & other_topology) const;

    const String & getLeaderInfo() const { return leader_info; }
    void setLeaderInfo(const String & leader_info_) { leader_info = leader_info_; }
    const String & getReason() const { return reason; }
    void setReason(const String & reason_) { reason = reason_; }

private:
    UInt64 lease_initialtime = 0;
    UInt64 lease_expiration = 0;
    UInt64 term = 0;
    String leader_info;
    String reason;
    HostWithPortsVec servers;
    std::map<String, CnchServerVwTopology> vw_topologies;
};


String dumpTopologies(const std::list<CnchServerTopology>& topologies);
/**
 * @brief Dump the difference two topologies. (a helper function for debugging)
 *
 * @param topology_diff Diffs of two topologies. Each part of the pair
 * is a map of vw_name to vw_topology. You can get the diff
 * by calling `diffWith` method of `CnchServerTopology`.
 * @return A string representation of the difference.
 */
String dumpTopologies(const std::pair<std::map<String, CnchServerVwTopology>, std::map<String, CnchServerVwTopology>> & topology_diff);
}
