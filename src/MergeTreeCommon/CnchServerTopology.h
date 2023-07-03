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
    CnchServerVwTopology(const String & vw_name_);

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
    CnchServerTopology() {}

    void addServer(const HostWithPorts & server, const String & server_vw_name = DEFAULT_SERVER_VW_NAME);

    std::map<String, CnchServerVwTopology> getVwTopologies() const;

    HostWithPorts getTargetServer(const String & uuid, const String & server_vw_name) const;

    HostWithPortsVec getServerList() const;

    bool empty() const;
    size_t getServerSize() const;

    void setExpiration(const UInt64 & new_expiration);

    UInt64 getExpiration() const;

    String format() const;

    bool isSameTopologyWith(const CnchServerTopology & other_topology) const;

private:
    UInt64 lease_expiration = 0;
    HostWithPortsVec servers;
    std::map<String, CnchServerVwTopology> vw_topologies;
};


String dumpTopologies(const std::list<CnchServerTopology>& topologies);

}

