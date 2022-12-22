#pragma once

#include <list>
#include <DataTypes/DataTypeUUID.h>
#include <Common/HostWithPorts.h>
#include <DataTypes/DataTypeString.h>

namespace DB
{

class CnchServerTopology
{

public:
    CnchServerTopology(const UInt64 & lease_expiration_, HostWithPortsVec && servers_);

    HostWithPortsVec getServerList() const;
    size_t getServerSize() const;

    void setExpiration(const UInt64 & new_expiration);

    UInt64 getExpiration() const;

    String format() const;

private:
    UInt64 lease_expiration;
    HostWithPortsVec servers;
};


String dumpTopologies(const std::list<CnchServerTopology>& topologies);

}

