#pragma once
#include <memory>
#include <unordered_map>
#include <Core/Types.h>
#include <Common/HostWithPorts.h>

namespace DB
{

enum class ComponentType
{
    SERVER,
    WORKER,
    TSO,
    DAEMON_MANAGER,
    NNPROXY,  /// look up only by consul client
    KMS,  /// look up only by consul client
    RESOURCE_MANAGER,
};

inline static String typeToString(ComponentType type)
{
    switch (type)
    {
        case ComponentType::SERVER:
            return "SERVER";
        case ComponentType::WORKER:
            return "WORKER";
        case ComponentType::TSO:
            return "TSO";
        case ComponentType::DAEMON_MANAGER:
            return "DAEMON_MANAGER";
        case ComponentType::NNPROXY:
            return "NNPROXY";
        case ComponentType::KMS:
            return "KMS";
        case ComponentType::RESOURCE_MANAGER:
            return "RESOURCE_MANAGER";
        // default:
        //     return "UNKNOWN";
    }
}

enum class ServiceDiscoveryMode
{
    LOCAL,
    CONSUL,
    DNS,
};

inline static String typeToString(ServiceDiscoveryMode mode)
{
    switch (mode)
    {
        case ServiceDiscoveryMode::LOCAL:
            return "LOCAL";
        case ServiceDiscoveryMode::CONSUL:
            return "CONSUL";
        case ServiceDiscoveryMode::DNS:
            return "DNS";
        // default:
        //     return "UNKNOWN";
    }
}

class IServiceDiscovery
{
public:
    using WorkerGroupMap = std::unordered_map<String, HostWithPortsVec>;

    /// Get the main function name.
    virtual std::string getName() const = 0;

    /// Get the mode of Service Discovery
    virtual ServiceDiscoveryMode getType() const = 0;

    /// Get the cluster name
    std::string getClusterName() const { return cluster; }

    virtual HostWithPortsVec lookup(const String & psm_name, ComponentType type, const String & vw_name = "") = 0;

    virtual WorkerGroupMap lookupWorkerGroupsInVW([[maybe_unused]]const String & psm_name, [[maybe_unused]]const String & vw_name)
    {
        return {}; /// TODO: make it virtual
    }

    virtual ~IServiceDiscovery() = default;

protected:
    String cluster = "default";

    bool cache_disabled = false;
    UInt32 cache_timeout = 5;  /// seconds
};

using ServiceDiscoveryClientPtr = std::shared_ptr<IServiceDiscovery>;

}
