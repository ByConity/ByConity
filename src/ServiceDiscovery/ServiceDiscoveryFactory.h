#pragma once

#include <Core/Types.h>
#include <ServiceDiscovery/IServiceDiscovery.h>

#include <functional>
#include <unordered_map>
#include <common/singleton.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{
class Context;

/**
 * ServiceDiscovery factory. Responsible for creating new service discovery objects.
 */
class ServiceDiscoveryFactory : public ext::singleton<ServiceDiscoveryFactory>
{
public:
    using Creator = std::function<ServiceDiscoveryClientPtr(const Poco::Util::AbstractConfiguration & config)>;

    void registerServiceDiscoveryType(const String & sd_type, Creator creator);

    ServiceDiscoveryClientPtr create(const Poco::Util::AbstractConfiguration & config);
    ServiceDiscoveryClientPtr get(const ServiceDiscoveryMode & mode) const;
    ServiceDiscoveryClientPtr tryGet(const ServiceDiscoveryMode & mode) const;
    ServiceDiscoveryClientPtr get(const Poco::Util::AbstractConfiguration & config) const;

private:
    using ServiceDiscoveryRegistry = std::unordered_map<String, Creator>;
    ServiceDiscoveryRegistry registry;

    using ServiceDiscoveryClients = std::unordered_map<ServiceDiscoveryMode, ServiceDiscoveryClientPtr>;
    ServiceDiscoveryClients sd_clients;
};

}
