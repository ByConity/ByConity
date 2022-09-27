#include <ServiceDiscovery/ServiceDiscoveryFactory.h>
#include <ServiceDiscovery/ServiceDiscoveryConsul.h>
#include <Poco/String.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
}

void ServiceDiscoveryFactory::registerServiceDiscoveryType(const String & sd_type, DB::ServiceDiscoveryFactory::Creator creator)
{
    if (!registry.emplace(sd_type, creator).second)
        throw Exception("ServiceDiscoveryFactory: the sd type '" + sd_type + "' is not unique", ErrorCodes::LOGICAL_ERROR);
}

ServiceDiscoveryClientPtr ServiceDiscoveryFactory::create(const Poco::Util::AbstractConfiguration & config)
{
    const auto sd_type = config.getRawString("service_discovery.mode","local");

    const auto found = registry.find(sd_type);
    if (found == registry.end())
        throw Exception{"ServiceDiscoveryFactory: unknown sd type: " + sd_type, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG};

    const auto & sd_creator = found->second;

    ServiceDiscoveryClientPtr sd = sd_creator(config);

    if (!sd_clients.emplace(sd->getType(), sd).second)
        throw Exception("ServiceDiscoveryFactory: try to create a duplicate " + sd_type + " service discovery client", ErrorCodes::LOGICAL_ERROR);

    /// We need SD Consul client for handling consul usage, i.e. for nnproxy, kms
    if (sd->getType() != ServiceDiscoveryMode::CONSUL)
    {
        ServiceDiscoveryClientPtr sd_consul = std::make_shared<ServiceDiscoveryConsul>(config);
        if (!sd_clients.emplace(sd_consul->getType(), sd_consul).second)
            throw Exception("ServiceDiscoveryFactory: try to create a duplicate " + sd_type + " service discovery client", ErrorCodes::LOGICAL_ERROR);
    }

    return sd;
}

ServiceDiscoveryClientPtr ServiceDiscoveryFactory::get(const ServiceDiscoveryMode & mode) const
{
    auto it = sd_clients.find(mode);
    if (it == sd_clients.end())
        throw Exception("No available service discovery client for " + typeToString(mode) + " mode", ErrorCodes::LOGICAL_ERROR);

    return it->second;
}

ServiceDiscoveryClientPtr ServiceDiscoveryFactory::get(const Poco::Util::AbstractConfiguration & config) const
{
    const auto sd_type = config.getRawString("service_discovery.mode", "LOCAL");
    return get(toServiceDiscoveryMode(Poco::toUpper(sd_type)));
}

ServiceDiscoveryClientPtr ServiceDiscoveryFactory::tryGet(const ServiceDiscoveryMode & mode) const
{
    auto it = sd_clients.find(mode);
    if (it == sd_clients.end())
        return nullptr;

    return it->second;
}

}
