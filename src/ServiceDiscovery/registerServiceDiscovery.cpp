#include <ServiceDiscovery/registerServiceDiscovery.h>
#include <ServiceDiscovery/ServiceDiscoveryFactory.h>

namespace DB
{

void registerServiceDiscoveryLocal(ServiceDiscoveryFactory & factory);
void registerServiceDiscoveryConsul(ServiceDiscoveryFactory & factory);
void registerServiceDiscoveryDNS(ServiceDiscoveryFactory & factory);

void registerServiceDiscovery()
{
    auto & factory = ServiceDiscoveryFactory::instance();

    registerServiceDiscoveryLocal(factory);
    registerServiceDiscoveryConsul(factory);
    registerServiceDiscoveryDNS(factory);
}

}
