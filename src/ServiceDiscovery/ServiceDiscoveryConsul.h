#pragma once
#include <ServiceDiscovery/IServiceDiscovery.h>
#include <ServiceDiscovery/ServiceDiscoveryCache.h>
#include <consul/discovery.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <memory>
#include <Poco/Logger.h>

namespace DB
{

class ServiceDiscoveryConsul : public IServiceDiscovery
{
public:
    using Endpoint = cpputil::consul::ServiceEndpoint;
    using Endpoints = std::vector<Endpoint>;

    ServiceDiscoveryConsul(const Poco::Util::AbstractConfiguration & config);

    std::string getName() const override { return "consul"; }

    ServiceDiscoveryMode getType() const override { return ServiceDiscoveryMode::CONSUL; }

    HostWithPortsVec lookup(const String & psm_name, ComponentType type, const String & vw_name = "") override;

    WorkerGroupMap lookupWorkerGroupsInVW(const String & psm, const String & vw_name) override;

    // format results
    static HostWithPortsVec formatResult(const Endpoints & eps, ComponentType type);

private:
    Poco::Logger * log = &Poco::Logger::get("ServiceDiscoveryConsul");

    ServiceDiscoveryCache<Endpoint> cache;

    bool passCheckCluster(const Endpoint & e);
    bool passCheckVwName(const Endpoint & e, const String & vw_name);

    // real interface
    Endpoints fetchEndpoints(const String & psm_name, const String & vw_name);
    Endpoints fetchEndpointsFromCache(const String & psm_name, const String & vw_name);
    Endpoints fetchEndpointsFromUpstream(const String & psm_name, const String & vw_name);

public:
    // fake interface, processing logics are same, but endpoints are faked.
    // for TEST ONLY!!!
    Endpoints fakedFetchEndpointsFromUpstream(const Endpoints & eps, const String & vw_name);
    Endpoints fakedFetchEndpointsFromCache(const Endpoints & eps, const String & psm_name, const String & vw_name);
    Endpoints fakedFetchEndpoints(const Endpoints & eps, const String & psm_name, const String & vw_name);

    void clearCache() { cache.clear(); }

    HostWithPortsVec fakedLookup(const Endpoints & eps, const String & psm_name, ComponentType type, const String & vw_name = "");
};

}
