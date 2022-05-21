#pragma once
#include <ServiceDiscovery/IServiceDiscovery.h>
#include <memory>
#include <Poco/Util/AbstractConfiguration.h>
#include <map>
#include <common/logger_useful.h>


namespace DB
{

struct Endpoint{
    String host;
    String hostname;
    std::map<String,String> ports;
    String virtual_warehouse = "vw_default";
    String worker_group;
};

using Endpoints = std::vector<Endpoint>;
class ServiceDiscoveryLocal : public IServiceDiscovery
{
public:
    using LookupTable = std::map<String, Endpoints>;

    ServiceDiscoveryLocal(const Poco::Util::AbstractConfiguration & config);

    void loadConfig(const Poco::Util::AbstractConfiguration & config);

    std::string getName() const override { return "local"; }

    ServiceDiscoveryMode getType() const override { return ServiceDiscoveryMode::LOCAL; }

    HostWithPortsVec lookup(const String & psm_name, ComponentType type, const String & vw_name = "") override;

    WorkerGroupMap lookupWorkerGroupsInVW(const String & psm, const String & vw_name) override;

private:
    LookupTable table;

    bool passCheckVwName(const Endpoint & e, const String & vw_name);

    bool exists(const String & name);
    
    void initService(const Poco::Util::AbstractConfiguration & config, const String & name);

    std::map<String, String> initPortsMap(const Poco::Util::AbstractConfiguration & config, const String & name);

    String toString(const Endpoint & e, const String & tag);
};

}
