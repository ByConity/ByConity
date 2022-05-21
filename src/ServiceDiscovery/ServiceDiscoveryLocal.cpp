#include <ServiceDiscovery/ServiceDiscoveryLocal.h>

#include <IO/ReadHelpers.h>
#include <ServiceDiscovery/ServiceDiscoveryFactory.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/StringUtils/StringUtils.h>

#include <sstream>
#include <consul/discovery.h>

namespace ProfileEvents
{
extern const Event SDRequest;
extern const Event SDRequestFailed;
extern const Event SDRequestUpstream;
extern const Event SDRequestUpstreamFailed;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int SD_EMPTY_ENDPOINTS;
    extern const int SD_INVALID_TAG;
    extern const int SD_PSM_NOT_EXISTS;
    extern const int SD_UNKOWN_LB_STRATEGY;
}


ServiceDiscoveryLocal::ServiceDiscoveryLocal(const Poco::Util::AbstractConfiguration & config)
{
    loadConfig(config);
}

HostWithPortsVec ServiceDiscoveryLocal::lookup(const String & psm_name, ComponentType type, const String & vw_name)
{
    if (!exists(psm_name))
        throw Exception("psm:" + psm_name + " not exists in service registry.", ErrorCodes::SD_PSM_NOT_EXISTS);

    HostWithPortsVec res;
    Endpoints endpoints = table[psm_name];

    if (type == ComponentType::SERVER || type == ComponentType::WORKER)
    {
        for (auto & ep : endpoints)
        {
            if (type == ComponentType::WORKER && !vw_name.empty() && ep.virtual_warehouse != vw_name)
                continue;

            res.emplace_back();
            res.back().id = ep.hostname;
            res.back().host = ep.host;
            if (ep.ports.count("PORT0"))
                res.back().tcp_port = parse<UInt16>(ep.ports["PORT0"]);
            if (ep.ports.count("PORT1"))
                res.back().rpc_port = parse<UInt16>(ep.ports["PORT1"]);
            if (ep.ports.count("PORT2"))
                res.back().http_port = parse<UInt16>(ep.ports["PORT2"]);
            if (ep.ports.count("PORT5"))
                res.back().exchange_port = parse<UInt16>(ep.ports["PORT5"]);
            if (ep.ports.count("PORT6"))
                res.back().exchange_status_port = parse<UInt16>(ep.ports["PORT6"]);
        }
    }
    else if (type == ComponentType::TSO || type == ComponentType::DAEMON_MANAGER)
    {
        for (auto & ep : endpoints)
        {
            res.emplace_back();
            res.back().id = ep.hostname;
            res.back().host = ep.host;
            if (ep.ports.count("PORT0"))
                res.back().rpc_port = parse<UInt16>(ep.ports["PORT0"]);
        }
    }

    return res;
}

IServiceDiscovery::WorkerGroupMap ServiceDiscoveryLocal::lookupWorkerGroupsInVW(const String & psm_name, const String & vw_name)
{
    if (!exists(psm_name))
        throw Exception("psm:" + psm_name + " not exists in service registry.", ErrorCodes::SD_PSM_NOT_EXISTS);

    IServiceDiscovery::WorkerGroupMap group_map;

    Endpoints endpoints = table[psm_name];
    for (auto & ep : endpoints)
    {
        if (ep.virtual_warehouse != vw_name)
            continue;

        auto & group = group_map[ep.worker_group];
        group.emplace_back();
        group.back().id = ep.hostname;
        group.back().host = ep.host;
        if (ep.ports.count("PORT0"))
            group.back().tcp_port = parse<UInt16>(ep.ports["PORT0"]);
        if (ep.ports.count("PORT1"))
            group.back().rpc_port = parse<UInt16>(ep.ports["PORT1"]);
        if (ep.ports.count("PORT2"))
            group.back().http_port = parse<UInt16>(ep.ports["PORT2"]);
        if (ep.ports.count("PORT5"))
            group.back().exchange_port = parse<UInt16>(ep.ports["PORT5"]);
        if (ep.ports.count("PORT6"))
            group.back().exchange_status_port = parse<UInt16>(ep.ports["PORT6"]);
    }

    return group_map;
}

bool ServiceDiscoveryLocal::exists(const String & name)
{
    if (table.find(name) == table.end())
        return false;
    return true;
}

void ServiceDiscoveryLocal::loadConfig(const Poco::Util::AbstractConfiguration & config)
{
    table.clear(); // reset the lookuptable;

    if (!config.has("service_discovery"))
        return;

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys("service_discovery", keys);

    for (auto & key : keys)
    {
        if (key == "mode" || key == "cluster" || key == "disable_cache" || key == "cache_timeout")
            continue;
        initService(config, "service_discovery." + key);
    }
}

void ServiceDiscoveryLocal::initService(const Poco::Util::AbstractConfiguration & config, const String & name)
{
    if (!config.has(name))
        return;

    Poco::Util::AbstractConfiguration::Keys keys;
    String psm;
    Endpoints endpoints;

    config.keys(name, keys);
    for (auto & key : keys)
    {
        if (startsWith(key, "node"))
        {
            Endpoint endpoint = {
                config.getString(name + "." + key + ".host"),
                config.getString(name + "." + key + ".hostname"),
                initPortsMap(config, name + "." + key + ".ports"),
                config.getString(name + "." + key + ".vw_name", "vw_default"),
                config.getString(name + "." + key + ".wg_name", "wg_default"),
            };
            endpoints.push_back(endpoint);
        }
        else if (key == "psm")
        {
            psm = config.getString(name + "." + key);
        }
    }
    table[psm] = endpoints;
}

std::map<String, String> ServiceDiscoveryLocal::initPortsMap(const Poco::Util::AbstractConfiguration & config, const String & name)
{
    std::map<String, String> parts_map;

    if (config.has(name))
    {
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(name, keys);

        for (auto & key : keys)
        {
            if (startsWith(key, "port"))
            {
                String tag = config.getString(name + "." + key + ".name");
                String port = config.getString(name + "." + key + ".value");
                parts_map[tag] = port;
            }
        }
    }

    return parts_map;
}

void registerServiceDiscoveryLocal(ServiceDiscoveryFactory & factory)
{
    factory.registerServiceDiscoveryType(
        "local", [](const Poco::Util::AbstractConfiguration & config) { return std::make_shared<ServiceDiscoveryLocal>(config); });
}

}
