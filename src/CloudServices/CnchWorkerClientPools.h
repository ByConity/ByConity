#pragma once
#include <CloudServices/CnchWorkerClient.h>
#include <ServiceDiscovery/IServiceDiscovery.h>
#include <Common/RpcClientPool.h>
#include <ResourceManagement/VirtualWarehouseType.h>

namespace DB
{

using CnchWorkerClientPool = RpcClientPool<CnchWorkerClient>;
using CnchWorkerClientPoolPtr = std::shared_ptr<CnchWorkerClientPool>;
using ResourceManagement::VirtualWarehouseType;
using ResourceManagement::VirtualWarehouseTypes;

class CnchWorkerClientPools
{
public:
    CnchWorkerClientPools(ServiceDiscoveryClientPtr sd_) : sd(std::move(sd_)) { }
    ~CnchWorkerClientPools() { }

    void setDefaultPSM(String psm) { default_psm = std::move(psm); }

    void addVirtualWarehouse(const String & name, const String & psm, VirtualWarehouseTypes vw_types);
    void removeVirtualWarehouse(const String & name);

    CnchWorkerClientPoolPtr getPool(VirtualWarehouseType vw_type);
    CnchWorkerClientPoolPtr getPool(const Strings & names, VirtualWarehouseTypes vw_types);
    CnchWorkerClientPoolPtr getPool(const String & name);

    CnchWorkerClientPtr getWorker(const HostWithPorts & host_ports);

private:
    void addVirtualWarehouseImpl(const String & name, const String & psm, VirtualWarehouseTypes vw_types, std::lock_guard<std::mutex> &);

    ServiceDiscoveryClientPtr sd;
    String default_psm;

    std::mutex pools_mutex;
    std::unordered_map<String, CnchWorkerClientPoolPtr> pools;
    std::array<CnchWorkerClientPoolPtr, size_t(VirtualWarehouseType::Num)> default_pools;
};

};
