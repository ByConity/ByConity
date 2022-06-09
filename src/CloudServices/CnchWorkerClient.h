#pragma once

#include <CloudServices/RpcClientBase.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{
namespace Protos
{
    class CnchWorkerService_Stub;
}

class CnchWorkerClient : public RpcClientBase
{
public:
    static String getName() { return "CnchWorkerClient"; }

    CnchWorkerClient(String host_port_);
    CnchWorkerClient(HostWithPorts host_ports_);
    ~CnchWorkerClient() override;

private:
    std::unique_ptr<Protos::CnchWorkerService_Stub> stub;
};

using CnchWorkerClientPtr = std::shared_ptr<CnchWorkerClient>;
using CnchWorkerClients = std::vector<CnchWorkerClientPtr>;


}
