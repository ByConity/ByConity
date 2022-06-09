#pragma once

#include <CloudServices/RpcClientBase.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{
namespace Protos
{
    class CnchServerService_Stub;
}

class CnchServerClient : public RpcClientBase
{
public:
    static String getName() { return "CnchServerClient"; }

    CnchServerClient(String host_port_);
    CnchServerClient(HostWithPorts host_ports_);

    ~CnchServerClient() override;

private:
    std::unique_ptr<Protos::CnchServerService_Stub> stub;
};

using CnchServerClientPtr = std::shared_ptr<CnchServerClient>;

}
