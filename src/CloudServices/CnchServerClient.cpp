#include <CloudServices/CnchServerClient.h>

#include <Protos/cnch_server_rpc.pb.h>

#include <brpc/channel.h>
#include <brpc/controller.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BRPC_CANNOT_INIT_CHANNEL;
    extern const int NOT_IMPLEMENTED;
}

CnchServerClient::CnchServerClient(String host_port_)
    : RpcClientBase(getName(), std::move(host_port_)), stub(std::make_unique<Protos::CnchServerService_Stub>(&getChannel()))
{
}

CnchServerClient::CnchServerClient(HostWithPorts host_ports_)
    : RpcClientBase(getName(), std::move(host_ports_)), stub(std::make_unique<Protos::CnchServerService_Stub>(&getChannel()))
{
}

CnchServerClient::~CnchServerClient()
{
}

}
