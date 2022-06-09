#include <CloudServices/CnchWorkerClient.h>

#include <Protos/cnch_worker_rpc.pb.h>

#include <brpc/channel.h>
#include <brpc/controller.h>

namespace DB
{

CnchWorkerClient::CnchWorkerClient(String host_port_)
    : RpcClientBase(getName(), std::move(host_port_)), stub(std::make_unique<Protos::CnchWorkerService_Stub>(&getChannel()))
{
}

CnchWorkerClient::CnchWorkerClient(HostWithPorts host_ports_)
    : RpcClientBase(getName(), std::move(host_ports_)), stub(std::make_unique<Protos::CnchWorkerService_Stub>(&getChannel()))
{
}

CnchWorkerClient::~CnchWorkerClient()
{
}

}
