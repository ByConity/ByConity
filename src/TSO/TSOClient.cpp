#include <TSO/TSOClient.h>

#include <Protos/tso.pb.h>
#include <Protos/RPCHelpers.h>
#include <TSO/TSOImpl.h>

#include <brpc/channel.h>
#include <brpc/controller.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TSO_INTERNAL_ERROR;
}

namespace TSO
{
TSOClient::TSOClient(String host_port_)
    : RpcClientBase(getName(), std::move(host_port_)), stub(std::make_unique<TSO_Stub>(&getChannel()))
{
}

TSOClient::TSOClient(HostWithPorts host_ports_)
    : RpcClientBase(getName(), std::move(host_ports_)), stub(std::make_unique<TSO_Stub>(&getChannel()))
{
}

TSOClient::~TSOClient() { }

GetTimestampResp TSOClient::getTimestamp()
{
    GetTimestampReq req;
    GetTimestampResp resp;
    brpc::Controller cntl;

    stub->GetTimestamp(&cntl, &req, &resp, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(resp);

    return resp;
}

GetTimestampsResp TSOClient::getTimestamps(UInt32 size)
{
    GetTimestampsReq req;
    GetTimestampsResp resp;
    brpc::Controller cntl;

    req.set_size(size);
    stub->GetTimestamps(&cntl, &req, &resp, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(resp);

    return resp;
}
}

}
