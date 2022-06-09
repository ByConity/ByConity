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
TSOClient::TSOClient(String host_port)
    : RpcClientBase(getName(), std::move(host_port)), stub(std::make_unique<TSO_Stub>(&getChannel()))
{
}

TSOClient::TSOClient(HostWithPorts host_ports)
    : RpcClientBase(getName(), std::move(host_ports)), stub(std::make_unique<TSO_Stub>(&getChannel()))
{
}

TSOClient::~TSOClient() { }

GetTimestampResp TSOClient::getTimestamp()
{
    GetTimestampReq req;
    GetTimestampResp resp;
    brpc::Controller cntl;

    stub->GetTimestamp(&cntl, &req, &resp, NULL);

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
    stub->GetTimestamps(&cntl, &req, &resp, NULL);

    assertController(cntl);
    RPCHelpers::checkResponse(resp);

    return resp;
}
}

}
