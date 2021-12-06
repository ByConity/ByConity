#include "RpcClient.h"

#include <errno.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BRPC_CANNOT_INIT_CHANNEL;
    extern const int BRPC_EXCEPTION;
}

RpcClient::RpcClient(String host_port_, brpc::ChannelOptions * options)
    : log(&Poco::Logger::get("RpcClient")), host_port(std::move(host_port_)), brpc_channel(std::make_unique<brpc::Channel>())
{
    initChannel(*brpc_channel, host_port, options);
}

void RpcClient::assertController(const brpc::Controller & cntl)
{
    if (cntl.Failed())
    {
        auto err = cntl.ErrorCode();
        if (err == EHOSTDOWN || err == ECONNREFUSED || err == ECONNRESET || err == ENETUNREACH || err == ENOTCONN)
        {
            ok_.store(false, std::memory_order_relaxed);
        }
        throw Exception("RpcClient exception happen-" + std::to_string(err) + ":" + cntl.ErrorText(), ErrorCodes::BRPC_EXCEPTION);
    }
    else
    {
        ok_.store(true, std::memory_order_relaxed);
    }
}

void RpcClient::initChannel(brpc::Channel & channel_, const String host_port_, brpc::ChannelOptions * options)
{
    if (0 != channel_.Init(host_port_.c_str(), options))
        throw Exception("Failed to initialize RPC channel to " + host_port_, ErrorCodes::BRPC_CANNOT_INIT_CHANNEL);

    LOG_TRACE(log, "Create rpc channel listening on : {}", host_port_);
}

}
