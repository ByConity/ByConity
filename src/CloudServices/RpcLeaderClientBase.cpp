#include <CloudServices/RpcLeaderClientBase.h>
#include <Common/Exception.h>
#include <brpc/channel.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BRPC_CANNOT_INIT_CHANNEL;
}

brpc::Channel & RpcLeaderClientBase::updateChannel(const String & host_port, brpc::ChannelOptions * options_)
{
    std::lock_guard lock(host_port_mutex);
    leader_host_port = host_port;
    if (0 != channel->Init(leader_host_port.c_str(), options_ ? options_ : default_options.get()))
        throw Exception("Failed to update RPC channel to " + leader_host_port, ErrorCodes::BRPC_CANNOT_INIT_CHANNEL);

    return getChannel();
}

}
