
#include <CloudServices/RpcClientBase.h>

namespace brpc
{
    class Channel;
    class ChannelOptions;
    class Controller;
}

namespace DB
{
class RpcLeaderClientBase : public RpcClientBase
{
public:
    RpcLeaderClientBase(const String & log_prefix,
                        const String & host_port,
                        brpc::ChannelOptions * options_ = nullptr)
        :RpcClientBase(log_prefix, host_port, options_)
        ,options(options_)
    {}

    RpcLeaderClientBase(const String & log_prefix,
                        HostWithPorts host_ports_,
                        brpc::ChannelOptions * options_ = nullptr)
        :RpcClientBase(log_prefix, host_ports_, options_)
        , options(options_)
    {}


protected:
    brpc::Channel & updateChannel(const String & host_port, brpc::ChannelOptions * options_ = nullptr);
    std::mutex host_port_mutex;
    String leader_host_port;
    brpc::ChannelOptions * options;

};
}
