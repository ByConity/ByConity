#pragma once

#include <atomic>
#include <Core/Types.h>
#include <boost/noncopyable.hpp>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <common/logger_useful.h>

namespace DB
{
class RpcClient : private boost::noncopyable
{
public:
    RpcClient(String host_port_, brpc::ChannelOptions * options = nullptr);
    ~RpcClient() = default;

    const auto & getAddress() const { return host_port; }
    bool ok() const { return ok_.load(std::memory_order_relaxed); }
    void reset() { ok_.store(true, std::memory_order_relaxed); }
    void checkAliveWithController(const brpc::Controller & cntl) noexcept;

    auto & getChannel() { return *brpc_channel; }

    void assertController(const brpc::Controller & cntl);

protected:
    void initChannel(brpc::Channel & channel_, const String host_port_, brpc::ChannelOptions * options = nullptr);

    Poco::Logger * log;
    String host_port;

    std::unique_ptr<brpc::Channel> brpc_channel;
    std::atomic_bool ok_{true};
};

}
