#include <string>
#include <fmt/core.h>
#include <Common/Brpc/BrpcChannelConfigHolder.h>

namespace DB
{

String BrpcChannelConfigHolder::dumpConfig(const brpc::ChannelOptions & options)
{
    return fmt::format(
        "BrpcOptions[type={}, connect_timeout_ms={}, timeout_ms={}",
        options.connection_type.name(),
        options.connect_timeout_ms,
        options.timeout_ms);
}

void BrpcChannelConfigHolder::afterInit(const brpc::ChannelOptions * conf_ptr)
{
    LOG_INFO(logger, "Init brpc channel with config: " + dumpConfig(*conf_ptr));
}

std::unique_ptr<brpc::ChannelOptions> BrpcChannelConfigHolder::createTypedConfig(RawConfAutoPtr conf_ptr) noexcept
{
    auto option_unique_ptr = std::make_unique<brpc::ChannelOptions>();
    option_unique_ptr->connection_type = conf_ptr->getString("connection_type", "pooled");
    option_unique_ptr->connect_timeout_ms = conf_ptr->getInt("connect_timeout_ms", 1000);
    option_unique_ptr->timeout_ms = conf_ptr->getInt("timeout_ms", 3000);
    return option_unique_ptr;
}

void BrpcChannelConfigHolder::onChange(const brpc::ChannelOptions * old_conf_ptr, const brpc::ChannelOptions * new_conf_ptr)
{
    LOG_INFO(logger, "Channel config changed from {} \n to {}", dumpConfig(*old_conf_ptr), dumpConfig(*new_conf_ptr));
}

bool BrpcChannelConfigHolder::hasChanged(const brpc::ChannelOptions * old_conf_ptr, const brpc::ChannelOptions * new_conf_ptr)
{
    bool changed = !(
        old_conf_ptr && static_cast<brpc::ConnectionType>(old_conf_ptr->connection_type) == static_cast<brpc::ConnectionType>(new_conf_ptr->connection_type)
        && old_conf_ptr->connect_timeout_ms == new_conf_ptr->connect_timeout_ms && old_conf_ptr->timeout_ms == new_conf_ptr->timeout_ms);
    return changed;
}

}
