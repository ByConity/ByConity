#pragma once

#include <memory>
#include <ostream> 
#include <common/logger_useful.h>
#include <brpc/channel.h>
#include <brpc/options.pb.h>
#include <butil/containers/doubly_buffered_data.h>
#include <brpc/adaptive_connection_type.h>
#include <Common/Brpc/QueryableConfigHolder.h>

namespace DB
{
/// Config Example:
///< <brpc>
///    <channel>
///        <connection_type>single</connection_type>
///        <connect_timeout_ms>1000</connect_timeout_ms>
///        <timeout_ms>3000</timeout_ms>
///    </channel>
///</brpc>

class BrpcChannelConfigHolder : public QueryableConfigHolder<BrpcChannelConfigHolder, brpc::ChannelOptions>
{
public:
    explicit BrpcChannelConfigHolder() { logger = &Poco::Logger::get("BrpcChannelConfigHolder"); }
    static inline std::string name{"channel"};
    void afterInit(const brpc::ChannelOptions * conf_ptr) override;
    bool hasChanged(const brpc::ChannelOptions * old_conf_ptr, const brpc::ChannelOptions * new_conf_ptr) override;
    void onChange(const brpc::ChannelOptions * old_conf_ptr, const brpc::ChannelOptions * new_conf_ptr) override;
    std::unique_ptr<brpc::ChannelOptions> createTypedConfig(RawConfAutoPtr conf_ptr) noexcept override;
    static String dumpConfig(const brpc::ChannelOptions & options);

private:
    Poco::Logger * logger;
};

}
