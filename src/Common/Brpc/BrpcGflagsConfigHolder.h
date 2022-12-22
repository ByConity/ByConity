#pragma once
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Brpc/SealedConfigHolder.h>
#include <common/logger_useful.h>

namespace DB
{
/// Config example:
/// <brpc>
///    <gflags>
///        <event_dispatcher_num>10</event_dispatcher_num>
///        <defer_close_second>13</defer_close_second>
///    </gflags>
///</brpc>
class BrpcGflagsConfigHolder : public SealedConfigHolder<BrpcGflagsConfigHolder, RawConfig, RawConfigDeleter>
{
public:
    static inline std::string name{"gflags"};
    explicit BrpcGflagsConfigHolder() { logger = &Poco::Logger::get("BrpcGflagsConfigHolder"); }
    void afterInit(const RawConfig * config_ptr) override;
    void onChange(const RawConfig * old_conf_ptr, const RawConfig * new_conf_ptr) override;
    bool hasChanged(const RawConfig * old_conf_ptr, const RawConfig * new_conf_ptr) override;

private:
    Poco::Logger * logger;
};

}
