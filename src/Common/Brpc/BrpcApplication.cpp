#include <Common/Brpc/BrpcApplication.h>
#include <Common/Brpc/BrpcChannelConfigHolder.h>
#include <Common/Brpc/BrpcGflagsConfigHolder.h>
#include <Common/Brpc/BrpcPocoLogSink.h>

namespace DB
{
BrpcApplication::BrpcApplication()
{
    logger = &Poco::Logger::get("BrpcApplication");

    //Init Brpc log
    initBrpcLog();

    //Init ConfigHolders
    initBuildinConfigHolders();
}

BrpcApplication::~BrpcApplication()
{
    std::lock_guard<std::mutex> guard(holder_map_mutex);
    config_holder_map.clear();
}

BrpcApplication & BrpcApplication::getInstance()
{
    static BrpcApplication singleton;
    return singleton;
}

void BrpcApplication::initBrpcLog()
{
    int bprc_log_priority = poco2BrpcLogPriority(logger->getLevel());
    ::logging::SetMinLogLevel(bprc_log_priority);
    ::logging::LogSink * poco_sink = new BrpcPocoLogSink();
    ::logging::LogSink * old_sink = ::logging::SetLogSink(poco_sink);
    delete old_sink;
}


void BrpcApplication::initialize(const RawConfig & app_conf)
{
    RawConfAutoPtr brpc_config = const_cast<RawConfig *>(app_conf.createView(BrpcApplication::prefix));
    auto holder_map = snapshotConfigHolderMap();

    for (const auto & item : holder_map)
    {
        item.second->init(brpc_config->createView(item.first));
    }
    LOG_INFO(logger, "Brpc is initialized with {} config holders", holder_map.size());
}

BrpcApplication::ConfigHolderMap BrpcApplication::snapshotConfigHolderMap()
{
    ConfigHolderMap holder_map;
    std::lock_guard<std::mutex> guard(holder_map_mutex);
    for (const auto & item : config_holder_map)
    {
        holder_map[item.first] = item.second;
    }
    return holder_map;
}


void BrpcApplication::reloadConfig(const RawConfig & app_conf)
{
    RawConfAutoPtr brpc_config = const_cast<RawConfig *>(app_conf.createView(BrpcApplication::prefix));

    auto holders_snapshot = snapshotConfigHolderMap();


    for (const auto & item : holders_snapshot)
    {
        item.second->reload(brpc_config->createView(item.first));
    }
    LOG_INFO(logger, "Reload brpc config with {} config holders", holders_snapshot.size());
}

void BrpcApplication::initBuildinConfigHolders()
{
    registerNamedConfigHolder(std::make_shared<BrpcGflagsConfigHolder>());
    registerNamedConfigHolder(std::make_shared<BrpcChannelConfigHolder>());
}


}
