#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <boost/noncopyable.hpp>
#include <butil/logging.h>
#include <Poco/AutoPtr.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>
#include <Common/Brpc/BaseConfigHolder.h>
#include <common/logger_useful.h>

namespace DB
{
/// The BrpcApplication class initializes the logging and configs for brpc framework;
///  It also manager the lifecycle of brpc servers and clients;
/// Singleton class, use std::cout for log
class BrpcApplication : public boost::noncopyable
{
public:
    using ConfigHolderMap = std::unordered_map<std::string, std::shared_ptr<BaseConfigHolder>>;
    ~BrpcApplication();
    static BrpcApplication & getInstance();
    static inline std::string prefix{"brpc"};
    void initialize(const RawConfig & app_conf);
    void reloadConfig(const RawConfig & app_conf);
    template <typename T>
    void registerNamedConfigHolder(std::shared_ptr<T> named_config_holder_ptr);

    template <typename T>
    std::shared_ptr<T> getConfigHolderByType();

private:
    ::logging::LogSink * old_sink;
    Poco::Logger * logger;
    ConfigHolderMap config_holder_map;
    mutable std::mutex holder_map_mutex;

private:
    BrpcApplication();
    void initBrpcLog();
    void initBuildinConfigHolders();
    ConfigHolderMap snapshotConfigHolderMap();
};

template <typename T>
void BrpcApplication::registerNamedConfigHolder(std::shared_ptr<T> named_config_holder_ptr)
{
    std::lock_guard<std::mutex> guard(holder_map_mutex);
    if (config_holder_map.find(T::name) != config_holder_map.end())
    {
        throw Poco::InvalidArgumentException(Poco::format("Duplicated NamedConfigHolder {}!", T::name));
    }
    config_holder_map.emplace(T::name, std::static_pointer_cast<BaseConfigHolder>(named_config_holder_ptr));
}

template <typename T>
std::shared_ptr<T> BrpcApplication::getConfigHolderByType()
{
    std::lock_guard<std::mutex> guard(holder_map_mutex);
    std::shared_ptr<BaseConfigHolder> configHolder = config_holder_map[T::name];
    if (!configHolder)
    {
        throw Poco::InvalidArgumentException(Poco::format("NamedConfigHolder {} does not exist!", T::name));
    }
    return std::dynamic_pointer_cast<T>(configHolder);
}

}
