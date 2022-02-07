#include <memory>
#include <mutex>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxy.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>
#include <common/logger_useful.h>

namespace DB
{
BroadcastSenderProxyRegistry::BroadcastSenderProxyRegistry() : logger(&Poco::Logger::get("BroadcastSenderProxyRegistry"))
{
}

BroadcastSenderProxyPtr BroadcastSenderProxyRegistry::getOrCreate(DataTransKeyPtr data_key)
{
    const String & key = data_key->getKey();

    std::lock_guard lock(mutex);
    auto it = proxies.find(key);
    if (it != proxies.end())
    {
        auto channel_ptr = it->second.lock();
        if (channel_ptr)
            return channel_ptr;
    }

    LOG_TRACE(logger, "Register sender proxy {} ", data_key->dump());
    auto channel_ptr = std::shared_ptr<BroadcastSenderProxy>(new BroadcastSenderProxy(std::move(data_key)));
    proxies.emplace(key, BroadcastSenderProxyEntry(channel_ptr));
    return channel_ptr;
}

void BroadcastSenderProxyRegistry::remove(DataTransKeyPtr data_key)
{
    std::lock_guard lock(mutex);
    auto result = proxies.erase(data_key->getKey());
    LOG_TRACE(logger, "remove proxy {} with result: {} ", data_key->dump(), result);
}

size_t BroadcastSenderProxyRegistry::countProxies()
{
    std::lock_guard lock(mutex);
    return proxies.size();
}

}
