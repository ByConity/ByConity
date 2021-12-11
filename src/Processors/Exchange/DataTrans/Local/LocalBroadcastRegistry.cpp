#include <memory>
#include <mutex>
#include <string>
#include <Processors/Exchange/DataTrans/Local/LocalBroadcastChannel.h>
#include <Processors/Exchange/DataTrans/Local/LocalBroadcastRegistry.h>
#include <Processors/Exchange/DataTrans/Local/LocalChannelOptions.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>

namespace DB
{
BroadcastSenderPtr LocalBroadcastRegistry::getOrCreateChannelAsSender(const DataTransKey & data_key, LocalChannelOptions options_)
{
    const String & key = data_key.getKey();
    std::unique_lock<std::mutex> lock{channels_mutex};
    auto it = channels_map.find(key);
    if (it != channels_map.end())
    {
        auto channel_ptr = it->second.lock();
        if (channel_ptr)
            return std::dynamic_pointer_cast<IBroadcastSender>(channel_ptr);
    }

    LOG_TRACE(logger, "Register channel {} ", key);
    auto channel_ptr = std::shared_ptr<LocalBroadcastChannel>(new LocalBroadcastChannel(data_key, options_));
    channels_map.emplace(key, LocalBroadcastChannelWeakPtr(channel_ptr));
    return std::dynamic_pointer_cast<IBroadcastSender>(channel_ptr);
}


BroadcastReceiverPtr LocalBroadcastRegistry::getOrCreateChannelAsReceiver(const DataTransKey & data_key, LocalChannelOptions options_)
{
    const String & key = data_key.getKey();
    std::unique_lock<std::mutex> lock{channels_mutex};
    auto it = channels_map.find(key);
    if (it != channels_map.end())
    {
        auto channel_ptr = it->second.lock();
        if (channel_ptr)
            return std::dynamic_pointer_cast<IBroadcastReceiver>(channel_ptr);
    }

    LOG_TRACE(logger, "Register channel {} ", key);
    auto channel_ptr = std::shared_ptr<LocalBroadcastChannel>(new LocalBroadcastChannel(data_key, options_));
    channels_map.emplace(key, LocalBroadcastChannelWeakPtr(channel_ptr));
    return std::dynamic_pointer_cast<IBroadcastReceiver>(channel_ptr);
}

void LocalBroadcastRegistry::clearChannel(const String & data_key_id)
{
    std::unique_lock<std::mutex> lock{channels_mutex};
    auto result = channels_map.erase(data_key_id);
    LOG_TRACE(logger, "Clear channel {} with result: {} ", data_key_id, result);
}


LocalBroadcastRegistry::LocalBroadcastRegistry() : logger(&Poco::Logger::get("LocalBroadcastRegistry"))
{
}

size_t LocalBroadcastRegistry::countChannel()
{
    std::unique_lock<std::mutex> lock{channels_mutex};
    return channels_map.size();
}

}
