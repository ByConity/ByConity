#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>
#include <boost/noncopyable.hpp>
#include <common/types.h>
#include <Poco/Logger.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/DataTransKey.h>
#include <Processors/Exchange/DataTrans/Local/LocalChannelOptions.h>

namespace DB
{
class LocalBroadcastChannel;
using LocalBroadcastChannelWeakPtr = std::weak_ptr<LocalBroadcastChannel>;
class LocalBroadcastRegistry final: private boost::noncopyable
{
public:
    static LocalBroadcastRegistry & getInstance()
    {
        static LocalBroadcastRegistry instance;
        return instance;
    }

    BroadcastSenderPtr getOrCreateChannelAsSender(const DataTransKey & data_key, LocalChannelOptions options_);
    BroadcastReceiverPtr getOrCreateChannelAsReceiver(const DataTransKey & data_key, LocalChannelOptions options_);
    size_t countChannel();
private:
    friend class LocalBroadcastChannel;
    
    LocalBroadcastRegistry();
    void clearChannel(const String & data_key_id);

    mutable std::mutex channels_mutex;
    std::unordered_map<String, LocalBroadcastChannelWeakPtr> channels_map;
    Poco::Logger * logger;
};

}
