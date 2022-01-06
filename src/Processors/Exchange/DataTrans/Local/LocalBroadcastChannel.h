#pragma once

#include <atomic>
#include <cstddef>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/DataTransKey.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/IBroadcastReceiver.h>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include <Processors/Exchange/DataTrans/Local/LocalChannelOptions.h>
#include <boost/noncopyable.hpp>
#include <Poco/Logger.h>
#include "Common/Exception.h"
#include <Common/ConcurrentBoundedQueue.h>
#include <common/types.h>

namespace DB
{
class LocalBroadcastChannel final : public IBroadcastReceiver, public IBroadcastSender, public std::enable_shared_from_this<LocalBroadcastChannel>, boost::noncopyable
{
public:
    explicit LocalBroadcastChannel(DataTransKeyPtr data_key_, LocalChannelOptions options_);
    virtual RecvDataPacket recv(UInt32 timeout_ms) override;
    virtual void registerToSenders(UInt32 timeout_ms) override;
    virtual void merge(IBroadcastSender &&) override;
    virtual String getName() const override;
    virtual BroadcastStatus finish(BroadcastStatusCode status_code, String message) override;
    virtual BroadcastStatus send(Chunk chunk) override;
    virtual ~LocalBroadcastChannel() override;

private:
    DataTransKeyPtr data_key;
    LocalChannelOptions options;
    ConcurrentBoundedQueue<Chunk> receive_queue;
    std::atomic<BroadcastStatus *> broadcast_status;
    Poco::Logger * logger;
};
}
