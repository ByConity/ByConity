#pragma once

#include <atomic>
#include <cstddef>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/DataTransKey.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/IBroadcastReceiver.h>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include <boost/noncopyable.hpp>
#include <Poco/Logger.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <common/types.h>
#include <Processors/Exchange/DataTrans/Local/LocalChannelOptions.h>

namespace DB
{
class LocalBroadcastChannel final : public IBroadcastReceiver, public IBroadcastSender, boost::noncopyable
{
public:

    virtual RecvDataPacket recv(UInt32 timeout_ms) override;
    virtual void registerToSenders(UInt32 timeout_ms) override;
    virtual BroadcastStatus finish(BroadcastStatusCode status_code, String message) override;
    virtual BroadcastStatus send(Chunk && chunk) override;
    virtual void waitAllReceiversReady(UInt32 /*timeout_ms*/) override { }
    virtual ~LocalBroadcastChannel() override;

private:
    friend class LocalBroadcastRegistry;
    
    explicit LocalBroadcastChannel(const DataTransKey & data_key_, LocalChannelOptions options_);

    String data_key_id;
    LocalChannelOptions options;
    ConcurrentBoundedQueue<Chunk> receive_queue;
    std::atomic<BroadcastStatus *> broadcast_status;
    Poco::Logger * logger;
};
}
