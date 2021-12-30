#pragma once

#include <Core/Block.h>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/DataTransKey.h>
#include <Processors/Exchange/DataTrans/DataTransStruct.h>
#include <brpc/stream.h>
#include <Poco/Logger.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/IBroadcastReceiver.h>

namespace DB
{
class BrpcRemoteBroadcastReceiver : public std::enable_shared_from_this<BrpcRemoteBroadcastReceiver>, public IBroadcastReceiver
{
public:
    BrpcRemoteBroadcastReceiver(DataTransKeyPtr trans_key_, String registry_address_, ContextPtr context_, Block header_);
    ~BrpcRemoteBroadcastReceiver() override;

    void registerToSenders(UInt32 timeout_ms) override;
    RecvDataPacket recv(UInt32 timeout_ms) noexcept override;
    BroadcastStatus finish(BroadcastStatusCode status_code_, String message) override;
    void pushReceiveQueue(Chunk & chunk);
    void pushException(const String & exception);
    void clearQueue() { queue->receive_queue->clear(); }
    Block getHeader() { return header; }

private:
    Poco::Logger * log = &Poco::Logger::get("BrpcRemoteBroadcastReceiver");
    DataTransKeyPtr trans_key;
    String registry_address;
    ContextPtr context;
    Block header;
    // todo::aron add MemoryTracker here
    // std::shared_ptr<MemoryTracker> memory_tracker = std::make_shared<MemoryTracker>(VariableContext::Global);
    ReceiveQueuePtr queue = std::make_unique<ReceiveQueue>();
    String data_key;
    brpc::StreamId stream_id{brpc::INVALID_STREAM_ID};
    std::atomic<BroadcastStatus *> broadcast_status;
};

using BrpcRemoteBroadcastReceiverShardPtr = std::shared_ptr<BrpcRemoteBroadcastReceiver>;
using BrpcRemoteBroadcastReceiverWeakPtr = std::weak_ptr<BrpcRemoteBroadcastReceiver>;
}
