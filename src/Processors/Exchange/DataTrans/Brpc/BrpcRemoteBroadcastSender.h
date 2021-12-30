#pragma once

#include <atomic>
#include "BrpcExchangeRegistryCenter.h"

#include <Interpreters/Context.h>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/DataTransKey.h>
#include <Processors/Exchange/DataTrans/DataTransStruct.h>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include <brpc/stream.h>

namespace DB
{
class BrpcRemoteBroadcastSender : public IBroadcastSender
{
public:
    BrpcRemoteBroadcastSender(std::vector<DataTransKeyPtr> trans_keys_, ContextPtr context_, Block header_);
    BrpcRemoteBroadcastSender(DataTransKeyPtr trans_key_, ContextPtr context_, Block header_);
    ~BrpcRemoteBroadcastSender() override;

    void waitAllReceiversReady(UInt32 timeout_ms) override;
    BroadcastStatus send(Chunk chunk) override;
    virtual BroadcastStatus finish(BroadcastStatusCode status_code, String message) override;

    bool sendIOBuffer(butil::IOBuf io_buffer, brpc::StreamId stream_id, const String & data_key);

private:
    Poco::Logger * log = &Poco::Logger::get("BrpcRemoteBroadcastSender");
    std::vector<DataTransKeyPtr> trans_keys;
    ContextPtr context;
    Block header;
    BrpcExchangeRegistryCenter & registry_center;
    std::vector<brpc::StreamId> sender_stream_ids;
    std::atomic<bool> is_ready = false;
};
}
