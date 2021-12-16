#pragma once

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
    BrpcRemoteBroadcastSender(std::vector<String> & receiver_ids_, ContextPtr context_, Block & header_, DataTransKeyPtr exchange_info_);
    BrpcRemoteBroadcastSender(String & receiver_id_, ContextPtr context_, Block & header_, DataTransKeyPtr exchange_info_);
    ~BrpcRemoteBroadcastSender() override;

    void waitAllReceiversReady(UInt32 timeout_ms) override;
    BroadcastStatus send(Chunk chunk) override;
    virtual BroadcastStatus finish(BroadcastStatusCode status_code, String message) override;

    bool sendIOBuffer(butil::IOBuf io_buffer, brpc::StreamId stream_id);

private:
    Poco::Logger * log = &Poco::Logger::get("BrpcRemoteBroadcastSender");
    std::vector<String> receiver_ids;
    ContextPtr context;
    Block header;
    DataTransKeyPtr trans_key;
    BrpcExchangeRegistryCenter & registry_center;
    std::vector<brpc::StreamId> sender_stream_ids;
    bool is_ready = false;
};
}
