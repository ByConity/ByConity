#pragma once

#include <atomic>
#include <mutex>
#include <vector>
#include <Interpreters/Context.h>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/DataTransKey.h>
#include <Processors/Exchange/DataTrans/DataTransStruct.h>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include <brpc/stream.h>
#include <bthread/mtx_cv_base.h>

namespace DB
{
class BrpcRemoteBroadcastSender : public IBroadcastSender
{
public:
    BrpcRemoteBroadcastSender(DataTransKeyPtr trans_key_, brpc::StreamId stream_id, ContextPtr context_, Block header_);
    ~BrpcRemoteBroadcastSender() override;

    BroadcastStatus send(Chunk chunk) noexcept override;
    BroadcastStatus finish(BroadcastStatusCode status_code_, String message) override;
    void merge(IBroadcastSender && /*sender*/) override;
    String getName() const override;
    BroadcastStatus sendIOBuffer(butil::IOBuf io_buffer, brpc::StreamId stream_id, const String & data_key);
    butil::IOBuf serializeChunkToIoBuffer(Chunk chunk) const;

private:
    Poco::Logger * log = &Poco::Logger::get("BrpcRemoteBroadcastSender");
    std::vector<DataTransKeyPtr> trans_keys;
    ContextPtr context;
    Block header;
    std::vector<brpc::StreamId> sender_stream_ids;
    std::atomic<BroadcastStatus *> broadcast_status;
};
}
