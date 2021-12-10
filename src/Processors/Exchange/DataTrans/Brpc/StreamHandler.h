#pragma once

#include "BrpcRemoteBroadcastReceiver.h"

#include <Core/Block.h>
#include <Processors/Exchange/DataTrans/BoundedDataQueue.h>
#include <Interpreters/Context.h>
#include <brpc/channel.h>
#include <common/logger_useful.h>

namespace DB
{
class StreamHandler : public brpc::StreamInputHandler
{
public:
    StreamHandler(const ContextPtr & context_, BrpcRemoteBroadcastReceiverWeakPtr receiver_) : context(context_), receiver(receiver_) { }

    int on_received_messages(brpc::StreamId id, butil::IOBuf * const * messages, size_t size) override;

    void on_idle_timeout(brpc::StreamId id) override;

    void on_closed(brpc::StreamId id) override;

private:
    ContextPtr context;
    Poco::Logger * log = &Poco::Logger::get("StreamHandler");
    BrpcRemoteBroadcastReceiverWeakPtr receiver;
};

}
