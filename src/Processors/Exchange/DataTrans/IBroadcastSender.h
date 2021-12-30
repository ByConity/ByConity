#pragma once

#include <memory>
#include <optional>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <common/types.h>

namespace DB
{
class IBroadcastSender
{
public:
    virtual BroadcastStatus send(Chunk chunk) = 0;
    virtual void waitAllReceiversReady(UInt32 timeout_ms) = 0;
    virtual BroadcastStatus finish(BroadcastStatusCode status_code_, String message) = 0;
    virtual ~IBroadcastSender() = default;
};

using BroadcastSenderPtr = std::shared_ptr<IBroadcastSender>;

}
