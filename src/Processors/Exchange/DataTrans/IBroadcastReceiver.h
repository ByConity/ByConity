#pragma once

#include <variant>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <common/types.h>

namespace DB
{
using RecvDataPacket = std::variant<Chunk, BroadcastStatus>;
class IBroadcastReceiver
{
public:
    virtual void registerToSenders(UInt32 timeout_ms) = 0;
    virtual RecvDataPacket recv(UInt32 timeout_ms) = 0;
    virtual BroadcastStatus finish(BroadcastStatusCode status_code_, String message) = 0;
    virtual ~IBroadcastReceiver() = default;
};

}
