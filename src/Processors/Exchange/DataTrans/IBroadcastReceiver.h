#pragma once

#include <variant>
#include <common/types.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Chunk.h>

namespace DB
{
using RecvDataPacket = std::variant<Chunk, BroadcastStatus>;
class IBroadcastReceiver
{
public:
    virtual RecvDataPacket recv(UInt32 timeout_ms)  = 0;
    virtual void registerToSenders(UInt32 timeout_ms) = 0;
    virtual BroadcastStatus finish(BroadcastStatusCode status_code, String message)  = 0;
    virtual ~IBroadcastReceiver() = default;
};

}
