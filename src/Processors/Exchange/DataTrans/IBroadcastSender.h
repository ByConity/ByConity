#pragma once

#include <memory>
#include <optional>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <common/types.h>

namespace DB
{

enum class BroadcastSenderType
{
    Local = 0,
    Brpc
};
class IBroadcastSender
{
public:
    virtual BroadcastStatus send(Chunk chunk) = 0;

    /// Merge sender to get 1:N sender and can avoid duplicated serialization when send chunk
    virtual void merge(IBroadcastSender && sender) = 0;

    virtual String getName() const = 0;
    virtual BroadcastSenderType getType() = 0;
    virtual BroadcastStatus finish(BroadcastStatusCode status_code_, String message) = 0;
    virtual ~IBroadcastSender() = default;
};

using BroadcastSenderPtr = std::shared_ptr<IBroadcastSender>;

}
