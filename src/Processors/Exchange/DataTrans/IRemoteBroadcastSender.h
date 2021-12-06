#pragma once

namespace DB
{
class IRemoteBroadcastSender
{
public:
    virtual void waitAllReceiversReady(Int32 timeout_ms) = 0;
    virtual void send(Chunk & chunk) = 0;
    virtual void finish(Int32 status_code) = 0;
    virtual ~IRemoteBroadcastSender() = default;
};

using IRemoteBroadcastSenderShardPtr = std::shared_ptr<IRemoteBroadcastSender>;

}
