#pragma once

#include <Processors/Exchange/DataTrans/DataTransStruct.h>

namespace DB
{
class IRemoteBroadcastReceiver
{
public:
    virtual void registerToSender(UInt32 timeout_ms) = 0;
    virtual DataTransPacket recv(UInt32 timeout_ms) = 0;
    virtual void finish(Int32 status_code) = 0;
    virtual ~IRemoteBroadcastReceiver() = default;
};

using IRemoteBroadcastReceiverShardPtr = std::shared_ptr<IRemoteBroadcastReceiver>;

}
