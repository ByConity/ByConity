#pragma once

#include <memory>
#include <string>
#include <variant>
#include <Core/Block.h>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/BoundedDataQueue.h>
#include <Processors/Exchange/DataTrans/IBroadcastReceiver.h>
#include <butil/iobuf.h>

namespace DB {

using SendDoneMark = std::string;
using RawPacket= std::unique_ptr<butil::IOBuf>;
struct DataPacket
{
    Chunk chunk;
};
using MultiPathDataPacket = std::variant<SendDoneMark, DataPacket>;
using MultiPathBoundedQueue = BoundedDataQueue<MultiPathDataPacket>;
using MultiPathQueuePtr = std::shared_ptr<MultiPathBoundedQueue>;
}
