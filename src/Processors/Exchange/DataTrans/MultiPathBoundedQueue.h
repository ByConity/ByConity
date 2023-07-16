#pragma once

#include <memory>
#include <string>
#include <variant>
#include <Processors/Chunk.h>
#include <Core/Block.h>
#include <Processors/Exchange/DataTrans/BoundedDataQueue.h>
#include <butil/iobuf.h>
namespace DB {

using SendDoneMark = std::string;
using RawPacket= std::unique_ptr<butil::IOBuf>;
using MultiPathDataPacket = std::variant<Chunk, RawPacket, SendDoneMark>;
using MultiPathBoundedQueue = BoundedDataQueue<MultiPathDataPacket>;
using MultiPathQueuePtr = std::shared_ptr<MultiPathBoundedQueue>;
}
