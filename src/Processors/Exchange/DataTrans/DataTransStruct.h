#pragma once

#include "BoundedDataQueue.h"

#include <Processors/Chunk.h>
#include <butil/iobuf.h>

namespace DB
{

/// Packet that received from server.
struct DataTransPacket
{
    DataTransPacket() = default;
    explicit DataTransPacket(Chunk && chunk_) : chunk(std::move(chunk_)) { }
    explicit DataTransPacket(const String & exception_) : exception(exception_) { }

    size_t size()
    {
        if (!chunk)
            return chunk.bytes();
        else
            return 0;
    }

    Chunk chunk;
    String exception;
};

struct ReceiveQueue
{
    explicit ReceiveQueue(std::shared_ptr<MemoryTracker> memory_tracker = nullptr)
        : receive_queue(std::make_shared<TrackBoundedQueue<DataTransPacket>>(memory_tracker)), active(true)
    {
    }
    std::shared_ptr<TrackBoundedQueue<DataTransPacket>> receive_queue;
    std::atomic<bool> active;
};

using ReceiveQueuePtr = std::unique_ptr<ReceiveQueue>;
}
