#pragma once

#include <Common/Logger.h>
#include <memory>
#include <string>
#include <variant>
#include <Core/Block.h>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/BoundedDataQueue.h>
#include <Processors/Exchange/DeserializeBufTransform.h>
#include <butil/iobuf.h>
#include <common/logger_useful.h>

namespace DB {

using SendDoneMark = std::string;
struct DataPacket
{
    Chunk chunk;
};
using MultiPathDataPacket = std::variant<SendDoneMark, DataPacket>;

class MemoryController
{
public:
    explicit MemoryController(size_t capacity_) : capacity(capacity_) {}
    ~MemoryController() {}
    size_t size() const { return amount.load(std::memory_order_relaxed); }
    bool exceedLimit() const { return amount.load(std::memory_order_relaxed) > Int64(capacity); }
    template <typename T>
    void increase(const T & packet)
    {
        Int64 size = calculate(packet);
        Int64 will_be = size + amount.fetch_add(size, std::memory_order_relaxed);
        if (will_be > peak.load(std::memory_order_relaxed))
            peak.store(will_be, std::memory_order_relaxed);
    }
    void decrease(const MultiPathDataPacket & packet) { amount.fetch_sub(calculate(packet), std::memory_order_relaxed); }
    void logPeakMemoryUsage() const { LOG_TRACE(getLogger("MemoryController"), "Peak memory usage: {}", ReadableSize(peak)); }
private:
    Int64 calculate(const MultiPathDataPacket & packet) const
    {
        if (!capacity)
            return 0;

        if (std::holds_alternative<DataPacket>(packet))
        {
            auto & chunk = std::get<DataPacket>(packet).chunk;
            const auto & info = chunk.getChunkInfo();
            if (info)
            {
                const auto iobuf_info = std::dynamic_pointer_cast<const DeserializeBufTransform::IOBufChunkInfo>(info);
                if (iobuf_info)
                    return iobuf_info->io_buf.length();
            }
            return chunk.bytes();
        }
        auto & send_down_mark = std::get<SendDoneMark>(packet);
        return send_down_mark.length();
    }
    std::atomic<Int64> amount{0};
    std::atomic<Int64> peak{0};
    const size_t capacity;
};

using MultiPathBoundedQueue = BoundedDataQueue<MultiPathDataPacket, std::shared_ptr<MemoryController>>;
using MultiPathQueuePtr = std::shared_ptr<MultiPathBoundedQueue>;

}
