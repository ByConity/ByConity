#include <limits>
#include <CloudServices/HiveDistributedReadingCoordinator.h>

#include "Common/ConcurrentBoundedQueue.h"
#include "CloudServices/ParallelReadRequestResponse.h"
#include "CloudServices/ParallelReadSplit.h"
#include "Storages/Hive/HiveDataPart.h"
#include "Storages/Hive/HiveDataPart_fwd.h"
#include "consistent_hashing.h"

namespace DB
{

struct HiveDistributedReadingCoordinator::State
{
    explicit State(HiveDistributedReadingCoordinator * coordinator_, size_t max_fill_)
        : coordinator(coordinator_), queue(max_fill_), pos(0)
    {
    }

    bool hasNext()
    {
        std::lock_guard lock(mu);

        if (!top)
        {
            /// fetch next part
            pos = 0;
            return queue.pop(top);
        }

        return pos < top->info.file_size;
    }
    
    std::pair<size_t, size_t> next(HiveDataPartCNCHPtr & res)
    {
        std::lock_guard lock(mu);
        res = top;
        size_t start = pos;
        size_t end = std::min(start + coordinator->split_len, res->info.file_size);

        if (end == res->info.file_size)
            top = nullptr;

        return {start, end};
    }

    HiveDistributedReadingCoordinator * coordinator;
    ConcurrentBoundedQueue<HiveDataPartCNCHPtr> queue;

    HiveDataPartCNCHPtr top;
    size_t pos{0};

    std::mutex mu;

};

HiveDistributedReadingCoordinator::HiveDistributedReadingCoordinator(size_t replicas_count_, size_t split_len_, Allocator allocator_)
    : replicas_count(replicas_count_), split_len(split_len_), allocator(allocator_)
{
    stats.resize(replicas_count_);
    for (size_t i = 0; i < replicas_count_; ++i)
    {
        size_t max_fill = std::numeric_limits<size_t>::max();
        stats[i] = std::make_unique<State>(this, max_fill);
    }
};

ParallelReadResponse HiveDistributedReadingCoordinator::handleRequest(ParallelReadRequest request)
{
    auto & state = *(stats[request.replica_num]);

    ParallelReadResponse response;

    if (state.hasNext())
    {
        HiveDataPartCNCHPtr part;
        auto range = state.next(part);
        response.split.emplace_back(HiveParallelReadSplit{part, range.first, range.second});
    }

    if (!state.hasNext())
        response.finish = true;

    return response;
};

size_t HiveDistributedReadingCoordinator::consistentHashAllocator(const HiveDataPartCNCHPtr & part, size_t replicas_count)
{
    auto hash = SipHash();
    hash.update(part->getFullDataPartPath());
    return ConsistentHashing(hash.get64(), replicas_count);
}

void HiveDistributedReadingCoordinator::finish()
{
    for (const auto & state : stats)
    {
        state->queue.finish();
    }
}

void HiveDistributedReadingCoordinator::addParts(HiveDataPartsCNCHVector & pending_parts)
{
    for (const auto & part : pending_parts)
    {
        size_t replica_num = allocator(part, replicas_count);
        auto & state = stats[replica_num];

        [[maybe_unused]] bool finished = state->queue.push(part);
    }
}

}
