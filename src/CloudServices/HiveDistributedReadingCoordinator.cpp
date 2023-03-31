#include <limits>
#include <CloudServices/HiveDistributedReadingCoordinator.h>

#include "Common/ConcurrentBoundedQueue.h"
#include "CloudServices/IDistributedReadingCoordinator.h"
#include "CloudServices/ParallelReadRequestResponse.h"
#include "CloudServices/ParallelReadSplit.h"
#include "Storages/Hive/HiveDataPart.h"
#include "Storages/Hive/HiveDataPart_fwd.h"
#include "consistent_hashing.h"
#include "Interpreters/WorkerGroupHandle.h"

namespace DB
{

struct HiveDistributedReadingCoordinator::State
{
    explicit State(HiveDistributedReadingCoordinator * coordinator_, size_t max_fill_)
        : coordinator(coordinator_), queue(max_fill_)
    {
    }

    bool hasNext()
    {
        std::lock_guard lock(mu);
        return queue.isFinishedAndEmpty();
    }

    HiveDataPartCNCHPtr tryGetNext()
    {
        std::lock_guard lock(mu);
        HiveDataPartCNCHPtr part = nullptr;
        if (queue.pop(part))
            return part;
        else
            return nullptr;
    }

    HiveDistributedReadingCoordinator * coordinator;
    ConcurrentBoundedQueue<HiveDataPartCNCHPtr> queue;

    std::mutex mu;
};

HiveDistributedReadingCoordinator::HiveDistributedReadingCoordinator(
    const std::shared_ptr<WorkerGroupHandleImpl> & worker_group_, Allocator allocator_, bool enable_work_stealing_)
    : allocator(allocator_), enable_work_stealing(enable_work_stealing_)
{
    stats.resize(worker_group_->size());
    for (auto & stat : stats)
    {
        size_t max_fill = std::numeric_limits<size_t>::max();
        stat = std::make_unique<State>(this, max_fill);
    }

    const auto & hosts = worker_group_->getHostWithPortsVec();
    Strings keys;
    keys.reserve(hosts.size());
    std::transform(hosts.begin(), hosts.end(), std::back_inserter(keys), [] (const auto & host){
        return host.getTCPAddress();
    });
    std::sort(keys.begin(), keys.end());

    for (const auto i : collections::range(0, keys.size()))
    {
        key_index[keys[i]] = i;
    }
};

HiveDistributedReadingCoordinator::~HiveDistributedReadingCoordinator() = default;

ParallelReadResponse HiveDistributedReadingCoordinator::handleRequest(ParallelReadRequest request)
{
    LOG_DEBUG(log, "Received request from replica {}", request.worker_id);

    ParallelReadResponse response;
    auto selectFilesToRead = [this, &request, &response] (HiveDistributedReadingCoordinator::State & state) {
        while (request.min_weight)
        {
            auto part = state.tryGetNext();
            if (!part)
                break;

            HiveParallelReadSplit split{part, 0, 0};
            LOG_DEBUG(log, "dispatch task {} to {}", split.describe(), request.worker_id);
            response.split.add(split);
            request.min_weight--;
        }
    };

    auto & state = *(stats[key_index.at(request.worker_id)]);
    selectFilesToRead(state);

    if (enable_work_stealing && request.min_weight)
    {
        for (const auto & [key, idx] : key_index)
        {
            if (key == request.worker_id)
                continue;
            auto & other = *(stats[idx]);
            selectFilesToRead(other);
        }
    }

    if (request.min_weight > 0)
    {
        LOG_DEBUG(log, "replica {} finish", request.worker_id);
        response.finish = true;
    }

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
    for (const auto & [key, idx] : key_index)
    {
        stats[idx]->queue.finish();
        LOG_TRACE(log, "replica {}, worker_id {}, task size {}", idx, key, stats[idx]->queue.size());
    }
}

void HiveDistributedReadingCoordinator::addParts(const HiveDataPartsCNCHVector & pending_parts)
{
    for (const auto & part : pending_parts)
    {
        size_t replica_num = allocator(part, stats.size());
        auto & state = stats[replica_num];
        [[maybe_unused]] bool finished = state->queue.push(part);
    }
}

}
