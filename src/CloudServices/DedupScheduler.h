#pragma once

#include <Core/Names.h>
#include <Common/HostWithPorts.h>

#include <ctime>
#include <mutex>

namespace DB
{

class DedupScheduler
{
public:

    const size_t HIGH_PRIORITY_SEC = 3600;

    /// Specify partition to dedup with high priority
    void dedupWithHighPriority(const String & partition_id);

    Names getHighPriorityPartition();

    /// For the same deduper index, try to use the same worker
    /// Need to judge empty, because we use cache to maintain the mapping(deduper index->worker) relationship
    HostWithPorts tryPickWorker(size_t deduper_index);

    inline void markIndexDedupWorker(size_t deduper_index, const HostWithPorts & host_port)
    {
        std::lock_guard lock(mutex);
        index_dedup_worker_map[deduper_index] = host_port.getRPCAddress();
    }

private:

    void checkHighPriorityExpired(time_t current_time);

    mutable std::mutex mutex;
    std::unordered_map<String, time_t> high_priority_dedup_gran_expiration_time;
    std::unordered_map<size_t, String> index_dedup_worker_map;
};

}
