#include <CloudServices/DedupScheduler.h>

namespace DB
{
void DedupScheduler::dedupWithHighPriority(const String & partition_id)
{
    std::lock_guard lock(mutex);
    high_priority_dedup_gran_expiration_time[partition_id] = time(nullptr) + HIGH_PRIORITY_SEC;
}

Names DedupScheduler::getHighPriorityPartition()
{
    std::lock_guard lock(mutex);
    checkHighPriorityExpired(time(nullptr));

    Names ret;
    for (const auto & entry : high_priority_dedup_gran_expiration_time)
        ret.emplace_back(entry.first);

    return ret;
}

HostWithPorts DedupScheduler::tryPickWorker(size_t deduper_index)
{
    std::lock_guard lock(mutex);
    if (index_dedup_worker_map.count(deduper_index))
        return HostWithPorts::fromRPCAddress(index_dedup_worker_map[deduper_index]);

    return {};
}

void DedupScheduler::checkHighPriorityExpired(time_t current_time)
{
    for (auto it = high_priority_dedup_gran_expiration_time.begin(); it != high_priority_dedup_gran_expiration_time.end();)
    {
        if (it->second < current_time)
            it = high_priority_dedup_gran_expiration_time.erase(it);
        else
            it++;
    }
}

}
