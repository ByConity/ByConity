#include <Storages/MergeTree/HaMergeTreeQueue.h>

#include <Storages/StorageHaMergeTree.h>

namespace DB
{
HaMergeTreeQueue::HaMergeTreeQueue(StorageHaMergeTree & storage_)
    : storage(storage_)
    , format_version(storage.format_version)
    , zookeeper_path(storage.zookeeper_path)
    , replica_name(storage.replica_name)
    , replica_path(storage.replica_path)
    , log(&Poco::Logger::get(storage.getStorageID().getFullTableName() + " (HaMergeTreeQueue)"))
    , unprocessed_queue(unprocessed_set.get<TagSequenced>())
    , unprocessed_by_lsn(unprocessed_set.get<TagByLSN>())
    , merge_mutate_future_parts(storage.format_version)
{
}


void HaMergeTreeQueue::initialize(const MergeTreeData::DataParts &)
{
    /// empty
}

void HaMergeTreeQueue::clear()
{
    std::lock_guard clear_lock(state_mutex);
    unprocessed_queue.clear();
    merge_mutate_future_parts.clear();
    for (auto & elem : unprocessed_counts)
        elem.store(0, std::memory_order_relaxed);
    unprocessed_merges_of_self.store(0, std::memory_order_relaxed);
    unprocessed_mutations_of_self.store(0, std::memory_order_relaxed);
    unprocessed_inserts_by_time.clear();
    min_unprocessed_insert_time.store(std::numeric_limits<time_t>::max(), std::memory_order_relaxed);
    max_processed_insert_time.store(0, std::memory_order_relaxed);
    last_queue_update_time.store(0, std::memory_order_relaxed);
}

HaMergeTreeLogManager & HaMergeTreeQueue::getLogManager() const
{
    return *storage.log_manager;
}

/*
HaMergeTreeLogExchanger & HaMergeTreeQueue::getLogExchanger() const
{
    return storage.log_exchanger;
}
*/

zkutil::ZooKeeperPtr HaMergeTreeQueue::getZooKeeper() const
{
    return storage.getZooKeeper();
}

void HaMergeTreeQueue::load(zkutil::ZooKeeperPtr zookeeper)
{
    int inserted_count = 0;

    {
        std::lock_guard insert_lock(state_mutex);
        auto manager_lock = getLogManager().lockMe();
        for (auto & entry : getLogManager().getEntryListUnsafe())
            inserted_count += insertWithStats(entry, insert_lock);
    }

    if (inserted_count)
        LOG_TRACE(log, "Copy {} entries into queue from log manager", inserted_count);

    {
        std::lock_guard pull_logs_lock(pull_logs_to_queue_mutex);
        zookeeper->tryGet(replica_path + "/mutation_pointer", mutation_pointer);
    }
}


}
