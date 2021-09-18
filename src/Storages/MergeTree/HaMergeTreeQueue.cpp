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

HaMergeTreeLogExchanger & HaMergeTreeQueue::getLogExchanger() const
{
    return storage.log_exchanger;
}

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

bool HaMergeTreeQueue::pullLogsToQueue(
    [[maybe_unused]] zkutil::ZooKeeperPtr zookeeper,
    [[maybe_unused]] Coordination::WatchCallback watch_callback,
    [[maybe_unused]] bool ignore_backoff)
{
    return {};
}


size_t HaMergeTreeQueue::insertWithStats(const LogEntryPtr & entry, std::lock_guard<std::mutex> &)
{
    if (entry->is_executed)
    {
        if (entry->type == LogEntry::GET_PART)
            max_processed_insert_time = std::max<time_t>(entry->create_time, max_processed_insert_time);
    }
    else
    {
        if (entry->type == LogEntry::GET_PART || entry->type == LogEntry::REPLACE_PARTITION)
        {
            if (entry->create_time < min_unprocessed_insert_time.load(std::memory_order_relaxed))
                min_unprocessed_insert_time.store(entry->create_time);
            unprocessed_inserts_by_time.insert(entry);
        }

        if (0 == unprocessed_by_lsn.count(entry->lsn))
        {
            if (entry->type == LogEntry::DROP_RANGE || entry->type == LogEntry::CLEAR_RANGE
                || entry->type == LogEntry::REPLACE_PARTITION) /// pick DROP_RANGE first
                unprocessed_queue.push_front(entry);
            else
                unprocessed_queue.push_back(entry);

            if (entry->hasMergeMutateFutureParts())
                merge_mutate_future_parts.add(entry->lsn, entry->new_parts);

            unprocessed_counts[entry->type].fetch_add(1, std::memory_order_relaxed);
            if (entry->type == LogEntry::MERGE_PARTS && entry->source_replica == storage.replica_name)
                unprocessed_merges_of_self.fetch_add(1, std::memory_order_relaxed);
            if (entry->type == LogEntry::MUTATE_PART && entry->source_replica == storage.replica_name)
                unprocessed_mutations_of_self.fetch_add(1, std::memory_order_relaxed);

            LOG_TRACE(log, "Insert entry to queue: {} ", entry->toString());
            return 1;
        }
    }

    return 0;
}

void HaMergeTreeQueue::write(const LogEntryVec & entries, bool broadcast)
{
    if (entries.empty())
        return;

    /// Mark trivial entries ahead for convenience and performance
    for (auto & entry : entries)
    {
        if (entry->shouldSkipOnReplica(storage.replica_name))
            entry->is_executed = true;
    }

    auto written_entries = getLogManager().writeLogEntries(entries);

    if (broadcast)
    {
        getLogExchanger().putLogEntries(written_entries);
    }

    size_t num_new_unprocessed = 0;
    {
        std::lock_guard insert_lock(state_mutex);
        for (auto & entry : entries)
            num_new_unprocessed += insertWithStats(entry, insert_lock);
    }

    if (num_new_unprocessed > 0)
        storage.background_executor.triggerTask();
}


}
