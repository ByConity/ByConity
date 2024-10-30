#pragma once

#include <atomic>
#include <Catalog/CatalogUtils.h>
#include <Storages/CnchPartitionInfo.h>
#include <Storages/TableDefinitionHash.h>
#include <Common/CurrentThread.h>
#include <Common/RWLock.h>
#include <Common/ScanWaitFreeMap.h>

namespace DB
{
class CacheVersion
{
public:
    PairInt64 get()
    {
        std::shared_lock lock(mutex_);
        return cache_version;
    }

    void set(const PairInt64 & new_version)
    {
        std::unique_lock lock(mutex_);
        cache_version = new_version;
    }

private:
    PairInt64 cache_version{0};
    mutable std::shared_mutex mutex_;
};


/// Table level lock holder, to ensure all the version of table meta are holding the same lock.
/// e.g. For different version of same partition of table in different thread(transient),
/// we need to make sure they all hold the same lock.
class MetaLockHolder
{
    /// Table level lock.
    RWLock table_lock;
    /// <partition_id, lock>.
    std::unordered_map<String, RWLock> partition_locks;
    /// Protect partition_locks.
    std::mutex lock_of_partition_locks;

public:
    explicit MetaLockHolder(RWLock table_lock_) : table_lock(table_lock_) { }
    RWLock getTableLock() { return table_lock; }
    RWLock getPartitionLock(const String & partition_id);
};

struct TableMetaEntry
{
    using TableLockHolder = RWLockImpl::LockHolder;

    TableLockHolder readLock() const
    {
        return lock_holder->getTableLock()->getLock(RWLockImpl::Read, CurrentThread::getQueryId().toString());
    }

    TableLockHolder writeLock() const
    {
        return lock_holder->getTableLock()->getLock(RWLockImpl::Write, CurrentThread::getQueryId().toString());
    }

    TableMetaEntry(
        const String & database_,
        const String & table_,
        const String & table_uuid_,
        const std::shared_ptr<MetaLockHolder> & lock_holder_ = nullptr,
        const bool on_table_creation = false)
        : database(database_)
        , table(table_)
        , table_uuid(table_uuid_)
        , partition_metrics_loaded(on_table_creation)
        , trash_item_metrics(std::make_shared<TableMetrics>(table_uuid_))
    {
        if (!lock_holder)
            lock_holder = std::make_shared<MetaLockHolder>(RWLockImpl::create());
        else
            lock_holder = lock_holder_;
    }

    String database;
    String table;
    String table_uuid;
    /// track the timestamp when last data ingestion or removal happens to this table; initialized with current time
    UInt64 last_update_time{0};
    /// Track the metrics change. Because metrics update time is not the same with data update time, so we track them separately.
    /// NOTE: Could be false positive.
    UInt64 metrics_last_update_time{0};
    /// Needs to wait for mayUpdateTableMeta to load all needed info from KV
    std::atomic<UInt32> cache_status{CacheStatus::UINIT};
    /// Check the cache version each time when visit the cache. To make sure the visited cache is still valid
    CacheVersion cache_version;

    bool is_clustered{true};
    TableDefinitionHash table_definition_hash;
    String preallocate_vw;
    std::shared_ptr<MetaLockHolder> lock_holder;
    std::atomic_bool partition_metrics_loaded = false;
    std::atomic_bool loading_metrics = false;
    std::atomic_bool load_parts_by_partition = false;
    std::mutex fetch_mutex;
    std::condition_variable fetch_cv;
    /// used to decide if the part/partition cache are still valid when enable write ha. If the fetched
    /// NHUT from metastore differs with cached one, we should update cache with metastore.
    std::atomic_uint64_t cached_non_host_update_ts{0};
    std::atomic_bool need_invalid_cache{false};
    std::shared_ptr<TableMetrics> trash_item_metrics;

    ScanWaitFreeMap<String, PartitionInfoPtr> partitions;
    String server_vw_name;

    Catalog::PartitionMap getPartitions(const Strings & wanted_partition_ids);
    std::unordered_set<String> getDeletingPartitions();
    Strings getPartitionIDs();
    std::vector<std::shared_ptr<MergeTreePartition>> getPartitionList();
    PartitionInfoPtr getPartitionInfo(const String & partition_id);

    void forEachPartition(std::function<void(PartitionInfoPtr)> callback);
};

using TableMetaEntryPtr = std::shared_ptr<TableMetaEntry>;
}
