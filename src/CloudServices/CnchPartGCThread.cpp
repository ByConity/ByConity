#include <CloudServices/CnchPartGCThread.h>

#include <random>
#include <Catalog/Catalog.h>
#include <CloudServices/CnchPartsHelper.h>
#include <Interpreters/ServerPartLog.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Storages/StorageCnchMergeTree.h>

namespace DB
{

CnchPartGCThread::CnchPartGCThread(ContextPtr context_, const StorageID & id) : ICnchBGThread(context_, CnchBGThreadType::PartGC, id)
{
}

void CnchPartGCThread::runImpl()
{
    UInt64 sleep_ms = 30 * 1000;

    try
    {
        auto istorage = getStorageFromCatalog();
        auto & storage = checkAndGetCnchTable(istorage);
        auto storage_settings = storage.getSettings();
        if (istorage->is_dropped)
        {
            LOG_DEBUG(log, "Table was dropped, wait for removing...");
            scheduled_task->scheduleAfter(10 * 1000);
            return;
        }

        try
        {
            clearOldParts(istorage, storage);
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }

        try
        {
            clearOldInsertionLabels(istorage, storage);
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }

        sleep_ms = storage_settings->cleanup_delay_period * 1000
            + std::uniform_int_distribution<UInt64>(0, storage_settings->cleanup_delay_period_random_add * 1000)(rng);
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    scheduled_task->scheduleAfter(sleep_ms);
}

void CnchPartGCThread::clearOldParts(const StoragePtr & istorage, StorageCnchMergeTree & storage)
{
    auto storage_settings = storage.getSettings();
    auto now = time(nullptr);

    bool in_wakeup = inWakeup();
    Strings partitions = catalog->getPartitionIDs(istorage, nullptr);

    /// Only inspect the parts small than gc_timestamp
    TxnTimestamp gc_timestamp = calculateGCTimestamp(storage_settings->old_parts_lifetime.totalSeconds(), in_wakeup);
    if (gc_timestamp <= last_gc_timestamp) /// Skip unnecessary gc
    {
        LOG_DEBUG(log, "Skip unnecessary GC as gc_timestamp {} <= last_gc_timestamp {} ", gc_timestamp, last_gc_timestamp);
        return;
    }

    Stopwatch watch;
    auto all_parts = catalog->getServerDataPartsInPartitions(istorage, partitions, gc_timestamp, nullptr);
    LOG_TRACE(log, "Get parts from Catalog cost {} us, all_parts {}", watch.elapsedMicroseconds(), all_parts.size());
    watch.restart();

    ServerDataPartsVector visible_alone_drop_ranges;
    ServerDataPartsVector invisible_dropped_parts;
    auto visible_parts = CnchPartsHelper::calcVisiblePartsForGC(all_parts, &visible_alone_drop_ranges, &invisible_dropped_parts);
    LOG_TRACE(log, "Calculate visible parts for GC cost {} us, visible_parts ", watch.elapsedMicroseconds(), visible_parts.size());
    watch.stop();

    /// Generate DROP_RANGE for expired partitions by the TTL
    if (!visible_parts.empty())
        tryMarkExpiredPartitions(storage, visible_parts);

    /// Clear special old parts
    UInt64 old_parts_lifetime = in_wakeup ? 0ull : UInt64(storage_settings->old_parts_lifetime.totalSeconds());

    invisible_dropped_parts.erase(
        std::remove_if(
            invisible_dropped_parts.begin(),
            invisible_dropped_parts.end(),
            [&](auto & part) { return UInt64(now) < TxnTimestamp(part->getCommitTime()).toSecond() + old_parts_lifetime; }),
        invisible_dropped_parts.end());
    pushToRemovingQueue(storage, invisible_dropped_parts, "Invisible Dropped Parts");

    visible_alone_drop_ranges.erase(
        std::remove_if(
            visible_alone_drop_ranges.begin(),
            visible_alone_drop_ranges.end(),
            [&](auto & part) { return UInt64(now) < TxnTimestamp(part->getCommitTime()).toSecond() + old_parts_lifetime; }),
        visible_alone_drop_ranges.end());
    pushToRemovingQueue(storage, visible_alone_drop_ranges, "Visible Alone Drop Ranges");

    if (!visible_alone_drop_ranges.empty() || !invisible_dropped_parts.empty())
    {
        LOG_DEBUG(
            log,
            "Clear old parts with timestamp {}, {} : all_parts {}, visible_alone_drop_ranges {}, invisible_dropped_parts {}",
            gc_timestamp,
            LocalDateTime(gc_timestamp.toSecond()),
            all_parts.size(),
            visible_alone_drop_ranges.size(),
            invisible_dropped_parts.size());
    }

    if (!visible_parts.empty())
    {
        LOG_DEBUG(
            log,
            "Will check visible_parts {} with timestamp {}, {}",
            visible_parts.size(),
            gc_timestamp,
            LocalDateTime(gc_timestamp.toSecond()));
    }

    auto checkpoints = getCheckpoints(storage, gc_timestamp);
    for (size_t i = 1; i < checkpoints.size(); ++i)
    {
        collectBetweenCheckpoints(storage, visible_parts, {}, checkpoints[i - 1], checkpoints[i]);
    }

    last_gc_timestamp = gc_timestamp;
}

/// TODO: optimize me
static time_t calcTTLForPartition(
    const MergeTreePartition & partition, const KeyDescription & partition_key_description, const TTLDescription & ttl_description)
{
    auto columns = partition_key_description.sample_block.cloneEmptyColumns();
    for (size_t i = 0; i < partition.value.size(); ++i)
        columns[i]->insert(partition.value[i]);

    auto block = partition_key_description.sample_block.cloneWithColumns(std::move(columns));
    ttl_description.expression->execute(block);

    auto & result_column_with_tn = block.getByName(ttl_description.result_column);
    auto & result_column = result_column_with_tn.column;
    auto & result_type = result_column_with_tn.type;

    if (isDate(result_type))
    {
        auto value = UInt16(result_column->getUInt(0));
        const auto & date_lut = DateLUT::instance();
        return date_lut.fromDayNum(DayNum(value));
    }
    else if (isDateTime(result_type))
    {
        return UInt32(result_column->getUInt(0));
    }
    else
    {
        throw Exception("Logical error in calcuate TTL value: unexpected TTL result column type", ErrorCodes::LOGICAL_ERROR);
    }
}

void CnchPartGCThread::tryMarkExpiredPartitions(StorageCnchMergeTree & storage, const ServerDataPartsVector & visible_parts)
{
    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    if (!metadata_snapshot->hasPartitionLevelTTL())
        return;

    const auto & partition_key_description = metadata_snapshot->partition_key;
    const auto & rows_ttl = metadata_snapshot->table_ttl.rows_ttl;

    time_t now = time(nullptr);
    NameOrderedSet partitions_to_clean;

    const String * curr_partition = nullptr;
    for (const auto & part : visible_parts)
    {
        if (curr_partition && *curr_partition == part->get_info().partition_id)
            continue;
        curr_partition = &part->get_info().partition_id;

        if (part->get_deleted())
            continue;

        auto ttl = calcTTLForPartition(part->get_partition(), partition_key_description, rows_ttl);
        if (ttl < now)
            partitions_to_clean.insert(*curr_partition);
    }
}


std::vector<TxnTimestamp> CnchPartGCThread::getCheckpoints(StorageCnchMergeTree & storage, TxnTimestamp max_timestamp)
{
    auto storage_settings = storage.getSettings();

    auto now = time(nullptr);

    std::vector<TxnTimestamp> timestamps{0};

    if (storage_settings->time_travel_retention_days)
    {
        Checkpoints checkpoints = catalog->getCheckpoints();

        for (auto & checkpoint : checkpoints)
        {
            TxnTimestamp ts = checkpoint.timestamp();
            if (checkpoint.status() == Checkpoint::Normal && UInt64(now) > ts.toSecond() + checkpoint.ttl())
                ; /// TODO:
            /// storage.removeCheckpoint(checkpoint);
            else
                timestamps.push_back(ts);
        }
    }

    timestamps.push_back(max_timestamp);
    return timestamps;
}

void CnchPartGCThread::collectBetweenCheckpoints(
    StorageCnchMergeTree & storage,
    const ServerDataPartsVector & visible_parts,
    [[maybe_unused]] const DeleteBitmapMetaPtrVector & visible_bitmaps,
    TxnTimestamp begin,
    TxnTimestamp end)
{
    ServerDataPartsVector stale_parts;
    DeleteBitmapMetaPtrVector stale_bitmaps;

    for (const auto & part : visible_parts)
        collectStaleParts(part, begin, end, false, stale_parts);
    // for (auto & bitmap : visible_bitmaps)
    //     collectStaleBitmaps(bitmap, begin, end, false, stale_bitmaps);

    pushToRemovingQueue(storage, stale_parts, "Stale Parts");
    /// removeDeleteBitmaps(storage, stale_bitmaps, "stale between " + begin.toString() + " and " + end.toString());

    std::unordered_map<String, Int32> remove_parts_num;

    for (auto & part : stale_parts)
        ++remove_parts_num[part->info().partition_id];

    if (!remove_parts_num.empty())
    {
        std::stringstream ss;
        for (auto & [partition_id, count]: remove_parts_num)
            ss << "(" << partition_id << ", " << count << "), ";
        LOG_TRACE(log, "Will remove parts: {}", ss.str());
    }

}

void CnchPartGCThread::collectStaleParts(
    const ServerDataPartPtr & parent_part,
    TxnTimestamp begin,
    TxnTimestamp end,
    bool has_visible_ancestor,
    ServerDataPartsVector & stale_parts) const
{
    if (const auto & prev_part = parent_part->tryGetPreviousPart())
    {
        bool child_has_visible_ancestor = has_visible_ancestor /* inherit from parent */
            || (UInt64(parent_part->getCommitTime()) <= UInt64(end) /* parent is visible */
                && !parent_part->isPartial() /* parent is a base one */);

        if (child_has_visible_ancestor && UInt64(prev_part->getCommitTime()) > UInt64(begin))
        {
            LOG_DEBUG(
                log, "Will remove part {} covered by {} /partial={}", prev_part->name(), parent_part->name(), parent_part->isPartial());
            stale_parts.push_back(prev_part);
        }

        collectStaleParts(prev_part, begin, end, child_has_visible_ancestor, stale_parts);
    }
}

TxnTimestamp CnchPartGCThread::calculateGCTimestamp(UInt64 delay_second, bool in_wakeup)
{
    auto local_context = getContext();

    TxnTimestamp gc_timestamp = local_context->getTimestamp();
    if (in_wakeup) /// XXX: will invalid all running queries
        return gc_timestamp;

    // auto server_clients = ->lobal_context->getCnchServerClientPool().getAll();
    // for (auto & c : server_clients)
    // {
    //     try
    //     {
    //         if (auto min_active_ts = c->getMinActiveTimestamp(storage_id))
    //             gc_timestamp = std::min(*min_active_ts, gc_timestamp);
    //     }
    //     catch (...)
    //     {
    //         tryLogCurrentException(log);
    //     }
    // }

    /// Will invalid the running queries of which the timestamp <= gc_timestamp
    TxnTimestamp max_gc_timestamp = ((time(nullptr) - delay_second) * 1000) << 18;
    gc_timestamp = std::min(gc_timestamp, max_gc_timestamp);
    return gc_timestamp;
}

void CnchPartGCThread::pushToRemovingQueue(
    StorageCnchMergeTree & storage, const ServerDataPartsVector & parts, const String & reason, bool is_staged_part)
{
    auto local_context = getContext();

    auto storage_settings = storage.getSettings();

    /// TODO P1: async ?
    /// removing_queue.push(std::move(part));
    if (parts.empty())
        return;

    LOG_DEBUG(log, "Try to remove {} part(s) by GCThread, reason: {}", parts.size(), reason);

    /// ThreadPool remove_pool(storage_settings->gc_remove_part_thread_pool_size);
    ThreadPool remove_pool(32);

    auto batch_remove = [&](size_t start, size_t end)
    {
        remove_pool.scheduleOrThrowOnError([&, start, end]
        {
            MergeTreeDataPartsCNCHVector remove_parts;
            for (auto it = parts.begin() + start; it != parts.begin() + end; ++it)
            {
                auto name = (*it)->name();
                try
                {
                    LOG_TRACE(log, "Will remove part: {}", name);
                    /// auto cnch_part = (*it)->toCNCHDataPart(storage);
                    /// if (!is_staged_part)
                    ///     cnch_part->remove();
                    /// remove_parts.emplace_back(cnch_part);
                }
                catch (...)
                {
                    tryLogCurrentException(log, "Error occurs when remove part: " + name);
                }
            }

            LOG_DEBUG(log, "Will remove {} part(s) in CnchCatalog, reason: {}", remove_parts.size(), reason);
            if (!is_staged_part)
                catalog->clearDataPartsMeta(storage.shared_from_this(), remove_parts);
            else
                catalog->clearStagePartsMeta(storage.shared_from_this(), remove_parts);

            if (auto server_part_log = local_context->getServerPartLog())
            {
                auto now = time(nullptr);

                for (auto & part : remove_parts)
                {
                    ServerPartLogElement elem;
                    elem.event_type = ServerPartLogElement::REMOVE_PART;
                    elem.event_time = now;
                    elem.database_name = storage.getDatabaseName();
                    elem.table_name = storage.getTableName();
                    elem.uuid = storage.getStorageUUID();
                    elem.part_name = part->name;
                    elem.partition_id = part->info.partition_id;
                    elem.is_staged_part = is_staged_part;

                    server_part_log->add(elem);
                }
            }
        });
    };

    /// size_t batch_size = storage_settings->gc_remove_part_batch_size;
    constexpr static size_t batch_size = 1000;

    for (size_t start = 0; start < parts.size(); start += batch_size)
    {
        auto end = std::min(start + batch_size, parts.size());
        batch_remove(start, end);
    }

    remove_pool.wait();
}

void CnchPartGCThread::clearOldInsertionLabels(const StoragePtr &, StorageCnchMergeTree & storage)
{
    time_t insertion_label_ttl = storage.getSettings()->insertion_label_ttl;

    std::vector<InsertionLabel> labels_to_remove;
    auto now = time(nullptr);
    auto labels = catalog->scanInsertionLabels(storage.getStorageID().uuid);
    for (auto & label : labels)
    {
        if (label.create_time + insertion_label_ttl <= now)
            labels_to_remove.emplace_back(std::move(label));
    }

    if (labels_to_remove.empty())
        return;

    catalog->removeInsertionLabels(labels_to_remove);
    LOG_DEBUG(log, "Removed {} insertion labels.", labels_to_remove.size());
}

}
