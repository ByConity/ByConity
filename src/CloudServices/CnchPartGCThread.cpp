/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <CloudServices/CnchPartGCThread.h>

#include <random>
#include <Catalog/Catalog.h>
#include <CloudServices/CnchPartsHelper.h>
#include <CloudServices/CnchDataWriter.h>
#include <Interpreters/ServerPartLog.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Storages/StorageCnchMergeTree.h>
#include <WorkerTasks/ManipulationType.h>
#include <Poco/Exception.h>

namespace DB
{

CnchPartGCThread::CnchPartGCThread(ContextPtr context_, const StorageID & id) : ICnchBGThread(context_, CnchBGThreadType::PartGC, id)
{
    partition_selector = getContext()->getBGPartitionSelector();
    data_remover = getContext()->getGCSchedulePool().createTask(log->name() + "(remover)", [this] { runDataRemoveTask(); });
    data_remover->deactivate();
}

void CnchPartGCThread::stop()
{
    if (data_remover->taskIsActive())
    {
        LOG_DEBUG(log, "Stopping data remover task.");
        data_remover->deactivate();
    }
    // stop current gc task;
    ICnchBGThread::stop();
}

void CnchPartGCThread::runImpl()
{
    UInt64 sleep_ms = 30 * 1000;

    try
    {
        if (!data_remover->taskIsActive())
        {
            // if data remover is not active and scheduled, schedule it
            LOG_DEBUG(log, "Start data remover task");
            data_remover->activateAndSchedule();
        }

        auto istorage = getStorageFromCatalog();
        if (istorage->is_dropped)
        {
            LOG_DEBUG(log, "Table was dropped, wait for removing...");
            scheduled_task->scheduleAfter(10 * 1000);
            return;
        }

        auto & storage = checkAndGetCnchTable(istorage);
        auto storage_settings = storage.getSettings();

        try
        {
            auto catalog = getContext()->getCnchCatalog();
            bool in_wakeup = inWakeup();
            Strings partitions = in_wakeup ? catalog->getPartitionIDs(istorage, nullptr) : selectPartitions(istorage);

            /// Only inspect the parts small than gc_timestamp
            TxnTimestamp gc_timestamp = calculateGCTimestamp(storage_settings->old_parts_lifetime.totalSeconds(), in_wakeup);
            if (gc_timestamp <= last_gc_timestamp) /// Skip unnecessary gc
            {
                LOG_INFO(log, "Skip unnecessary GC as gc_timestamp <= last_gc_timestamp: {} vs. {}", gc_timestamp, last_gc_timestamp);
            }
            else
            {
                /// Do GC partition by partition to avoid holding too many ServerParts.
                for (const auto & p : partitions)
                    clearOldPartsByPartition(istorage, storage, p, in_wakeup, gc_timestamp);

                last_gc_timestamp = gc_timestamp;
            }
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

void CnchPartGCThread::clearData()
{
    removeCandidatePartitions();
}

void CnchPartGCThread::clearOldPartsByPartition(const StoragePtr & istorage, StorageCnchMergeTree & storage, const String & partition_id, bool in_wakeup, TxnTimestamp gc_timestamp)
{
    auto storage_settings = storage.getSettings();
    auto now = time(nullptr);

    Stopwatch watch;
    auto all_parts = catalog->getServerDataPartsInPartitions(istorage, {partition_id}, gc_timestamp, nullptr, true);
    LOG_TRACE(log, "Get parts for {} from Catalog cost {} us, size: {}", partition_id, watch.elapsedMicroseconds(), all_parts.size());
    watch.restart();

    // Get zombie parts to remove and filter out invisible intermediate parts.
    auto intermediate_parts_to_remove = processIntermediateParts(all_parts, gc_timestamp);
    if (!intermediate_parts_to_remove.empty())
    {
        LOG_TRACE(log, "Get {} staged intermediate parts to remove for {} ", intermediate_parts_to_remove.size(), partition_id);
        pushToRemovingQueue(storage, intermediate_parts_to_remove, "Staged Intermediate Parts");
    }

    ServerDataPartsVector visible_alone_drop_ranges;
    ServerDataPartsVector invisible_dropped_parts;
    auto visible_parts = CnchPartsHelper::calcVisiblePartsForGC(all_parts, &visible_alone_drop_ranges, &invisible_dropped_parts);
    LOG_TRACE(log, "Calculate visible parts for GC cost {} us, visible_parts: {}", watch.elapsedMicroseconds(), visible_parts.size());
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
    pushToRemovingQueue(storage, invisible_dropped_parts, "invisible dropped");

    visible_alone_drop_ranges.erase(
        std::remove_if(
            visible_alone_drop_ranges.begin(),
            visible_alone_drop_ranges.end(),
            [&](auto & part) { return UInt64(now) < TxnTimestamp(part->getCommitTime()).toSecond() + old_parts_lifetime; }),
        visible_alone_drop_ranges.end());
    pushToRemovingQueue(storage, visible_alone_drop_ranges, "visible alone drop range");

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


    /// Clear special old bitmaps
    DeleteBitmapMetaPtrVector visible_bitmaps, visible_alone_tombstone_bitmaps, unvisible_bitmaps;
    if (storage.getInMemoryMetadataPtr()->hasUniqueKey())
    {
        auto all_bitmaps = catalog->getDeleteBitmapsInPartitions(istorage, {partition_id}, gc_timestamp);
        // TODO: filter out uncommitted bitmaps
        CnchPartsHelper::calcVisibleDeleteBitmaps(all_bitmaps, visible_bitmaps, true, &visible_alone_tombstone_bitmaps, &unvisible_bitmaps);
    }

    removeDeleteBitmaps(storage, unvisible_bitmaps, "covered by range tombstones");

    visible_alone_tombstone_bitmaps.erase(
        std::remove_if(
            visible_alone_tombstone_bitmaps.begin(),
            visible_alone_tombstone_bitmaps.end(),
            [&](auto & bitmap) { return UInt64(now)  < (bitmap->getCommitTime() >> 18) / 1000 + old_parts_lifetime; }),
        visible_alone_tombstone_bitmaps.end()
    );
    removeDeleteBitmaps(storage, visible_alone_tombstone_bitmaps, "alone tombstone");

    /// Clear staged parts
    if (storage.getInMemoryMetadataPtr()->hasUniqueKey())
    {
        /// staged part is tmp part which is unecessary to last for old_parts_lifetime, delete it directly
        ServerDataPartsVector staged_parts = createServerPartsFromDataParts(
            storage, catalog->getStagedParts(istorage, TxnTimestamp{storage.getContext()->getTimestamp()}));
        staged_parts = CnchPartsHelper::calcVisiblePartsForGC(staged_parts, nullptr, nullptr);
        size_t size = staged_parts.size();
        size_t handle_size = 0;
        for (size_t i = 0; i < size; ++i)
        {
            auto staged_part = staged_parts[i];
            if (!staged_part->deleted())
                continue;
            /// In response to fail situation, it's necessary to handle previous stage part before handle stage part who marks delete state.
            if (staged_part->tryGetPreviousPart())
                pushToRemovingQueue(storage, {staged_part->tryGetPreviousPart()}, "Clear staged parts", true);
            else
            {
                pushToRemovingQueue(storage, {staged_part}, "Clear staged parts", true);
                handle_size++;
            }
        }
        LOG_DEBUG(log, "All staged parts: {}, Clear staged parts {}", staged_parts.size(), handle_size);
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
        collectBetweenCheckpoints(storage, visible_parts, visible_bitmaps, checkpoints[i - 1], checkpoints[i]);
    }
}

void CnchPartGCThread::tryMarkExpiredPartitions(StorageCnchMergeTree & storage, const ServerDataPartsVector & visible_parts)
{
    time_t now = time(nullptr);

    StorageCnchMergeTree::PartitionDropInfos partition_infos;
    for (const auto & part : visible_parts)
    {
        if (part->deleted())
            continue;

        if (auto iter = partition_infos.find(part->info().partition_id); iter != partition_infos.end())
        {
            iter->second.max_block = std::max(iter->second.max_block, part->info().max_block);
        }
        else
        {
            auto ttl = storage.getTTLForPartition(part->get_partition());
            if (ttl && ttl < now)
            {
                auto it = partition_infos.try_emplace(part->info().partition_id).first;
                it->second.max_block = part->info().max_block;
                it->second.value.assign(part->partition());
            }
        }
    }

    ContextMutablePtr query_context = Context::createCopy(storage.getContext());

    auto txn = query_context->getCnchTransactionCoordinator().createTransaction(CreateTransactionOption().setInitiator(CnchTransactionInitiator::GC));
    SCOPE_EXIT({
        if (txn)
            query_context->getCnchTransactionCoordinator().finishTransaction(txn);
    });

    query_context->setCurrentTransaction(txn, false);
    // query_context->getHdfsConnectionParams().lookupOnNeed();
    query_context->setQueryContext(query_context);

    auto drop_ranges = storage.createDropRangesFromPartitions(partition_infos, txn);
    auto bitmap_tombstones = storage.createDeleteBitmapTombstones(drop_ranges, txn->getTransactionID());

    auto cnch_writer = CnchDataWriter(storage, query_context, ManipulationType::Drop);
    cnch_writer.dumpAndCommitCnchParts(drop_ranges, bitmap_tombstones);
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
    const DeleteBitmapMetaPtrVector & visible_bitmaps,
    TxnTimestamp begin,
    TxnTimestamp end)
{
    ServerDataPartsVector stale_parts;
    DeleteBitmapMetaPtrVector stale_bitmaps;

    for (const auto & part : visible_parts)
        collectStaleParts(part, begin, end, false, stale_parts);
    for (const auto & bitmap : visible_bitmaps)
        collectStaleBitmaps(bitmap, begin, end, false, stale_bitmaps);

    pushToRemovingQueue(storage, stale_parts, "stale");
    removeDeleteBitmaps(storage, stale_bitmaps, "stale between " + begin.toString() + " and " + end.toString());

    std::unordered_map<String, Int32> remove_parts_num;

    for (auto & part : stale_parts)
        ++remove_parts_num[part->info().partition_id];

    if (!remove_parts_num.empty())
    {
        std::stringstream ss;
        for (auto & [partition_id, count]: remove_parts_num)
            ss << "(" << partition_id << ", " << count << "), ";
        LOG_TRACE(log, "Removed stale parts: {}", ss.str());
    }
}

void CnchPartGCThread::collectStaleParts(
    ServerDataPartPtr parent_part,
    TxnTimestamp begin,
    TxnTimestamp end,
    bool has_visible_ancestor,
    ServerDataPartsVector & stale_parts) const
{
    do
    {
        const auto & prev_part = parent_part->tryGetPreviousPart();

        if (!prev_part)
            break;

        has_visible_ancestor = has_visible_ancestor           // inherit from parent
            || (parent_part->getCommitTime() < end.toUInt64() // parent is visible
                && !parent_part->isPartial());                // parent is a base one

        if (has_visible_ancestor && prev_part->getCommitTime() > begin.toUInt64())
        {
            LOG_DEBUG(
                log, "Will remove part {} covered by {} /partial={}", prev_part->name(), parent_part->name(), parent_part->isPartial());
            stale_parts.push_back(prev_part);
        }

        parent_part = prev_part;
    }
    while (true);
}

void CnchPartGCThread::collectStaleBitmaps(
    DeleteBitmapMetaPtr parent_bitmap,
    TxnTimestamp begin,
    TxnTimestamp end,
    bool has_visible_ancestor,
    DeleteBitmapMetaPtrVector & stale_bitmaps)
{
    do
    {
        const auto & prev_bitmap = parent_bitmap->tryGetPrevious();
        if (!prev_bitmap || prev_bitmap->getCommitTime() <= begin.toUInt64())
            break;

        /// inherit from parent;
        if (!has_visible_ancestor) // 1. parent is visible;                           &&  2. parent is a base bitmap
            has_visible_ancestor = ((parent_bitmap->getCommitTime() <= end.toUInt64()) && !parent_bitmap->isPartial());

        if (has_visible_ancestor)
        {
            LOG_DEBUG(log, "Will remove delete bitmap {} covered by {}", prev_bitmap->getNameForLogs(), parent_bitmap->getNameForLogs());
            stale_bitmaps.push_back(prev_bitmap);
        }

        parent_bitmap = prev_bitmap;
    } while (true);
}

TxnTimestamp CnchPartGCThread::calculateGCTimestamp(UInt64 delay_second, bool in_wakeup)
{
    auto local_context = getContext();

    TxnTimestamp gc_timestamp = local_context->getTimestamp();
    if (in_wakeup) /// XXX: will invalid all running queries
        return gc_timestamp;

    /// Will invalid the running queries of which the timestamp <= gc_timestamp
    auto server_min_active_ts = calculateMinActiveTimestamp();
    TxnTimestamp max_gc_timestamp = ((time(nullptr) - delay_second) * 1000) << 18;

    return std::min({gc_timestamp, server_min_active_ts, max_gc_timestamp});
}

Strings CnchPartGCThread::selectPartitions(const StoragePtr & storage)
{
    StringSet res;
    swapCandidatePartitions(res);
    LOG_TRACE(log, "Candidate partitions: {}", fmt::format("{}", fmt::join(res, ",")));

    auto from_partition_selector = partition_selector->selectForGC(storage);
    if (from_partition_selector.empty())
    {
        /// Partition selector would always return non-empty result (by round-robin or other strategies).
        /// In some corner case (like the table was detached before calling partition selector), we fall back to Catalog API.
        LOG_DEBUG(log, "Get all partitions from Catalog as partition selector returns empty result.");
        return getContext()->getCnchCatalog()->getPartitionIDs(storage, nullptr);
    }
    for (const auto & p : from_partition_selector)
        res.insert(p);
    return {res.begin(), res.end()};
}

void CnchPartGCThread::pushToRemovingQueue(
    StorageCnchMergeTree & storage, const ServerDataPartsVector & parts, const String & part_type, bool is_staged_part)
{
    auto local_context = getContext();
    auto storage_settings = storage.getSettings();

    if (parts.empty())
        return;

    LOG_DEBUG(log, "Try to remove {} {} part(s) by GCThread", parts.size(), part_type);

    /// Operations on the metastore are fast, so a fixed size pool is used.
    ThreadPool remove_pool(4);

    auto batch_remove = [&](size_t start, size_t end)
    {
        remove_pool.scheduleOrThrowOnError([&, start, end]
        {
            Catalog::TrashItems items;

            for (auto it = parts.begin() + start; it != parts.begin() + end; ++it)
            {
                if (is_staged_part)
                    items.staged_parts.push_back(*it);
                else
                    items.data_parts.push_back(*it);
            }

            LOG_DEBUG(
                log,
                "Will remove {} data part(s), {} staged part(s) to trash in CnchCatalog",
                items.data_parts.size(),
                items.staged_parts.size());


            catalog->moveDataItemsToTrash(storage.shared_from_this(), items);

            if (auto server_part_log = local_context->getServerPartLog())
            {
                auto now = time(nullptr);
                std::unordered_map<String, size_t> count_by_partition{};

                for (const auto & part: parts)
                {
                    ServerPartLogElement elem;
                    elem.event_type = ServerPartLogElement::REMOVE_PART;
                    elem.event_time = now;
                    elem.database_name = storage.getDatabaseName();
                    elem.table_name = storage.getTableName();
                    elem.uuid = storage.getStorageUUID();
                    elem.part_name = part->name();
                    elem.partition_id = part->info().partition_id;
                    elem.is_staged_part = is_staged_part;
                    elem.rows = part->rowsCount();

                    server_part_log->add(elem);

                    if (elem.rows > 0)
                        count_by_partition[elem.partition_id] += 1;
                }

                if (partition_selector)
                {
                    for (const auto & [partition, count] : count_by_partition)
                        partition_selector->addRemoveParts(storage.getStorageUUID(), partition, count, now);
                }
            }
        });
    };

    size_t batch_size = storage_settings->gc_trash_part_batch_size;

    for (size_t start = 0; start < parts.size(); start += batch_size)
    {
        auto end = std::min(start + batch_size, parts.size());
        batch_remove(start, end);
    }

    if (storage_settings->enable_gc_evict_disk_cache)
    {
        try
        {
            ContextPtr ctx = storage.getContext();
            storage.sendDropDiskCacheTasks(ctx, parts);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Fail to drop disk cache for " + storage.getStorageID().getNameForLogs());
        }
    }

    remove_pool.wait();
}

void CnchPartGCThread::removeDeleteBitmaps(StorageCnchMergeTree & storage, const DeleteBitmapMetaPtrVector & bitmaps, const String & reason)
{
    if (bitmaps.empty())
        return;

    LOG_DEBUG(log, "Try to remove {} bitmap(s) by GCThread, reason: {}", bitmaps.size(), reason);

    ThreadPool remove_pool(storage.getSettings()->gc_remove_bitmap_thread_pool_size);

    auto batch_remove = [&](size_t start, size_t end)
    {
        remove_pool.scheduleOrThrowOnError([&, start, end] {
            Catalog::TrashItems items;

            for (auto it = bitmaps.begin() + start; it != bitmaps.begin() + end; ++it)
            {
                items.delete_bitmaps.push_back(*it);
            }

            LOG_DEBUG(log, "Will remove {} delete bitmap(s), reason: {}", items.delete_bitmaps.size(), reason);

            try
            {
                getContext()->getCnchCatalog()->moveDataItemsToTrash(storage.shared_from_this(), items);
            }
            catch (Exception & e)
            {
                LOG_WARNING(log, "Error occurs when remove bitmaps from catalog. Message : ", e.what());
            }
        });
    };

    size_t batch_size = storage.getSettings()->gc_remove_bitmap_batch_size;
    size_t s = 0;

    while (s + batch_size < bitmaps.size())
    {
        batch_remove(s, s + batch_size);
        s += batch_size;
    }
    batch_remove(s, bitmaps.size());
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

void CnchPartGCThread::runDataRemoveTask()
{
    UInt64 sleep_ms = 30 * 1000;

    try
    {
        auto istorage = getStorageFromCatalog();

        if (!istorage->is_dropped)
        {
            auto & storage = checkAndGetCnchTable(istorage);
            auto storage_settings = storage.getSettings();
            size_t removed_size = clearDataFileInTrash(istorage, storage);

            if (removed_size)
            {
                sleep_ms = std::uniform_int_distribution<UInt64>(0, storage_settings->cleanup_delay_period_random_add * 1000)(rng);
                round_removing_no_data = 0;
            }
            else
            {
                round_removing_no_data++;
                sleep_ms = std::min(storage_settings->cleanup_delay_period * 1000 * std::pow(1.2, round_removing_no_data), 60 * 60 * 1000.0);
                LOG_TRACE(log, "Removed no data for {} round(s). Delay schedule for {} ms.", round_removing_no_data, sleep_ms);
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    data_remover->scheduleAfter(sleep_ms);
}

size_t CnchPartGCThread::clearDataFileInTrash(const StoragePtr & istorage, StorageCnchMergeTree & storage)
{
    size_t pool_size = storage.getSettings()->gc_remove_part_thread_pool_size;
    size_t batch_size = storage.getSettings()->gc_remove_part_batch_size;

    Stopwatch watch;
    const auto trash_items = catalog->getDataItemsInTrash(istorage, batch_size * pool_size);

    if (trash_items.empty())
        return 0;

    ThreadPool remove_pool(pool_size);
    std::atomic<std::size_t> total_removed_items;

    auto batch_remove = [&](size_t p_start, size_t p_end, size_t d_start, size_t d_end) {
        remove_pool.trySchedule([&, p_start, p_end, d_start, d_end] {
            Catalog::TrashItems trash_items_removed;

            /// Remove part file.
            for (size_t i = p_start; i < p_end; i++)
            {
                const auto & part = trash_items.data_parts[i];
                String name = part->name();
                try
                {
                    LOG_DEBUG(log, "Will remove part: {}", name);
                    auto cnch_part = part->toCNCHDataPart(storage);
                    cnch_part->remove();
                    trash_items_removed.data_parts.push_back(part);
                }
                catch (...)
                {
                    tryLogCurrentException(log, "Error occurs when remove part: " + name);
                }
            }

            /// Remove delete bitmap file
            for (size_t i = d_start; i < d_end; i++)
            {
                const auto & bitmap = trash_items.delete_bitmaps[i];
                try
                {
                    LOG_DEBUG(log, "Will remove delete bitmap : {}", bitmap->getNameForLogs());
                    bitmap->removeFile();
                    trash_items_removed.delete_bitmaps.push_back(bitmap);
                }
                catch (...)
                {
                    tryLogCurrentException(log, "Error occurs when remove bitmap " + bitmap->getNameForLogs());
                }
            }

            size_t removed_parts_number = trash_items_removed.data_parts.size();
            size_t removed_delete_bitmap_number = trash_items_removed.delete_bitmaps.size();
            LOG_DEBUG(
                log, "Will remove trash items (with {} parts, {} delete bitmaps)", removed_parts_number, removed_delete_bitmap_number);
            catalog->clearTrashItems(istorage, trash_items_removed);
            LOG_DEBUG(
                log,
                "Removed {} parts, {} delete bitmaps in {} ms.",
                removed_parts_number,
                removed_delete_bitmap_number,
                watch.elapsedMilliseconds());

            total_removed_items += (removed_parts_number + removed_delete_bitmap_number);
        });
    };

    size_t offset = 0;

    while (offset + batch_size < trash_items.data_parts.size())
    {
        batch_remove(offset, offset + batch_size, 0, 0);
        offset += batch_size;
    }
    batch_remove(offset, trash_items.data_parts.size(), 0, 0);

    offset = 0;
    while (offset + batch_size < trash_items.delete_bitmaps.size())
    {
        batch_remove(0, 0, offset, offset + batch_size);
        offset += batch_size;
    }
    batch_remove(0, 0, offset, trash_items.delete_bitmaps.size());

    remove_pool.wait();

    return total_removed_items;
}

ServerDataPartsVector CnchPartGCThread::processIntermediateParts(ServerDataPartsVector & parts, TxnTimestamp gc_timestamp)
{
    ServerDataPartsVector intermediate_parts;
    std::set<TxnTimestamp> txn_ids;
    // Collect all intermediate parts and related transactions.
    parts.erase(std::remove_if(parts.begin(), parts.end(),
        [&](const ServerDataPartPtr & p)
        {
            if (p->getCommitTime() == IMergeTreeDataPart::NOT_INITIALIZED_COMMIT_TIME)
            {
                intermediate_parts.push_back(p);
                txn_ids.insert(p->part_model_wrapper->part_model->part_info().mutation());
                return true;
            }
            return false;
        }
    ), parts.end());

    // no intermediate part to process.
    if (txn_ids.empty())
        return {};

    // get all transaction records that intermediated parts belongs to from catalog.
    std::vector<TransactionRecord> txn_records;
    try
    {
        txn_records = catalog->getTransactionRecords(std::vector<TxnTimestamp>(txn_ids.begin(), txn_ids.end()), 100000);
    }
    catch (...)
    {
        LOG_WARNING(log, "Fail to get transaction records from catalog. Will skip all intermediate parts.");
        return {};
    }

    std::unordered_map<UInt64, TxnTimestamp> transactions; //txn_id -> commit_ts
    // find out inactive and finished transactions
    for (const auto & record : txn_records)
    {
        if (record.isInactive())
            transactions.emplace(record.txnID(), 0); // commit_ts == 0 means txn is not active here.
        else if (record.status() == CnchTransactionStatus::Finished)
            transactions.emplace(record.txnID(), record.commitTs());
    }

    // return if no zombie/active intermediate parts to process
    if (transactions.empty())
        return {};

    // now collect zombie intermediate parts to remove and put back visible intermediate parts for further processing
    ServerDataPartsVector zombie_intermediate_parts;
    for (const auto & part : intermediate_parts)
    {
        auto txn_id = part->part_model_wrapper->part_model->part_info().mutation();
        // skip those invisible intermediate parts (not zombie).
        if (!transactions.contains(txn_id))
            continue;

        if (!transactions[txn_id])
            zombie_intermediate_parts.push_back(part);
        else if (transactions[txn_id] <= gc_timestamp)
            parts.push_back(part);
    }
    return zombie_intermediate_parts;
}

}
