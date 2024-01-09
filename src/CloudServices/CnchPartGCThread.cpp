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
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ServerPartLog.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Storages/StorageCnchMergeTree.h>
#include <WorkerTasks/ManipulationType.h>
#include <Poco/Exception.h>

namespace CurrentMetrics
{
extern const Metric BackgroundGCSchedulePoolTask;
}

namespace DB
{

CnchPartGCThread::CnchPartGCThread(ContextPtr context_, const StorageID & id) : ICnchBGThread(context_, CnchBGThreadType::PartGC, id)
{
    partition_selector = getContext()->getBGPartitionSelector();
    data_remover = getContext()
                       ->getExtraSchedulePool(
                           SchedulePool::GC,
                           getContext()->getSettingsRef().background_gc_schedule_pool_size,
                           CurrentMetrics::BackgroundGCSchedulePoolTask,
                           "GCPool")
                       .createTask(log->name() + "(remover)", [this] { runDataRemoveTask(); });
    data_remover->deactivate();
}

void CnchPartGCThread::executeManually(const ASTPtr & partition, ContextPtr local_context)
{
    auto istorage = getStorageFromCatalog();
    auto & storage = checkAndGetCnchTable(istorage);
    if (istorage->is_dropped)
    {
        LOG_DEBUG(log, "Table was dropped, do nothing...");
        return;
    }

    Strings partitions;
    if (partition)
        partitions.push_back(storage.getPartitionIDFromQuery(partition, local_context));
    else
        partitions = catalog->getPartitionIDs(istorage, nullptr);

    Stopwatch watch;
    doPhaseOneGC(istorage, storage, partitions);
    LOG_INFO(log, "Successfully execute phase-one GC in {} ms", watch.elapsedMilliseconds());

    watch.restart();
    auto nremoved = doPhaseTwoGC(istorage, storage);
    LOG_INFO(log, "Successfully execute phase-two GC in {} ms, removed {} files", watch.elapsedMilliseconds(), nremoved);
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

void CnchPartGCThread::doPhaseOneGC(const StoragePtr & istorage, StorageCnchMergeTree & storage, const Strings & partitions)
{
    auto storage_settings = storage.getSettings();
    bool in_wakeup = inWakeup() || static_cast<bool>(storage_settings->gc_ignore_running_transactions_for_test);
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
            doPhaseOnePartitionGC(istorage, storage, p, in_wakeup, gc_timestamp);

        last_gc_timestamp = gc_timestamp;
    }

    clearOldInsertionLabels(istorage, storage);
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
            Strings partitions = inWakeup() ? catalog->getPartitionIDs(istorage, nullptr) : selectPartitions(istorage);
            doPhaseOneGC(istorage, storage, partitions);
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
    txn->commitV2();
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
        return catalog->getPartitionIDs(storage, nullptr);
    }
    for (const auto & p : from_partition_selector)
        res.insert(p);
    return {res.begin(), res.end()};
}

void CnchPartGCThread::movePartsToTrash(const StoragePtr & storage, const ServerDataPartsVector & parts, bool is_staged, String log_type, size_t pool_size, size_t batch_size)
{
    auto local_context = getContext();

    if (parts.empty())
        return;

    LOG_DEBUG(log, "Try to clear metadata of {} {}", parts.size(), log_type);

    /// TODO: adjust pool_size based on num_batch
    ThreadPool remove_pool(pool_size);
    std::atomic<size_t> num_moved = 0;

    auto batch_remove = [&](size_t start, size_t end)
    {
        remove_pool.scheduleOrThrowOnError([&, start, end]
        {
            Catalog::TrashItems items;

            for (auto it = parts.begin() + start; it != parts.begin() + end; ++it)
            {
                if (is_staged)
                    items.staged_parts.push_back(*it);
                else
                    items.data_parts.push_back(*it);
            }

            try
            {
                catalog->moveDataItemsToTrash(storage, items);
                num_moved.fetch_add(is_staged ? items.staged_parts.size() : items.data_parts.size());

                if (auto server_part_log = local_context->getServerPartLog())
                {
                    auto now = time(nullptr);
                    std::unordered_map<String, size_t> count_by_partition{};

                    for (const auto & part: parts)
                    {
                        /// TODO: add commit_time and end_time field
                        ServerPartLogElement elem;
                        elem.event_type = ServerPartLogElement::REMOVE_PART;
                        elem.event_time = now;
                        elem.database_name = storage->getDatabaseName();
                        elem.table_name = storage->getTableName();
                        elem.uuid = storage->getStorageUUID();
                        elem.part_name = part->name();
                        elem.partition_id = part->info().partition_id;
                        elem.is_staged_part = is_staged;
                        elem.rows = part->rowsCount();

                        server_part_log->add(elem);

                        if (elem.rows > 0)
                            count_by_partition[elem.partition_id] += 1;
                    }

                    if (partition_selector)
                    {
                        for (const auto & [partition, count] : count_by_partition)
                            partition_selector->addRemoveParts(storage->getStorageUUID(), partition, count, now);
                    }
                }
            }
            catch (Exception & e)
            {
                e.addMessage("while moving " + log_type + " to trash");
                tryLogCurrentException(log);
            }
        });
    };

    for (size_t start = 0; start < parts.size(); start += batch_size)
    {
        auto end = std::min(start + batch_size, parts.size());
        batch_remove(start, end);
    }
    remove_pool.wait();
    LOG_DEBUG(log, "Successfully cleared metadata of {} {}", num_moved.load(), log_type);
}

void CnchPartGCThread::moveDeleteBitmapsToTrash(const StoragePtr & storage, const DeleteBitmapMetaPtrVector & bitmaps, size_t pool_size, size_t batch_size)
{
    if (bitmaps.empty())
        return;

    LOG_DEBUG(log, "Try to move {} delete bitmaps to trash", bitmaps.size());

    ThreadPool remove_pool(pool_size);
    std::atomic<size_t> num_moved = 0;
    auto batch_remove = [&](size_t start, size_t end)
    {
        remove_pool.scheduleOrThrowOnError([&, start, end] {
            Catalog::TrashItems items;

            for (auto it = bitmaps.begin() + start; it != bitmaps.begin() + end; ++it)
            {
                const auto & bitmap = *it;
                LOG_DEBUG(
                    log,
                    "Will move bitmap to trash: {} [{} - {}) {}",
                    bitmap->getNameForLogs(),
                    bitmap->getCommitTime(),
                    bitmap->getEndTime(),
                    (bitmap->isTombstone() ? "tombstone" : ""));
                items.delete_bitmaps.push_back(bitmap);
            }

            try
            {
                catalog->moveDataItemsToTrash(storage, items);
                num_moved.fetch_add(items.delete_bitmaps.size());
            }
            catch (Exception & e)
            {
                LOG_WARNING(log, "Error occurs when removing stale delete bitmaps from catalog. Message : ", e.what());
            }
        });
    };

    for (size_t start = 0; start < bitmaps.size(); start += batch_size)
    {
        auto end = std::min(start + batch_size, bitmaps.size());
        batch_remove(start, end);
    }
    remove_pool.wait();
    LOG_DEBUG(log, "Successfully moved {} delete bitmaps to trash", num_moved.load());
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
            size_t removed_size = doPhaseTwoGC(istorage, storage);

            auto storage_settings = storage.getSettings();
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

size_t CnchPartGCThread::doPhaseTwoGC(const StoragePtr & istorage, StorageCnchMergeTree & storage)
{
    std::vector<UInt64> snapshot_times;
    auto db = DatabaseCatalog::instance().getDatabase(storage.getDatabaseName(), getContext());
    if (db->supportSnapshot())
    {
        auto snapshots = db->getAllSnapshotsForStorage(istorage->getStorageUUID());
        for (auto & ele : snapshots)
            snapshot_times.push_back(ele->commit_time());
        std::sort(snapshot_times.begin(), snapshot_times.end());
    }

    const UInt64 ttl_for_trash_items = static_cast<UInt64>(storage.getSettings()->ttl_for_trash_items.totalSeconds());
    TxnTimestamp gc_timestamp = TxnTimestamp::fromUnixTimestamp(time(nullptr) - ttl_for_trash_items);

    /// remove all ts > gc_timestamp
    std::erase_if(snapshot_times, [&](auto ts) { return ts > gc_timestamp; });
    snapshot_times.push_back(gc_timestamp.toUInt64());

    auto can_remove = [&](UInt64 commit_ts, UInt64 end_ts) -> bool
    {
        /// zombie intermediate parts or items created in old version
        if (end_ts == 0)
            return true; /// same behavior as before

        if (commit_ts > end_ts)
        {
            /// TODO: log warning
            return false;
        }

        auto it = std::upper_bound(snapshot_times.begin(), snapshot_times.end(), end_ts);

        /// end_ts >= gc_timestamp
        /// keep item within fail-safe period
        if (it == snapshot_times.end())
            return false;

        /// end_ts < snapshot_times.front()
        /// remove item which is not visible to the earliest snapshot
        if (it == snapshot_times.begin())
            return true;

        /// commit_ts > *(it-1) && end_ts < *(it)
        /// item is created and then removed between consecutive snapshots
        /// remove it because it's not visible to any snapshot
        if (commit_ts > *std::prev(it))
            return true;

        return false;
    };

    size_t pool_size = std::max(static_cast<size_t>(storage.getSettings()->gc_remove_part_thread_pool_size), static_cast<size_t>(1));
    size_t batch_size = storage.getSettings()->gc_remove_part_batch_size;
    auto trash_items = catalog->getDataItemsInTrash(istorage, /*limit*/ pool_size * batch_size);

    Catalog::TrashItems items_to_remove;
    for (auto & part : trash_items.data_parts)
    {
        if (can_remove(part->getCommitTime(), part->getEndTime()))
        {
            items_to_remove.data_parts.push_back(part);
        }
    }
    for (auto & bitmap : trash_items.delete_bitmaps)
    {
        if (can_remove(bitmap->getCommitTime(), bitmap->getEndTime()))
        {
            items_to_remove.delete_bitmaps.push_back(bitmap);
        }
    }

    if (items_to_remove.empty())
        return 0;

    /// remove data file and meta of items_to_remove
    ThreadPool remove_pool(pool_size);
    std::atomic<size_t> ntotal = 0;

    auto batch_remove = [&](size_t p_beg, size_t p_end, size_t d_beg, size_t d_end) {
        /// TODO: old impl use trySchedule, what's the difference?
        remove_pool.scheduleOrThrowOnError([&, p_beg, p_end, d_beg, d_end] {
            Stopwatch watch;
            Catalog::TrashItems items_removed;

            for (size_t i = p_beg; i < p_end; ++i)
            {
                const auto & part = items_to_remove.data_parts[i];
                try
                {
                    auto cnch_part = part->toCNCHDataPart(storage);
                    LOG_DEBUG(log, "Will remove part {} under path {}", part->name(), cnch_part->getFullRelativePath());
                    cnch_part->remove();
                    items_removed.data_parts.push_back(part);
                }
                catch (...)
                {
                    tryLogCurrentException(log, "Error occurs while remove part " + part->name());
                }
            }

            for (size_t i = d_beg; i < d_end; ++i)
            {
                const auto & bitmap = items_to_remove.delete_bitmaps[i];
                try
                {
                    auto opt_path = bitmap->getFullRelativePath();
                    LOG_DEBUG(log, "Will remove delete bitmap {} under path {}", bitmap->getNameForLogs(), opt_path.has_value() ? *opt_path : "");
                    bitmap->removeFile();
                    items_removed.delete_bitmaps.push_back(bitmap);
                }
                catch (...)
                {
                    tryLogCurrentException(log, "Error occurs while remove delete bitmap " + bitmap->getNameForLogs());
                }
            }

            size_t nparts = items_removed.data_parts.size();
            size_t nbitmaps = items_removed.delete_bitmaps.size();
            LOG_DEBUG(log, "Will remove trash records of {} parts, {} delete bitmaps", nparts, nbitmaps);
            catalog->clearTrashItems(istorage, items_removed);
            LOG_DEBUG(
                log, "Completely removed data of {} parts and {} delete bitmaps in {} ms", nparts, nbitmaps, watch.elapsedMilliseconds());
            ntotal += (nparts + nbitmaps);
        });
    };

    for (size_t i = 0; i < items_to_remove.data_parts.size(); i += batch_size)
    {
        size_t end = std::min(i + batch_size, items_to_remove.data_parts.size());
        batch_remove(i, end, 0, 0);
    }
    for (size_t i = 0; i < items_to_remove.delete_bitmaps.size(); i += batch_size)
    {
        size_t end = std::min(i + batch_size, items_to_remove.delete_bitmaps.size());
        batch_remove(0, 0, i, end);
    }
    remove_pool.wait();
    return ntotal;
}

ServerDataPartsVector CnchPartGCThread::processIntermediateParts(ServerDataPartsVector & parts, TxnTimestamp gc_timestamp)
{
    ServerDataPartsVector intermediate_parts;
    std::set<TxnTimestamp> txn_ids;
    // Collect all intermediate parts and related transactions.
    std::erase_if(parts, [&](const ServerDataPartPtr & p) {
        if (p->getCommitTime() == IMergeTreeDataPart::NOT_INITIALIZED_COMMIT_TIME)
        {
            intermediate_parts.push_back(p);
            txn_ids.insert(p->txnID());
            return true;
        }
        return false;
    });

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
        auto txn_id = part->txnID();
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

void CnchPartGCThread::doPhaseOnePartitionGC(const StoragePtr & istorage, StorageCnchMergeTree & storage, const String & partition_id, bool in_wakeup, TxnTimestamp gc_timestamp)
{
    auto storage_settings = storage.getSettings();
    auto now = time(nullptr);

    Stopwatch watch;
    // for gc thread, intermediate parts are required in case that TransactionCleaner fails to clean them.
    auto all_parts = catalog->getServerDataPartsInPartitions(istorage, {partition_id}, gc_timestamp, nullptr, Catalog::VisibilityLevel::All);
    LOG_TRACE(log, "Get {} parts for {} from Catalog cost {} us", all_parts.size(), partition_id, watch.elapsedMicroseconds());
    watch.restart();

    // Get zombie parts to remove and filter out invisible intermediate parts.
    auto intermediate_parts_to_remove = processIntermediateParts(all_parts, gc_timestamp);
    if (!intermediate_parts_to_remove.empty())
    {
        LOG_TRACE(log, "Get {} intermediate parts to remove for {} ", intermediate_parts_to_remove.size(), partition_id);
        movePartsToTrash(istorage, intermediate_parts_to_remove, /*is_staged*/ false, "intermediate parts",
            storage_settings->gc_trash_part_thread_pool_size, storage_settings->gc_trash_part_batch_size);
    }

    auto visible_parts = CnchPartsHelper::calcVisibleParts(all_parts, false);
    LOG_TRACE(log, "Calculate visible parts cost {} us, visible_parts: {}", watch.elapsedMicroseconds(), visible_parts.size());

    /// Generate DROP_RANGE for expired partitions by the TTL
    if (!visible_parts.empty())
        tryMarkExpiredPartitions(storage, visible_parts);

    /// Clear special old parts
    UInt64 old_parts_lifetime = in_wakeup ? 0ull : static_cast<UInt64>(storage_settings->old_parts_lifetime.totalSeconds());

    /// clear parts
    {
        watch.restart();
        ServerDataPartsVector parts_to_gc;
        CnchPartsHelper::calcPartsForGC(all_parts, &parts_to_gc, nullptr);
        LOG_DEBUG(
            log,
            "Get {}/{} parts to gc at {}({}), took {} us",
            parts_to_gc.size(),
            all_parts.size(),
            gc_timestamp,
            LocalDateTime(gc_timestamp.toSecond()),
            watch.elapsedMicroseconds());

        std::erase_if(parts_to_gc, [&](auto & part) {
            return !part->getEndTime() || static_cast<UInt64>(now) < TxnTimestamp(part->getEndTime()).toSecond() + old_parts_lifetime;
        });

        if (size_t limit = storage_settings->gc_trash_part_limit; limit && parts_to_gc.size() > limit)
            parts_to_gc.resize(limit);

        if (storage_settings->enable_gc_evict_disk_cache)
        {
            try
            {
                storage.sendDropDiskCacheTasks(getContext(), parts_to_gc);
            }
            catch (...)
            {
                tryLogCurrentException(log, "Fail to drop disk cache for " + storage.getStorageID().getNameForLogs());
            }
        }

        movePartsToTrash(istorage, parts_to_gc, /*is_staged*/ false, "parts",
            storage_settings->gc_trash_part_thread_pool_size, storage_settings->gc_trash_part_batch_size);
    }

    /// clear delete bitmaps
    if (storage.getInMemoryMetadataPtr()->hasUniqueKey())
    {
        watch.restart();
        /// TODO: handle zombie intermediate bitmaps
        DeleteBitmapMetaPtrVector all_bitmaps = catalog->getDeleteBitmapsInPartitions(istorage, {partition_id}, gc_timestamp);
        DeleteBitmapMetaPtrVector bitmaps_to_gc;
        CnchPartsHelper::calcBitmapsForGC(all_bitmaps, &bitmaps_to_gc, nullptr);
        LOG_DEBUG(
            log,
            "Get {}/{} bitmaps to gc at {}({}), took {} us",
            bitmaps_to_gc.size(),
            all_bitmaps.size(),
            gc_timestamp,
            LocalDateTime(gc_timestamp.toSecond()),
            watch.elapsedMicroseconds());

        std::erase_if(bitmaps_to_gc, [&](auto & bitmap) {
            return !bitmap->getEndTime() || static_cast<UInt64>(now) < TxnTimestamp(bitmap->getEndTime()).toSecond() + old_parts_lifetime;
        });
        moveDeleteBitmapsToTrash(istorage, bitmaps_to_gc, storage_settings->gc_remove_bitmap_thread_pool_size, storage_settings->gc_remove_bitmap_batch_size);
    }

    /// clear staged parts
    if (storage.getInMemoryMetadataPtr()->hasUniqueKey())
    {
        watch.restart();
        NameSet partition_filter { partition_id };
        /// not using gc_timestamp here because
        /// staged part is tmp part which is unecessary to last for old_parts_lifetime, delete it directly
        TxnTimestamp ts = storage.getContext()->getTimestamp();
        ServerDataPartsVector all_staged_parts = catalog->getStagedServerDataParts(istorage, ts, &partition_filter);
        ServerDataPartsVector staged_parts_to_gc;
        CnchPartsHelper::calcPartsForGC(all_staged_parts, &staged_parts_to_gc, nullptr);
        LOG_DEBUG(
            log,
            "Get {}/{} staged parts to gc at {}({}), took {} us",
            staged_parts_to_gc.size(),
            all_staged_parts.size(),
            ts,
            LocalDateTime(ts.toSecond()),
            watch.elapsedMicroseconds());

        if (size_t limit = storage_settings->gc_trash_part_limit; limit && staged_parts_to_gc.size() > limit)
            staged_parts_to_gc.resize(limit);

        /// due to its temporary nature, staged parts don't actually have a trash,
        /// `Catalog::moveDataItemsToTrash` will removes its metadata directly.
        movePartsToTrash(istorage, staged_parts_to_gc, /*is_staged*/ true, "staged parts",
            storage_settings->gc_trash_part_thread_pool_size, storage_settings->gc_trash_part_batch_size);
    }
}

}
