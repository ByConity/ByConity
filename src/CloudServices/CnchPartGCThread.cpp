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
#include <CloudServices/CnchBGThreadPartitionSelector.h>
#include <CloudServices/CnchPartsHelper.h>
#include <CloudServices/CnchDataWriter.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ServerPartLog.h>
#include <Storages/MergeTree/MergeTreeBgTaskStatistics.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Storages/StorageCnchMergeTree.h>
#include <WorkerTasks/ManipulationType.h>
#include <Poco/Exception.h>
#include "IO/WriteBufferFromString.h"

namespace CurrentMetrics
{
extern const Metric BackgroundGCSchedulePoolTask;
}

namespace DB
{

CnchPartGCThread::CnchPartGCThread(ContextPtr context_, const StorageID & id) : ICnchBGThread(context_, CnchBGThreadType::PartGC, id)
{
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

bool CnchPartGCThread::doPhaseOneGC(const StoragePtr & istorage, StorageCnchMergeTree & storage, const Strings & partitions)
{
    bool items_removed = false;
    Strings may_empty_partitions;
    auto storage_settings = storage.getSettings();
    bool in_wakeup = inWakeup() || static_cast<bool>(storage_settings->gc_ignore_running_transactions_for_test);
    /// Only inspect the parts small than gc_timestamp
    TxnTimestamp gc_timestamp = calculateGCTimestamp(storage_settings->old_parts_lifetime.totalSeconds(), in_wakeup);
    if (gc_timestamp <= last_gc_timestamp) /// Skip unnecessary gc
    {
        LOG_INFO(log, "[p1] Skip unnecessary GC as gc_timestamp <= last_gc_timestamp: {} vs. {}", gc_timestamp, last_gc_timestamp);
    }
    else
    {
        /// Do GC partition by partition to avoid holding too many ServerParts.
        for (const auto & p : partitions)
        {
            size_t items_in_partition = doPhaseOnePartitionGC(istorage, storage, p, in_wakeup, gc_timestamp, items_removed);
            if (items_in_partition == 0)
                may_empty_partitions.emplace_back(p);
        }

        last_gc_timestamp = gc_timestamp;
    }

    if (!may_empty_partitions.empty())
        clearEmptyPartitions(istorage, storage, may_empty_partitions);

    clearOldInsertionLabels(istorage, storage);
    return items_removed;
}

void CnchPartGCThread::runImpl()
{
    UInt64 sleep_ms = 30 * 1000;

    try
    {
        if (!data_remover->taskIsActive())
        {
            // if data remover is not active and scheduled, schedule it
            LOG_DEBUG(log, "[p1] Start data remover task");
            data_remover->activateAndSchedule();
        }

        auto istorage = getStorageFromCatalog();
        if (istorage->is_dropped)
        {
            LOG_DEBUG(log, "[p1] Table was dropped, wait for removing...");
            scheduled_task->scheduleAfter(10 * 1000);
            return;
        }

        auto & storage = checkAndGetCnchTable(istorage);
        auto storage_settings = storage.getSettings();

        try
        {
            Strings partitions = inWakeup() ? catalog->getPartitionIDs(istorage, nullptr) : selectPartitions(istorage, storage_settings);
            auto hit = doPhaseOneGC(istorage, storage, partitions);
            phase_one_continuous_hits = hit ? phase_one_continuous_hits + 1 : 0;
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }

        if (phase_one_continuous_hits != 0)
            sleep_ms = std::uniform_int_distribution<UInt64>(0, storage_settings->cleanup_delay_period_random_add * 1000)(rng);
        else
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

/// Calculate partition's ttl value and drop expired partitions.
/// There are two ways to get a partition's ttl:
/// - If TTL expression is matched with PARTITION BY expression, just evaluate the TTL expression on PARTITION keys.
/// - Otherwise, scan all parts and get the max ttl value as the result.
void CnchPartGCThread::tryMarkExpiredPartitions(StorageCnchMergeTree & storage, const ServerDataPartsVector & visible_parts)
{
    time_t now = time(nullptr);

    bool use_partition_ttl = storage.getInMemoryMetadataPtr()->hasPartitionLevelTTL();
    bool use_partition_ttl_fallback = storage.getInMemoryMetadataPtr()->hasRowsTTL() && storage.getSettings()->enable_partition_ttl_fallback;
    if (!use_partition_ttl && !use_partition_ttl_fallback)
        return;

    StorageCnchMergeTree::PartitionDropInfos partition_infos;

    /// Option 1. Get ttl by evaluating TTL expression on partition keys.
    /// And update max_block (for generating DROP_RANGE) if ttl is expired.
    if (use_partition_ttl)
    {
        NameSet skip_partitions;
        for (const auto & part : visible_parts)
        {
            if (part->deleted())
                continue;

            const auto partition_id = part->info().partition_id;
            if (skip_partitions.contains(partition_id))
                continue;

            if (auto iter = partition_infos.find(partition_id); iter != partition_infos.end())
            {
                iter->second.max_block = std::max(iter->second.max_block, part->info().max_block);
            }
            else
            {
                auto ttl = storage.getTTLForPartition(part->get_partition());
                if (ttl && ttl < now)
                {
                    LOG_TRACE(log, "[partition] {} is expired, ttl value is {}", partition_id, ttl);
                    auto it = partition_infos.try_emplace(partition_id).first;
                    it->second.max_block = part->info().max_block;
                    it->second.value.assign(part->partition());
                }
                else
                {
                    skip_partitions.insert(partition_id);
                }
            }
        }
    }
    /// Option 2. Get ttl by scanning parts' max_ttl.
    else if (use_partition_ttl_fallback)
    {
        NameSet skip_partitions;
        for (const auto & part : visible_parts)
        {
            if (part->deleted())
                continue;

            const auto partition_id = part->info().partition_id;
            if (skip_partitions.contains(partition_id))
                continue;

            auto ttl = part->part_model().has_part_ttl_info() ? part->part_model().part_ttl_info().part_max_ttl() : 0;
            if (ttl == 0 || ttl >= static_cast<UInt64>(now))
            {
                skip_partitions.insert(partition_id);
                continue;
            }

            if (auto iter = partition_infos.find(partition_id); iter != partition_infos.end())
            {
                iter->second.max_block = std::max(iter->second.max_block, part->info().max_block);
            }
            else
            {
                auto it = partition_infos.try_emplace(partition_id).first;
                it->second.max_block = part->info().max_block;
                it->second.value.assign(part->partition());
            }
        }

        for (const auto & p : skip_partitions)
        {
            partition_infos.erase(p);
        }
    }

    if (partition_infos.empty())
        return;

    WriteBufferFromOwnString wb;
    for (const auto & [partition, info]: partition_infos)
        wb << partition << ", " << info.max_block << "; ";
    LOG_TRACE(log, "[partition] expired partitions and corresponding max_block list: {}", wb.str());

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

/// the returned ts is guaranteed to be non-zero.
TxnTimestamp CnchPartGCThread::calculateGCTimestamp(UInt64 delay_second, bool in_wakeup)
{
    if (in_wakeup) /// XXX: will invalid all running queries
        return getContext()->getTimestamp();

    /// Will invalid the running queries of which the timestamp <= gc_timestamp
    auto server_min_active_ts = calculateMinActiveTimestamp();
    TxnTimestamp max_gc_timestamp = ((time(nullptr) - delay_second) * 1000) << 18;

    return std::min({server_min_active_ts, max_gc_timestamp});
}

Strings CnchPartGCThread::selectPartitions(const StoragePtr & istorage, MergeTreeSettingsPtr & storage_settings)
{
    StringSet res;
    swapCandidatePartitions(res);
    LOG_TRACE(log, "[p1] Candidate partitions: {}", fmt::format("{}", fmt::join(res, ",")));

    auto bg_task_stats = MergeTreeBgTaskStatisticsInitializer::instance().getOrCreateTableStats(storage_id);
    auto get_all_partitions_callback = [&istorage, &bg_task_stats, this] () {
        Strings partition_ids = catalog->getPartitionIDs(istorage, nullptr);
        /// Add all unknown partitions to bg task stats, so these partitions can be selected by partition selector next time.
        bg_task_stats->fillMissingPartitions(partition_ids);
        return partition_ids;
    };
    CnchBGThreadPartitionSelector partition_selector(storage_id, bg_task_stats, {}, get_all_partitions_callback);

    auto from_partition_selector = partition_selector.selectForGC(
        /* n = */ storage_settings->cnch_gc_round_robin_partitions_number.value,
        /* round_robin_interval = */ storage_settings->cnch_gc_round_robin_partitions_interval.value,
        partition_round_robin_state);

    if (from_partition_selector.empty())
    {
        /// Partition selector would always return non-empty result (by round-robin or other strategies).
        /// In some corner case (like the table was detached before calling partition selector), we fall back to Catalog API.
        LOG_DEBUG(log, "[p1] Get all partitions from Catalog as partition selector returns empty result.");
        return get_all_partitions_callback();
    }
    for (const auto & p : from_partition_selector)
        res.insert(p);
    return {res.begin(), res.end()};
}

void CnchPartGCThread::movePartsToTrash(const StoragePtr & storage, const ServerDataPartsVector & parts, bool is_staged, String log_type, size_t pool_size, size_t batch_size, bool is_zombie_with_staging_txn_id)
{
    auto local_context = getContext();

    if (parts.empty())
        return;

    LOG_DEBUG(log, "[p1] Try to clear metadata of {} {}", parts.size(), log_type);

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
                catalog->moveDataItemsToTrash(storage, items, is_zombie_with_staging_txn_id);
                num_moved.fetch_add(is_staged ? items.staged_parts.size() : items.data_parts.size());
                ServerPartLog::addRemoveParts(local_context, storage_id, parts, is_staged);
            }
            catch (Exception & e)
            {
                e.addMessage("while moving " + log_type + " to trash");
                tryLogCurrentException(log);
            }
        });
    };

    try
    {
        for (size_t start = 0; start < parts.size(); start += batch_size)
        {
            auto end = std::min(start + batch_size, parts.size());
            batch_remove(start, end);
        }
        remove_pool.wait();
        LOG_DEBUG(log, "[p1] Successfully cleared metadata of {} {}", num_moved.load(), log_type);
    }
    catch (...)
    {
        remove_pool.wait();
        throw;
    }
}

void CnchPartGCThread::moveDeleteBitmapsToTrash(const StoragePtr & storage, const DeleteBitmapMetaPtrVector & bitmaps, size_t pool_size, size_t batch_size)
{
    if (bitmaps.empty())
        return;

    LOG_DEBUG(log, "[p1] Try to move {} delete bitmaps to trash", bitmaps.size());

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
                    "[p1] Will move bitmap to trash: {} [{} - {}) {}",
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
                LOG_WARNING(log, "[p1] Error occurs when removing stale delete bitmaps from catalog. Message : ", e.what());
            }
        });
    };

    for (size_t start = 0; start < bitmaps.size(); start += batch_size)
    {
        auto end = std::min(start + batch_size, bitmaps.size());
        batch_remove(start, end);
    }
    remove_pool.wait();
    LOG_DEBUG(log, "[p1] Successfully moved {} delete bitmaps to trash", num_moved.load());
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
    LOG_DEBUG(log, "[p1] Removed {} insertion labels.", labels_to_remove.size());
}

void CnchPartGCThread::runDataRemoveTask()
{
    size_t sleep_ms = 30 * 1000;

    try
    {
        auto istorage = getStorageFromCatalog();

        if (!istorage->is_dropped)
        {
            auto & storage = checkAndGetCnchTable(istorage);
            cleaned_items_in_a_round += doPhaseTwoGC(istorage, storage);

            auto storage_settings = storage.getSettings();
            if (!phase_two_start_key.empty() || cleaned_items_in_a_round)
            {
                sleep_ms
                    = std::uniform_int_distribution<UInt64>(0, storage_settings->cleanup_delay_period_random_add * 1000)(rng);
                round_removing_no_data = 0;
                phase_two_continuous_hits++;
            }
            else
            {
                round_removing_no_data++;
                phase_two_continuous_hits = 0;
                sleep_ms = storage_settings->cleanup_delay_period_upper_bound * 1000;
                LOG_TRACE(log, "[p2] Removed no data for {} round(s). Delay schedule for {} ms.", round_removing_no_data, sleep_ms);
            }

            if (phase_two_start_key.empty())
                cleaned_items_in_a_round = 0;
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
            /// NOTE: When `INSERT` and `TRUNCATE` are executed concurrently,
            /// it is possible that the DROP_RANGE part is generated before the inserted parts committed.
            /// In this case, these parts will be invisible to users immediately, but leaves commit_ts > end_ts.
            /// We should delete them otherwise the disk space will not be released.
            LOG_WARNING(log, "[p2] receive a item that commit_ts {} > end_ts {}", commit_ts, end_ts);
            return true;
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

    /// pool_size should be at least 1.
    size_t pool_size = std::max(
        std::min(
            /// Avoid the number get too large.
            static_cast<size_t>(2 * std::pow(2.0, std::min(phase_two_continuous_hits, 15ul))),
            static_cast<size_t>(storage.getSettings()->gc_remove_part_thread_pool_size)),
        1ul);
    /// If batch_size <= 1, then round-robin may never move forward.
    size_t batch_size = std::max(static_cast<size_t>(storage.getSettings()->gc_remove_part_batch_size), static_cast<size_t>(2));
    LOG_TRACE(
        log,
        "[p2] getDataItemsInTrash start with start_key: `{}`, pool_size: {}, batch_size: {}",
        phase_two_start_key,
        pool_size,
        batch_size);
    auto trash_items = catalog->getDataItemsInTrash(istorage, /*limit*/ pool_size * batch_size, &phase_two_start_key);
    LOG_TRACE(log, "[p2] getDataItemsInTrash end with start_key: `{}`, items: {}", phase_two_start_key, trash_items.size());

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
    LOG_TRACE(log, "[p2] items after filtered: {}", items_to_remove.size());

    if (items_to_remove.empty())
        return 0;

    /// remove data file and meta of items_to_remove
    ThreadPool remove_pool(pool_size);
    std::atomic<size_t> ntotal = 0;

    auto batch_remove = [&](size_t p_beg, size_t p_end, size_t d_beg, size_t d_end) {
        remove_pool.scheduleOrThrowOnError([&, p_beg, p_end, d_beg, d_end] {
            Stopwatch watch;
            Catalog::TrashItems items_removed;

            for (size_t i = p_beg; i < p_end; ++i)
            {
                const auto & part = items_to_remove.data_parts[i];
                try
                {
                    auto cnch_part = part->toCNCHDataPart(storage);
                    LOG_TRACE(log, "[p2] Will remove part {} under path {}", part->name(), cnch_part->getFullRelativePath());
                    cnch_part->remove();
                    items_removed.data_parts.push_back(part);
                }
                catch (...)
                {
                    tryLogCurrentException(log, "[p2] Error occurs while remove part " + part->name());
                }
            }

            for (size_t i = d_beg; i < d_end; ++i)
            {
                const auto & bitmap = items_to_remove.delete_bitmaps[i];
                try
                {
                    auto opt_path = bitmap->getFullRelativePath();
                    LOG_TRACE(log, "[p2] Will remove delete bitmap {} under path {}", bitmap->getNameForLogs(), opt_path.has_value() ? *opt_path : "");
                    bitmap->removeFile();
                    items_removed.delete_bitmaps.push_back(bitmap);
                }
                catch (...)
                {
                    tryLogCurrentException(log, "[p2] Error occurs while remove delete bitmap " + bitmap->getNameForLogs());
                }
            }

            size_t nparts = items_removed.data_parts.size();
            size_t nbitmaps = items_removed.delete_bitmaps.size();
            LOG_DEBUG(log, "[p2] Will remove trash records of {} parts, {} delete bitmaps", nparts, nbitmaps);
            catalog->clearTrashItems(istorage, items_removed);
            ServerPartLog::addDeleteParts(getContext(), storage_id, items_removed.data_parts);
            LOG_DEBUG(
                log, "[p2] Completely removed data of {} parts and {} delete bitmaps in {} ms", nparts, nbitmaps, watch.elapsedMilliseconds());
            ntotal += (nparts + nbitmaps);
        });
    };

    auto p_batch_size = std::lround(std::ceil(items_to_remove.data_parts.size() * 1.0 / pool_size));
    auto d_batch_size = std::lround(std::ceil(items_to_remove.delete_bitmaps.size() * 1.0 / pool_size));
    LOG_DEBUG(
        log,
        "[p2] After filter, there are {} items, new batch size for parts: {}, delete bitmaps: {}, start_key: `{}`",
        items_to_remove.size(),
        p_batch_size,
        d_batch_size,
        phase_two_start_key);
    for (size_t i = 0; i < pool_size; i++)
    {
        size_t p_start = std::min(p_batch_size * i, items_to_remove.data_parts.size());
        size_t p_end = std::min(p_batch_size * (i + 1), items_to_remove.data_parts.size());
        size_t d_start = std::min(d_batch_size * i, items_to_remove.delete_bitmaps.size());
        size_t d_end = std::min(d_batch_size * (i + 1), items_to_remove.delete_bitmaps.size());
        batch_remove(p_start, p_end, d_start, d_end);
    }
    remove_pool.wait();
    return ntotal;
}

std::pair<ServerDataPartsVector, ServerDataPartsVector> CnchPartGCThread::processIntermediateParts(ServerDataPartsVector & parts, TxnTimestamp gc_timestamp)
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
        LOG_WARNING(log, "[p1] Fail to get transaction records from catalog. Will skip all intermediate parts.");
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

    // now collect zombie parts to remove and put back visible intermediate parts for further processing
    // note that zombie parts with staging_txn_id don't own its data files, so we can only clean its part metadata,
    // refer to https://bytedance.larkoffice.com/docx/InHmdbM97oh1oMxEwVVcMbTinXg for more details
    ServerDataPartsVector zombie_metadata_only_parts;
    ServerDataPartsVector zombie_parts;
    for (const auto & part : intermediate_parts)
    {
        auto txn_id = part->txnID();
        // skip those invisible intermediate parts (not zombie).
        if (!transactions.contains(txn_id))
            continue;

        if (!transactions[txn_id])
        {
            if (part->hasStagingTxnID())
                zombie_metadata_only_parts.push_back(part);
            else
                zombie_parts.push_back(part);
        }
        else if (transactions[txn_id] <= gc_timestamp)
        {
            part->setCommitTime(transactions[txn_id]); /// make sure committed parts have commit time set
            parts.push_back(part);
        }
    }
    return {zombie_metadata_only_parts, zombie_parts};
}

size_t CnchPartGCThread::doPhaseOnePartitionGC(
    const StoragePtr & istorage,
    StorageCnchMergeTree & storage,
    const String & partition_id,
    bool in_wakeup,
    TxnTimestamp gc_timestamp,
    bool & items_removed)
{
    if (!gc_timestamp)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "gc_timestamp should not be zero");

    size_t items_count_in_partition = 0;
    auto storage_settings = storage.getSettings();

    Stopwatch watch;
    // for gc thread, intermediate parts are required in case that TransactionCleaner fails to clean them.
    auto all_parts
        = catalog->getServerDataPartsInPartitions(istorage, {partition_id}, gc_timestamp, nullptr, Catalog::VisibilityLevel::All);
    items_count_in_partition += all_parts.size();
    LOG_TRACE(log, "[p1] Get {} parts for {} from Catalog cost {} us", all_parts.size(), partition_id, watch.elapsedMicroseconds());
    watch.restart();

    // Get zombie parts to remove and filter out invisible intermediate parts.
    auto [zombie_metadata_only_parts, zombie_parts] = processIntermediateParts(all_parts, gc_timestamp);
    if (!zombie_metadata_only_parts.empty())
    {
        LOG_TRACE(log, "[p1] Get {} zombie metadata-only parts to remove for {} ", zombie_metadata_only_parts.size(), partition_id);
        items_removed = true;
        movePartsToTrash(
            istorage,
            zombie_metadata_only_parts,
            /*is_staged*/ false,
            "zombie metadata-only parts",
            storage_settings->gc_trash_part_thread_pool_size,
            storage_settings->gc_trash_part_batch_size,
            /*is_zombie_with_staging_txn_id*/ true);
    }
    if (!zombie_parts.empty())
    {
        LOG_TRACE(log, "[p1] Get {} zombie parts to remove for {} ", zombie_parts.size(), partition_id);
        items_removed = true;
        movePartsToTrash(
            istorage,
            zombie_parts,
            /*is_staged*/ false,
            "zombie parts",
            storage_settings->gc_trash_part_thread_pool_size,
            storage_settings->gc_trash_part_batch_size);
    }

    auto visible_parts = CnchPartsHelper::calcVisibleParts(all_parts, false);
    LOG_TRACE(log, "[p1] Calculate visible parts cost {} us, visible_parts: {}", watch.elapsedMicroseconds(), visible_parts.size());

    /// Generate DROP_RANGE for expired partitions by the TTL
    if (!visible_parts.empty())
    {
        try
        {
            tryMarkExpiredPartitions(storage, visible_parts);
        }
        catch(...)
        {
            LOG_DEBUG(log, "[p1] Failed to mark expired partitions, just skip.");
        }
    }

    UInt64 now = time(nullptr);
    UInt64 old_parts_lifetime = in_wakeup ? 0ull : static_cast<UInt64>(storage_settings->old_parts_lifetime.totalSeconds());
    auto should_move_to_trash = [now, old_parts_lifetime, gc_timestamp](TxnTimestamp end_time)
    {
        return (end_time)                   // end_time is set
            && (old_parts_lifetime == 0 || (end_time.toSecond() + old_parts_lifetime < now)) // exceed lifetime
            && (end_time < gc_timestamp);   // invisible to gc_timestamp and beyond
    };

    /// clear parts
    {
        watch.restart();
        ServerDataPartsVector parts_to_gc;
        CnchPartsHelper::calcPartsForGC(all_parts, &parts_to_gc, nullptr);
        LOG_DEBUG(
            log,
            "[p1] Get {}/{} parts to gc at {}({}), took {} us",
            parts_to_gc.size(),
            all_parts.size(),
            gc_timestamp,
            LocalDateTime(gc_timestamp.toSecond()),
            watch.elapsedMicroseconds());

        std::erase_if(parts_to_gc, [&](auto & part) {
            return !should_move_to_trash(part->getEndTime());
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
                tryLogCurrentException(log, "[p1] Fail to drop disk cache for " + storage.getStorageID().getNameForLogs());
            }
        }

        if (!parts_to_gc.empty())
        {
            items_removed = true;
            movePartsToTrash(
                istorage,
                parts_to_gc,
                /*is_staged*/ false,
                "parts",
                storage_settings->gc_trash_part_thread_pool_size,
                storage_settings->gc_trash_part_batch_size);
        }
    }

    /// clear delete bitmaps
    if (storage.getInMemoryMetadataPtr()->hasUniqueKey())
    {
        watch.restart();
        /// TODO: handle zombie intermediate bitmaps
        DeleteBitmapMetaPtrVector all_bitmaps = catalog->getDeleteBitmapsInPartitionsFromMetastore(istorage, {partition_id}, gc_timestamp);
        items_count_in_partition += all_bitmaps.size();
        DeleteBitmapMetaPtrVector bitmaps_to_gc;
        CnchPartsHelper::calcBitmapsForGC(all_bitmaps, &bitmaps_to_gc, nullptr);
        LOG_DEBUG(
            log,
            "[p1] Get {}/{} bitmaps to gc at {}({}), took {} us",
            bitmaps_to_gc.size(),
            all_bitmaps.size(),
            gc_timestamp,
            LocalDateTime(gc_timestamp.toSecond()),
            watch.elapsedMicroseconds());

        std::erase_if(bitmaps_to_gc, [&](auto & bitmap) {
            return !should_move_to_trash(bitmap->getEndTime());
        });
        if (!bitmaps_to_gc.empty())
        {
            items_removed = true;
            moveDeleteBitmapsToTrash(
                istorage,
                bitmaps_to_gc,
                storage_settings->gc_remove_bitmap_thread_pool_size,
                storage_settings->gc_remove_bitmap_batch_size);
        }
    }

    /// clear staged parts
    if (storage.getInMemoryMetadataPtr()->hasUniqueKey())
    {
        watch.restart();
        NameSet partition_filter { partition_id };
        /// not using gc_timestamp here because
        /// staged part is tmp part which is unecessary to last for old_parts_lifetime, delete it directly
        TxnTimestamp ts = storage.getContext()->getTimestamp();
        /// TODO: handle zombie staged parts
        ServerDataPartsVector all_staged_parts = catalog->getStagedServerDataParts(istorage, ts, &partition_filter);
        items_count_in_partition += all_staged_parts.size();
        ServerDataPartsVector staged_parts_to_gc;
        CnchPartsHelper::calcPartsForGC(all_staged_parts, &staged_parts_to_gc, nullptr);
        LOG_DEBUG(
            log,
            "[p1] Get {}/{} staged parts to gc at {}({}), took {} us",
            staged_parts_to_gc.size(),
            all_staged_parts.size(),
            ts,
            LocalDateTime(ts.toSecond()),
            watch.elapsedMicroseconds());

        if (size_t limit = storage_settings->gc_trash_part_limit; limit && staged_parts_to_gc.size() > limit)
            staged_parts_to_gc.resize(limit);

        if (!staged_parts_to_gc.empty())
        {
            items_removed = true;
            /// due to its temporary nature, staged parts don't actually have a trash,
            /// `Catalog::moveDataItemsToTrash` will removes its metadata directly.
            movePartsToTrash(
                istorage,
                staged_parts_to_gc,
                /*is_staged*/ true,
                "staged parts",
                storage_settings->gc_trash_part_thread_pool_size,
                storage_settings->gc_trash_part_batch_size);
        }
    }

    return items_count_in_partition;
}

void CnchPartGCThread::clearEmptyPartitions(const StoragePtr & istorage, StorageCnchMergeTree & storage, const Strings & partitions)
{
    if (partitions.empty())
        return;

    auto now = time(nullptr);

    Strings empty_partitions;
    for (const auto & p : partitions)
    {
        // double check if the partition is truely empty. Get parts here should be lightweight since most of the partitions should be empty;
        auto data_parts_in_partition
            = catalog->getServerDataPartsInPartitions(istorage, {p}, 0, nullptr, Catalog::VisibilityLevel::All);
        NameSet partition_filter{p};
        auto staged_parts_in_partition = catalog->getStagedServerDataParts(istorage, 0, &partition_filter);
        if (data_parts_in_partition.empty() && staged_parts_in_partition.empty())
            empty_partitions.push_back(p);
    }

    Catalog::PartitionWithGCStatus partitions_with_gctime = catalog->getPartitionsWithGCStatus(istorage, empty_partitions);

    if (partitions_with_gctime.empty())
        return;

    UInt64 batch_size = storage.getSettings()->gc_partition_batch_size;
    // lifetime of deleting partitions should be more than ttl for trash items since it may be still referenced by a snapshot.
    UInt64 partition_life_time = std::max(storage.getSettings()->ttl_for_trash_items.totalSeconds() * 2, storage.getSettings()->gc_partition_lifetime_before_remove.totalSeconds());
    Strings partitions_to_mark_delete;
    size_t counter_mark=0, counter_delete=0;
    Catalog::PartitionWithGCStatus partitions_to_remove;
    for (const auto & p : empty_partitions)
    {
        if (auto it = partitions_with_gctime.find(p); it!=partitions_with_gctime.end())
        {
            if (it->second == 0)
            {
                counter_mark++;
                partitions_to_mark_delete.emplace_back(it->first);
            }
            else if (it->second + partition_life_time < UInt64(now))
            {
                if (!catalog->hasTrashedPartsInPartition(istorage, p))
                {
                    counter_delete++;
                    partitions_to_remove.emplace(it->first, it->second);
                }
            }
        }

        if (partitions_to_mark_delete.size() >= batch_size)
        {
            catalog->markPartitionDeleted(istorage, partitions_to_mark_delete);
            partitions_to_mark_delete.clear();
        }
        if (partitions_to_remove.size() >= batch_size)
        {
            catalog->deletePartitionsMetadata(istorage, partitions_to_remove);
            partitions_to_remove.clear();
        }
    }

    if (partitions_to_mark_delete.size())
        catalog->markPartitionDeleted(istorage, partitions_to_mark_delete);

    if (partitions_to_remove.size())
        catalog->deletePartitionsMetadata(istorage, partitions_to_remove);

    if (counter_mark || counter_delete)
        LOG_DEBUG(log, "[p1] Partition GC marked {} partitions as deleting and removed {} partitions from metastore.", counter_mark, counter_delete);
}

}
