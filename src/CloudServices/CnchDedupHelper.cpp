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

#include <Catalog/Catalog.h>
#include <CloudServices/CnchDedupHelper.h>
#include <CloudServices/CnchServerClient.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Core/UUID.h>
#include <Interpreters/CnchSystemLog.h>
#include <MergeTreeCommon/MergeTreeDataDeduper.h>
#include <CloudServices/CnchDataWriter.h>
#include <WorkerTasks/ManipulationType.h>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int ABORTED;
extern const int CNCH_LOCK_ACQUIRE_FAILED;
}

namespace DB::CnchDedupHelper
{

static void checkDedupScope(const DedupScope & scope, const MergeTreeMetaBase & storage)
{
    if (!storage.getSettings()->partition_level_unique_keys && !scope.isTableDedup())
        throw Exception("Expect TABLE scope for table level uniqueness", ErrorCodes::LOGICAL_ERROR);
}

std::vector<LockInfoPtr>
getLocksToAcquire(const DedupScope & scope, TxnTimestamp txn_id, const MergeTreeMetaBase & storage, UInt64 timeout_ms)
{
    checkDedupScope(scope, storage);

    std::vector<LockInfoPtr> res;
    if (scope.isTableDedup())
    {
        if (scope.isBucketLock())
        {
            for (const auto & bucket : scope.getBuckets())
            {
                auto lock_info = std::make_shared<LockInfo>(txn_id);
                lock_info->setMode(LockMode::X);
                lock_info->setTimeout(timeout_ms);
                /// TODO: (lta, zuochuang.zema) use a separated prefix.
                lock_info->setUUIDAndPrefix(storage.getCnchStorageUUID());
                lock_info->setBucket(bucket);
                res.push_back(std::move(lock_info));
            }
        }
        else
        {
            auto lock_info = std::make_shared<LockInfo>(txn_id);
            lock_info->setMode(LockMode::X);
            lock_info->setTimeout(timeout_ms);
            lock_info->setUUIDAndPrefix(storage.getCnchStorageUUID());
            res.push_back(std::move(lock_info));
        }
    }
    else
    {
        if (scope.isBucketLock())
        {
            for (const auto & bucket_with_partition : scope.getBucketWithPartitionSet())
            {
                auto lock_info = std::make_shared<LockInfo>(txn_id);
                lock_info->setMode(LockMode::X);
                lock_info->setTimeout(timeout_ms);
                lock_info->setUUIDAndPrefix(storage.getCnchStorageUUID());
                lock_info->setPartition(bucket_with_partition.first);
                lock_info->setBucket(bucket_with_partition.second);
                res.push_back(std::move(lock_info));
            }
        }
        else
        {
            for (const auto & partition : scope.getPartitions())
            {
                auto lock_info = std::make_shared<LockInfo>(txn_id);
                lock_info->setMode(LockMode::X);
                lock_info->setTimeout(timeout_ms);
                lock_info->setUUIDAndPrefix(storage.getCnchStorageUUID());
                lock_info->setPartition(partition);
                res.push_back(std::move(lock_info));
            }
        }
    }
    return res;
}

MergeTreeDataPartsCNCHVector getStagedPartsToDedup(const DedupScope & scope, StorageCnchMergeTree & cnch_table, TxnTimestamp ts)
{
    checkDedupScope(scope, cnch_table);

    MergeTreeDataPartsCNCHVector staged_parts;
    if (!scope.isTableDedup())
    {
        const auto & partitions = scope.getPartitions();
        NameSet partitions_filter {partitions.begin(), partitions.end()};
        return cnch_table.getStagedParts(ts, &partitions_filter);
    }
    else
    {
        /// For table-level unique key, row may be updated from one partition to another,
        /// therefore we must dedup with staged parts from all partitions to ensure correctness.
        /// Example: foo(p String, k Int32) partition by p unique key k
        ///  Time   Action
        ///   t1    insert into foo ('P1', 1);
        ///   t2    dedup worker found staged part in P1 and waited for lock
        ///   t3    insert into foo ('P2', 1);
        ///   t4    insert into foo ('P1', 1);
        ///   t5    dedup worker acquired lock and began to dedup
        /// If dedup worker only processes staged parts from P1 at t5, the final data of foo would be ('P2', 1), which is wrong.
        return cnch_table.getStagedParts(ts);
    }
}

MergeTreeDataPartsCNCHVector getVisiblePartsToDedup(const DedupScope & scope, StorageCnchMergeTree & cnch_table, TxnTimestamp ts, bool force_bitmap)
{
    checkDedupScope(scope, cnch_table);

    if (!scope.isTableDedup())
    {
        const auto & partitions = scope.getPartitions();
        Names partitions_filter {partitions.begin(), partitions.end()};
        std::set<Int64> bucket_numbers;
        if (scope.isBucketLock())
        {
            for (const auto & bucket_with_partition : scope.getBucketWithPartitionSet())
                bucket_numbers.insert(bucket_with_partition.second);
        }
        return cnch_table.getUniqueTableMeta(ts, partitions_filter, force_bitmap, bucket_numbers);
    }
    else
    {
        return cnch_table.getUniqueTableMeta(ts, {}, force_bitmap, (scope.isBucketLock() ? scope.getBuckets() : std::set<Int64>()));
    }
}

Block filterBlock(const Block & block, const FilterInfo & filter_info)
{
    if (filter_info.num_filtered == 0)
        return block;

    Block res = block.cloneEmpty();
    ssize_t new_size_hint = res.rows() - filter_info.num_filtered;
    for (size_t i = 0; i < res.columns(); ++i)
    {
        ColumnWithTypeAndName & dst_col = res.getByPosition(i);
        const ColumnWithTypeAndName & src_col = block.getByPosition(i);
        dst_col.column = src_col.column->filter(filter_info.filter, new_size_hint);
    }
    return res;
}

CnchDedupHelper::DedupScope
getDedupScope(MergeTreeMetaBase & storage, IMergeTreeDataPartsVector & data_parts, bool force_normal_dedup)
{
    MutableMergeTreeDataPartsCNCHVector cnch_parts;
    cnch_parts.reserve(data_parts.size());
    for (auto & part : data_parts)
        cnch_parts.emplace_back(const_pointer_cast<MergeTreeDataPartCNCH>(dynamic_pointer_cast<const MergeTreeDataPartCNCH>(part)));
    return getDedupScope(storage, cnch_parts, force_normal_dedup);
}

CnchDedupHelper::DedupScope
getDedupScope(MergeTreeMetaBase & storage, const MutableMergeTreeDataPartsCNCHVector & preload_parts, bool force_normal_dedup)
{
    auto settings = storage.getSettings();
    auto checkIfUseBucketLock = [&]() -> bool {
        if (force_normal_dedup)
            return false;
        if (storage.isBucketTable() && settings->enable_bucket_level_unique_keys)
            return true;

        /// If it's partition/table level dedup, we will convert into bucket level dedup only when cluster by key is same with unique key and table definition of all parts are same.
        if (!storage.getInMemoryMetadataPtr()->checkIfClusterByKeySameWithUniqueKey())
            return false;
        auto table_definition_hash = storage.getTableHashForClusterBy();
        /// Check whether all parts has same table_definition_hash.
        auto it = std::find_if(preload_parts.begin(), preload_parts.end(), [&](const auto & part) {
            return part->bucket_number == -1 || !table_definition_hash.match(part->table_definition_hash);
        });
        return it == preload_parts.end();
    };

    if (checkIfUseBucketLock())
    {
        if (settings->partition_level_unique_keys)
        {
            CnchDedupHelper::DedupScope::BucketWithPartitionSet bucket_with_partition_set;
            for (const auto & part : preload_parts)
                bucket_with_partition_set.insert({part->info.partition_id, part->bucket_number});
            return CnchDedupHelper::DedupScope::PartitionDedupWithBucket(bucket_with_partition_set);
        }
        else
        {
            CnchDedupHelper::DedupScope::BucketSet buckets;
            for (const auto & part : preload_parts)
                buckets.insert(part->bucket_number);
            return CnchDedupHelper::DedupScope::TableDedupWithBucket(buckets);
        }
    }
    else
    {
        /// acquire locks for all the written partitions
        NameOrderedSet sorted_partitions;
        for (const auto & part : preload_parts)
            sorted_partitions.insert(part->info.partition_id);

        return settings->partition_level_unique_keys ? CnchDedupHelper::DedupScope::PartitionDedup(sorted_partitions)
                                                            : CnchDedupHelper::DedupScope::TableDedup();
    }
}

bool checkBucketParts(
    MergeTreeMetaBase & storage,
    const MergeTreeDataPartsCNCHVector & visible_parts,
    const MergeTreeDataPartsCNCHVector & staged_parts)
{
    auto settings = storage.getSettings();
    /// If use bucket level dedup directly, just return true
    if (settings->enable_bucket_level_unique_keys)
        return true;

    /// If use partition/table level dedup, we can convert to bucket level dedup only when cluster by key is same with unique key and table definition of all parts are same.
    if (!storage.getInMemoryMetadataPtr()->checkIfClusterByKeySameWithUniqueKey())
        return false;
    auto table_definition_hash = storage.getTableHashForClusterBy();
    auto checkIfBucketPartValid = [&table_definition_hash](const MergeTreeDataPartsCNCHVector & parts) -> bool {
        auto it = std::find_if(parts.begin(), parts.end(), [&](const auto & part) {
            return part->bucket_number == -1 || !table_definition_hash.match(part->table_definition_hash);
        });
        return it == parts.end();
    };
    return checkIfBucketPartValid(visible_parts) && checkIfBucketPartValid(staged_parts);
}

void DedupScope::filterParts(MergeTreeDataPartsCNCHVector & parts) const
{
    if (!isBucketLock())
        return;
    parts.erase(
        std::remove_if(
            parts.begin(),
            parts.end(),
            [&](const MergeTreeDataPartCNCHPtr & part) {
                if (isTableDedup())
                    return !buckets.count(part->bucket_number);
                else
                    return !bucket_with_partition_set.count({part->info.partition_id, part->bucket_number});
            }),
        parts.end());
}

UInt64 getWriteLockTimeout(StorageCnchMergeTree & cnch_table, ContextPtr local_context)
{
    UInt64 session_value = local_context->getSettingsRef().unique_acquire_write_lock_timeout.value.totalMilliseconds();
    return session_value == 0 ? cnch_table.getSettings()->unique_acquire_write_lock_timeout.value.totalMilliseconds() : session_value;
}

void acquireLockAndFillDedupTask(StorageCnchMergeTree & cnch_table, DedupTask & dedup_task, CnchServerTransaction & txn, ContextPtr local_context)
{
    /// Note: when txn is launched by worker, local_context is global context which means session settings will not take effect. TBD: support later.
    TxnTimestamp ts;
    std::sort(dedup_task.new_parts.begin(), dedup_task.new_parts.end(), [](auto & lhs, auto & rhs) { return lhs->info < rhs->info; });
    std::sort(dedup_task.delete_bitmaps_for_new_parts.begin(), dedup_task.delete_bitmaps_for_new_parts.end(), LessDeleteBitmapMeta());
    CnchLockHolderPtr cnch_lock;
    MergeTreeDataPartsCNCHVector visible_parts, staged_parts;
    bool force_normal_dedup = false;
    Stopwatch watch;
    do
    {
        CnchDedupHelper::DedupScope scope = CnchDedupHelper::getDedupScope(cnch_table, dedup_task.new_parts, force_normal_dedup);

        std::vector<LockInfoPtr> locks_to_acquire = CnchDedupHelper::getLocksToAcquire(
            scope, txn.getTransactionID(), cnch_table, CnchDedupHelper::getWriteLockTimeout(cnch_table, local_context));
        watch.restart();
        cnch_lock = std::make_shared<CnchLockHolder>(local_context, std::move(locks_to_acquire));
        if (!cnch_lock->tryLock())
        {
            if (auto unique_table_log = local_context->getCloudUniqueTableLog())
            {
                auto current_log = UniqueTable::createUniqueTableLog(UniqueTableLogElement::ERROR, cnch_table.getCnchStorageID());
                current_log.txn_id = txn.getTransactionID();
                current_log.metric = ErrorCodes::CNCH_LOCK_ACQUIRE_FAILED;
                current_log.event_msg = "Failed to acquire lock for txn " + txn.getTransactionID().toString();
                unique_table_log->add(current_log);
            }
            throw Exception("Failed to acquire lock for txn " + txn.getTransactionID().toString(), ErrorCodes::CNCH_LOCK_ACQUIRE_FAILED);
        }
        dedup_task.statistics.acquire_lock_cost += watch.elapsedMilliseconds();

        watch.restart();
        ts = local_context->getTimestamp(); /// must get a new ts after locks are acquired
        visible_parts = CnchDedupHelper::getVisiblePartsToDedup(scope, cnch_table, ts);
        staged_parts = CnchDedupHelper::getStagedPartsToDedup(scope, cnch_table, ts);
        dedup_task.statistics.get_metadata_cost += watch.elapsedMilliseconds();

        /// In some case, visible parts or staged parts doesn't have same bucket definition or not a bucket part, we need to convert bucket lock to normal lock.
        /// Otherwise, it may lead to duplicated data.
        if (scope.isBucketLock() && !cnch_table.getSettings()->enable_bucket_level_unique_keys
            && !CnchDedupHelper::checkBucketParts(cnch_table, visible_parts, staged_parts))
        {
            force_normal_dedup = true;
            cnch_lock->unlock();
            LOG_TRACE(txn.getLogger(), "Check bucket parts failed, switch to normal lock to dedup.");
            continue;
        }
        else
        {
            /// Filter staged parts if lock scope is bucket level
            scope.filterParts(staged_parts);
            break;
        }
    } while (true);

    if (unlikely(local_context->getSettingsRef().unique_sleep_seconds_after_acquire_lock.totalSeconds()))
    {
        /// Test purpose only
        std::this_thread::sleep_for(std::chrono::seconds(local_context->getSettingsRef().unique_sleep_seconds_after_acquire_lock.totalSeconds()));
    }

    for (auto & visible_part: visible_parts)
    {
        dedup_task.visible_parts.emplace_back(std::const_pointer_cast<MergeTreeDataPartCNCH>(visible_part));
        for (const auto & bitmap_model : visible_part->delete_bitmap_metas)
            dedup_task.delete_bitmaps_for_visible_parts.emplace_back(createFromModel(cnch_table, *bitmap_model));
    }
    for (auto & staged_part: staged_parts)
    {
        dedup_task.staged_parts.emplace_back(std::const_pointer_cast<MergeTreeDataPartCNCH>(staged_part));
        for (const auto & bitmap_model: staged_part->delete_bitmap_metas)
            dedup_task.delete_bitmaps_for_staged_parts.emplace_back(createFromModel(cnch_table, *bitmap_model));
    }
    txn.appendLockHolder(cnch_lock);
}

void executeDedupTask(StorageCnchMergeTree & cnch_table, DedupTask & dedup_task, const TxnTimestamp & txn_id, ContextPtr local_context)
{
    /// Precondition: parts already be sorted.
    cnch_table.getDeleteBitmapMetaForCnchParts(dedup_task.visible_parts, dedup_task.delete_bitmaps_for_visible_parts, /*force_found=*/true);
    cnch_table.getDeleteBitmapMetaForCnchParts(dedup_task.new_parts, dedup_task.delete_bitmaps_for_new_parts, /*force_found=*/false);
    cnch_table.getDeleteBitmapMetaForCnchParts(dedup_task.staged_parts, dedup_task.delete_bitmaps_for_staged_parts, /*force_found=*/false);
    MergeTreeDataDeduper deduper(cnch_table, local_context, dedup_task.dedup_mode);
    LocalDeleteBitmaps bitmaps_to_dump = deduper.dedupParts(
        txn_id,
        {dedup_task.visible_parts.begin(), dedup_task.visible_parts.end()},
        {dedup_task.staged_parts.begin(), dedup_task.staged_parts.end()},
        {dedup_task.new_parts.begin(), dedup_task.new_parts.end()});

    Stopwatch watch;
    CnchDataWriter cnch_writer(cnch_table, local_context, ManipulationType::Insert);
    cnch_writer.publishStagedParts({dedup_task.staged_parts.begin(), dedup_task.staged_parts.end()}, bitmaps_to_dump);
    LOG_DEBUG(
        cnch_table.getLogger(),
        "Publish staged parts take {} ms, txn id: {}, dedup mode: {}",
        watch.elapsedMilliseconds(),
        txn_id.toUInt64(),
        typeToString(dedup_task.dedup_mode));
}
}
