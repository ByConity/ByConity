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

#pragma once

#include <memory>
#include <Core/Names.h>
#include <Core/Block.h>
#include <Columns/IColumn.h>
#include <Common/PODArray.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Transaction/LockRequest.h>

namespace DB
{
class MergeTreeMetaBase;
class StorageCnchMergeTree;
class DeleteBitmapMeta;
using DeleteBitmapMetaPtr = std::shared_ptr<DeleteBitmapMeta>;
using DeleteBitmapMetaPtrVector = std::vector<DeleteBitmapMetaPtr>;
class CnchServerTransaction;
}


namespace DB::CnchDedupHelper
{

enum class DedupMode : unsigned int
{
    APPEND = 0,
    UPSERT,
    THROW,
    IGNORE
};

inline String typeToString(DedupMode type)
{
    switch (type)
    {
        case DedupMode::APPEND:
            return "APPEND";
        case DedupMode::UPSERT:
            return "UPSERT";
        case DedupMode::THROW:
            return "THROW";
        case DedupMode::IGNORE:
            return "IGNORE";
        default:
            return "Unknown";
    }
}

class DedupScope
{
public:

    enum class DedupLevel
    {
        TABLE,
        PARTITION,
    };

    enum class LockLevel
    {
        NORMAL, /// For NORMAL lock mode, if dedup mode is table, it's table level. Otherwise, it's partition level.
        BUCKET, /// BUCKET level lock mode.
    };

    using BucketSet = std::set<Int64>;
    using BucketWithPartition = std::pair<String, Int64>;
    struct BucketWithPartitionComparator
    {
        bool operator()(const BucketWithPartition & item1, const BucketWithPartition & item2) const
        {
            return std::forward_as_tuple(item1.first, item1.second) < std::forward_as_tuple(item2.first, item2.second);
        }
    };
    using BucketWithPartitionSet = std::set<BucketWithPartition, BucketWithPartitionComparator>;

    static DedupScope TableDedup()
    {
        static DedupScope table_scope{DedupLevel::TABLE};
        return table_scope;
    }

    static DedupScope TableDedupWithBucket(const BucketSet & buckets_)
    {
        DedupScope table_scope{DedupLevel::TABLE, LockLevel::BUCKET};
        table_scope.buckets = buckets_;
        return table_scope;
    }

    static DedupScope PartitionDedup(const NameOrderedSet & partitions_)
    {
        DedupScope partition_scope{DedupLevel::PARTITION};
        partition_scope.partitions = partitions_;
        return partition_scope;
    }

    static DedupScope PartitionDedupWithBucket(const BucketWithPartitionSet & bucket_with_partition_set_)
    {
        DedupScope partition_scope{DedupLevel::PARTITION, LockLevel::BUCKET};
        partition_scope.bucket_with_partition_set = bucket_with_partition_set_;
        for (const auto & bucket_with_partition : partition_scope.bucket_with_partition_set)
            partition_scope.partitions.insert(bucket_with_partition.first);
        return partition_scope;
    }

    bool isTableDedup() const { return dedup_level == DedupLevel::TABLE; }
    bool isBucketLock() const { return lock_level == LockLevel::BUCKET; }

    const NameOrderedSet & getPartitions() const { return partitions; }

    const BucketSet & getBuckets() const { return buckets; }

    const BucketWithPartitionSet & getBucketWithPartitionSet() const { return bucket_with_partition_set; }

    /// Filter parts if lock scope is bucket level
    void filterParts(MergeTreeDataPartsCNCHVector & parts) const;

private:
    DedupScope(DedupLevel dedup_level_, LockLevel lock_level_ = LockLevel::NORMAL) : dedup_level(dedup_level_), lock_level(lock_level_) { }

    DedupLevel dedup_level;
    LockLevel lock_level;

    NameOrderedSet partitions;
    BucketSet buckets;
    BucketWithPartitionSet bucket_with_partition_set;
};

std::vector<LockInfoPtr>
getLocksToAcquire(const DedupScope & scope, TxnTimestamp txn_id, const MergeTreeMetaBase & storage, UInt64 timeout_ms);

MergeTreeDataPartsCNCHVector getStagedPartsToDedup(const DedupScope & scope, StorageCnchMergeTree & cnch_table, TxnTimestamp ts);

MergeTreeDataPartsCNCHVector
getVisiblePartsToDedup(const DedupScope & scope, StorageCnchMergeTree & cnch_table, TxnTimestamp ts, bool force_bitmap = true);

struct FilterInfo
{
    IColumn::Filter filter;
    size_t num_filtered{0};
};

Block filterBlock(const Block & block, const FilterInfo & filter_info);

CnchDedupHelper::DedupScope
getDedupScope(MergeTreeMetaBase & storage, IMergeTreeDataPartsVector & source_data_parts, bool force_normal_dedup = false);

CnchDedupHelper::DedupScope
getDedupScope(MergeTreeMetaBase & storage, const MutableMergeTreeDataPartsCNCHVector & preload_parts, bool force_normal_dedup = false);

/// Check whether we can use bucket level dedup, according to whether all parts is the same table definition, otherwise we need to use normal lock instead of bucket lock.
bool checkBucketParts(
    MergeTreeMetaBase & storage,
    const MergeTreeDataPartsCNCHVector & visible_parts,
    const MergeTreeDataPartsCNCHVector & staged_parts);

struct DedupTask
{
    DedupMode dedup_mode;
    StorageID storage_id;
    MutableMergeTreeDataPartsCNCHVector new_parts;
    DeleteBitmapMetaPtrVector delete_bitmaps_for_new_parts;

    MutableMergeTreeDataPartsCNCHVector staged_parts;
    DeleteBitmapMetaPtrVector delete_bitmaps_for_staged_parts;

    MutableMergeTreeDataPartsCNCHVector visible_parts;
    DeleteBitmapMetaPtrVector delete_bitmaps_for_visible_parts;

    struct Statistics
    {
        /// Record time cost for each stage(ms)
        UInt64 acquire_lock_cost = 0;
        UInt64 get_metadata_cost = 0;
        UInt64 execute_task_cost = 0;
        UInt64 other_cost = 0;
        UInt64 total_cost = 0;

        String toString()
        {
            return fmt::format(
                "[acquire lock cost {} ms, get metadata cost {} ms, execute task cost {} ms, other cost {} ms, total cost {} ms]",
                acquire_lock_cost,
                get_metadata_cost,
                execute_task_cost,
                other_cost,
                total_cost);
        }
    } statistics;

    explicit DedupTask(const DedupMode & dedup_mode_, const StorageID & storage_id_) : dedup_mode(dedup_mode_), storage_id(storage_id_) { }
};
using DedupTaskPtr = std::shared_ptr<DedupTask>;

UInt64 getWriteLockTimeout(StorageCnchMergeTree & cnch_table, ContextPtr local_context);

void acquireLockAndFillDedupTask(StorageCnchMergeTree & cnch_table, DedupTask & dedup_task, CnchServerTransaction & txn, ContextPtr local_context);

void executeDedupTask(StorageCnchMergeTree & cnch_table, DedupTask & dedup_task, const TxnTimestamp & txn_id, ContextPtr local_context);

}
