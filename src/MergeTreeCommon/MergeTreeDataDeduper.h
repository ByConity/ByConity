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

#include <Common/Logger.h>
#include <Storages/MergeTree/DeleteBitmapMeta.h>
#include <MergeTreeCommon/ReplacingSortedKeysIterator.h>
#include <Transaction/TxnTimestamp.h>
#include <common/logger_useful.h>
#include <CloudServices/CnchDedupHelper.h>

namespace DB
{
class Context;
class MergeTreeMetaBase;

class MergeTreeDataDeduper
{
public:
    using DedupTaskProgressReporter = std::function<String()>;
    using VersionMode = ReplacingSortedKeysIterator::VersionMode;
    using DeleteInfoPtr = ReplacingSortedKeysIterator::DeleteInfoPtr;
    using RowPos = ReplacingSortedKeysIterator::RowPos;
    using DeleteCallback = ReplacingSortedKeysIterator::DeleteCallback;

    MergeTreeDataDeduper(
        const MergeTreeMetaBase & data_,
        ContextPtr context_,
        const CnchDedupHelper::DedupMode & dedup_mode_);

    /// Remove duplicate keys among visible, staged, and uncommitted parts.
    /// Assumes that
    /// 1. All parts themselves don't contain duplicate keys
    /// 2. There is no duplicate keys among visible parts with the help of delete bitmaps
    /// Returns all new delete bitmaps to dump in order to remove duplicated keys.
    /// Visible part may or may not have bitmap to dump, but every staged and uncommitted part
    /// should have one bitmap to dump.
    LocalDeleteBitmaps dedupParts(
        TxnTimestamp txn_id,
        const IMergeTreeDataPartsVector & visible_parts,
        const IMergeTreeDataPartsVector & staged_parts,
        const IMergeTreeDataPartsVector & uncommitted_parts = {});

    LocalDeleteBitmaps repairParts(TxnTimestamp txn_id, IMergeTreeDataPartsVector visible_parts);

    Names getTasksProgress();

    bool isCancelled() { return deduper_cancelled.load(std::memory_order_relaxed); }

    void setCancelled() { deduper_cancelled = true; }

    void setDeduperContext(ContextPtr deduper_context) { context = deduper_context; }

    struct DedupTask
    {
        /// Record txn_id in DedupTask for LOG
        TxnTimestamp txn_id;

        String partition_id;
        bool bucket_valid;
        Int64 bucket_number;
        IMergeTreeDataPartsVector visible_parts;
        IMergeTreeDataPartsVector new_parts;
        UInt64 total_dedup_row_num = 0;
        /// Iterator of new_parts' keys
        std::shared_ptr<ReplacingSortedKeysIterator> iter;

        DedupTask(
            TxnTimestamp txn_id_,
            String partition_id_,
            bool bucket_valid_,
            Int64 bucket_number_,
            const IMergeTreeDataPartsVector & visible_parts_,
            const IMergeTreeDataPartsVector & new_parts_);

        String getDedupLevelInfo() const;

        /// DedupLevelInfo + [ iter visited row num / total nums of new_parts ]
        String getDedupTaskProgress() const;
    };
    using DedupTaskPtr = std::shared_ptr<DedupTask>;
    using DedupTasks = std::vector<DedupTaskPtr>;

private:
    /// Low-level interface to dedup `new_parts` with `visible_parts`.
    /// Return delete bitmaps of input parts to remove duplicate keys.
    /// Size of the result vector is `visible_parts.size() + new_parts.size()`.
    /// The result bitmap(could be nullptr) for visible_parts[i] is stored in res[i].
    /// The result bitmap(could be nullptr) for new_parts[j] is stored in res[visible_parts.size() + j].
    DeleteBitmapVector dedupImpl(const IMergeTreeDataPartsVector & visible_parts, const IMergeTreeDataPartsVector & new_parts, DedupTaskPtr & dedup_task);

    /// Handling the case when new parts have partial update parts.
    /// Differentiate into different sub-iterations according to the type of part(normal_parts or partial_update_parts)
    DeleteBitmapVector processDedupSubTaskInPartialUpdateMode(const IMergeTreeDataPartsVector & visible_parts, const IMergeTreeDataPartsVector & new_parts, DedupTaskPtr & dedup_task, IMergeTreeDataPartsVector & partial_update_processed_parts);

    /// For partial update mode: Restore block from new parts
    std::vector<Block> restoreBlockFromNewParts(
        const IMergeTreeDataPartsVector & current_dedup_new_parts,
        const DedupTaskPtr & dedup_task,
        bool & optimize_for_same_update_columns,
        NameSet & same_update_column_set);

    /// For partial update mode: Remove duplicate keys in block and get replace info.
    size_t removeDupKeysInPartialUpdateMode(
        Block & block,
        ColumnPtr version_column,
        IColumn::Filter & filter,
        PaddedPODArray<UInt32> & replace_dst_indexes,
        PaddedPODArray<UInt32> & replace_src_indexes);

    /// For partial update mode:
    /// Given input keys, for each key, search in the existing parts to find the duplicated key.
    /// Returns: {part_index, rowid, row version} for each key. If a key is never existed before, its part_index will be -1
    SearchKeysResult searchPartForKeys(const IMergeTreeDataPartsVector & visible_parts, UniqueKeys & keys);

    /// For partial update mode: read data from part.
    void readColumnsFromStorage(const IMergeTreeDataPartPtr & part, RowidPairs & rowid_pairs,
        Block & to_block, PaddedPODArray<UInt32> & to_block_rowids, const DedupTaskPtr & dedup_task);

    void readColumnsFromStorageParallel(const IMergeTreeDataPartPtr & part, const RowidPairs & rowid_pairs,
        Block & to_block, PaddedPODArray<UInt32> & to_block_rowids, const DedupTaskPtr & dedup_task);

    /// For partial update mode: replace data in block.
    void replaceColumnsAndFilterData(
        Block & block,
        Block & columns_from_storage,
        IColumn::Filter & filter,
        size_t & num_filtered,
        PaddedPODArray<UInt32> & block_rowids,
        PaddedPODArray<UInt32> & replace_dst_indexes,
        PaddedPODArray<UInt32> & replace_src_indexes,
        const IMergeTreeDataPartsVector & current_dedup_new_parts,
        const DedupTaskPtr & dedup_task,
        const bool & optimize_for_same_update_columns,
        const NameSet & same_update_column_set);

    /// For partial update mode: parse update columns & fill default filter
    void parseUpdateColumns(
        String columns_name,
        std::unordered_map<String, ColumnVector<UInt8>::MutablePtr> & default_filters,
        std::function<bool(String)> check_column,
        std::function<String(size_t, size_t)> get_column_by_index,
        size_t idx);

    /// For partial update mode: generate & commit part with data and unique index.
    IMergeTreeDataPartPtr generateAndCommitPartInPartialUpdateMode(
        Block & block,
        const IMergeTreeDataPartsVector & new_parts,
        const DedupTaskPtr & dedup_task);

    DeleteBitmapVector repairImpl(const IMergeTreeDataPartsVector & parts);

    void dedupKeysWithParts(
        std::shared_ptr<ReplacingSortedKeysIterator> & keys,
        const IMergeTreeDataPartsVector & parts,
        DeleteBitmapVector & delta_bitmaps,
        DedupTaskProgressReporter reporter,
        DedupTaskPtr & dedup_task);

    /// Convert dedup task into multiple sub dedup tasks. If valid_bucket_table is true, it will split dedup task into bucket granule.
    DedupTasks convertIntoSubDedupTasks(
        const IMergeTreeDataPartsVector & all_visible_parts,
        const IMergeTreeDataPartsVector & all_staged_parts,
        const IMergeTreeDataPartsVector & all_uncommitted_parts,
        const bool & bucket_level_dedup,
        TxnTimestamp txn_id);

    size_t prepareBitmapsToDump(
        const IMergeTreeDataPartsVector & visible_parts,
        const IMergeTreeDataPartsVector & new_parts,
        const DeleteBitmapVector & bitmaps,
        TxnTimestamp txn_id,
        LocalDeleteBitmaps & res);

    size_t prepareBitmapsForPartialUpdate(
        const IMergeTreeDataPartsVector & visible_parts,
        const DeleteBitmapVector & bitmaps,
        TxnTimestamp txn_id,
        LocalDeleteBitmaps & res,
        const IMergeTreeDataPartsVector & partial_update_processed_parts);

    DeleteBitmapVector generateNextIterationBitmaps(
        const IMergeTreeDataPartsVector & visible_parts,
        const IMergeTreeDataPartsVector & new_parts,
        const DeleteBitmapVector & bitmaps,
        TxnTimestamp txn_id);

    void logDedupDetail(const IMergeTreeDataPartsVector & visible_parts,
        const IMergeTreeDataPartsVector & new_parts,
        const DedupTaskPtr & task,
        bool is_sub_iteration);

    /// Used to protect dedup_tasks member change & iter change
    mutable std::mutex dedup_tasks_mutex;
    /// Sub dedup tasks after calling convertIntoSubDedupTasks
    DedupTasks dedup_tasks;
    /// Used to indicate whether deduper should be cancelled
    std::atomic<bool> deduper_cancelled{false};

    const MergeTreeMetaBase & data;
    ContextPtr context;
    LoggerPtr log;
    VersionMode version_mode;
    CnchDedupHelper::DedupMode dedup_mode;
};

}
