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

    /// Used to protect dedup_tasks member change & iter change
    mutable std::mutex dedup_tasks_mutex;
    /// Sub dedup tasks after calling convertIntoSubDedupTasks
    DedupTasks dedup_tasks;
    /// Used to indicate whether deduper should be cancelled
    std::atomic<bool> deduper_cancelled{false};

    const MergeTreeMetaBase & data;
    ContextPtr context;
    Poco::Logger * log;
    VersionMode version_mode;
    CnchDedupHelper::DedupMode dedup_mode;
};

}
