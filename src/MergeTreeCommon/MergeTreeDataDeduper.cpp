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

#include <unordered_map>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/CnchSystemLog.h>
#include <MergeTreeCommon/MergeTreeDataDeduper.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Storages/IndexFile/IndexFileMergeIterator.h>
#include <Storages/UniqueKeyIndex.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Processors/Executors/PipelineExecutingBlockInputStream.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/Coding.h>
#include <Storages/MergeTree/MergeTreeThreadSelectBlockInputProcessor.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Core/Names.h>
#include <Core/Block.h>
#include <CloudServices/CnchDedupHelper.h>
#include <CloudServices/CnchDataWriter.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CORRUPTED_DATA;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int UNIQUE_TABLE_DUPLICATE_KEY_FOUND;
}

using IndexFileIteratorPtr = std::unique_ptr<IndexFile::Iterator>;
using IndexFileIterators = std::vector<IndexFileIteratorPtr>;

MergeTreeDataDeduper::MergeTreeDataDeduper(
    const MergeTreeMetaBase & data_, ContextPtr context_, const CnchDedupHelper::DedupMode & dedup_mode_)
    : data(data_), context(context_), log(getLogger(data_.getLogName() + " (Deduper)")), dedup_mode(dedup_mode_)
{
    if (data.merging_params.hasExplicitVersionColumn())
        version_mode = VersionMode::ExplicitVersion;
    else if (data.merging_params.partition_value_as_version)
        version_mode = VersionMode::PartitionValueAsVersion;
    else
        version_mode = VersionMode::NoVersion;

    if (dedup_mode == CnchDedupHelper::DedupMode::APPEND)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Dedup mode in dedup process is APPEND for table {}, it's a bug!",
            data.getCnchStorageID().getNameForLogs());
}

namespace
{
    using DeleteBitmapGetter = std::function<ImmutableDeleteBitmapPtr(const IMergeTreeDataPartPtr &)>;

    IndexFileIterators openUniqueKeyIndexIterators(
        const IMergeTreeDataPartsVector & parts,
        std::vector<UniqueKeyIndexPtr> & index_holders,
        bool fill_cache,
        DeleteBitmapGetter delete_bitmap_getter)
    {
        index_holders.clear();
        index_holders.reserve(parts.size());
        for (const auto & part : parts)
            index_holders.push_back(part->getUniqueKeyIndex());

        IndexFileIterators iters;
        iters.reserve(parts.size());
        for (size_t i = 0; i < parts.size(); ++i)
        {
            if (parts[i]->deleted)
            {
                iters.push_back(std::unique_ptr<IndexFile::Iterator>(IndexFile::NewEmptyIterator()));
                continue;
            }
            IndexFile::ReadOptions opts;
            opts.fill_cache = fill_cache;
            {
                ImmutableDeleteBitmapPtr delete_bitmap = delete_bitmap_getter(parts[i]);
                if (delete_bitmap && !delete_bitmap->isEmpty())
                {
                    opts.select_predicate = [bitmap = std::move(delete_bitmap)](const Slice &, const Slice & val) {
                        UInt32 rowid;
                        Slice input = val;
                        /// TODO: handle corrupt data in a better way.
                        /// E.g., make select_predicate return Status in order to propogate the error to the client
                        bool deleted = ReplacingSortedKeysIterator::DecodeRowid(input, rowid) && bitmap->contains(rowid);
                        return !deleted;
                    };
                }
            }
            iters.push_back(index_holders[i]->new_iterator(opts));
        }
        return iters;
    }

    void addRowIdToBitmap(DeleteBitmapPtr & bitmap, UInt32 rowid)
    {
        if (bitmap)
        {
            bitmap->add(rowid);
        }
        else
        {
            bitmap = std::make_shared<Roaring>();
            bitmap->add(rowid);
        }
    }

    PaddedPODArray<UInt32> getRowidPermutation(const PaddedPODArray<UInt32> & xs)
    {
        PaddedPODArray<UInt32> perm;
        perm.resize(xs.size());
        for (UInt32 i = 0; i < xs.size(); ++i)
            perm[i] = i;
        std::sort(perm.begin(), perm.end(), [&xs](UInt32 lhs, UInt32 rhs) { return xs[lhs] < xs[rhs]; });
        return perm;
    }

    PaddedPODArray<UInt32> permuteRowids(const PaddedPODArray<UInt32> & rowids, const PaddedPODArray<UInt32> & perm)
    {
        PaddedPODArray<UInt32> res;
        res.resize(rowids.size());
        for (size_t i = 0; i < res.size(); ++i)
        {
            if (perm[i] >= rowids.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Row id {} is out of size {}", perm[i], rowids.size());
            res[i] = rowids[perm[i]];
        }
        return res;
    }

    /**
    * Permutate the indexes in correct order, the rules of order are as follow:
    * 1. Sort by target index ascending.
    * 2. In the case that target indexes are same, sort by source index descending because the index is larger, the data is newer.
    * For example, replace_dst_indexes is {5, 5, 4, 5}, replace_src_indexes is {0, 2, 1, 3}
    * After correct permutation, replace_dst_indexes is {4, 5, 5, 5}, replace_src_indexes is {1, 3, 2, 0}
    */
    void handleCorrectPermutation(PaddedPODArray<UInt32> & replace_dst_indexes, PaddedPODArray<UInt32> & replace_src_indexes)
    {
        PaddedPODArray<UInt32> perm;
        perm.resize(replace_dst_indexes.size());
        for (UInt32 i = 0; i < replace_dst_indexes.size(); ++i)
            perm[i] = i;
        std::sort(perm.begin(), perm.end(), [&](UInt32 lhs, UInt32 rhs) {
            if (replace_dst_indexes[lhs] != replace_dst_indexes[rhs])
                return replace_dst_indexes[lhs] < replace_dst_indexes[rhs];
            else
                return replace_src_indexes[lhs] > replace_src_indexes[rhs];
        });
        replace_dst_indexes = permuteRowids(replace_dst_indexes, perm);
        replace_src_indexes = permuteRowids(replace_src_indexes, perm);
    }

    void mergeIndices(PaddedPODArray<UInt32> & rowids, PaddedPODArray<UInt32> & tmpids)
    {
        size_t rowids_size = rowids.size();
        size_t tmpids_size = rowids.size();
        rowids.reserve(rowids_size + tmpids_size);
        for (auto val: tmpids)
            rowids.push_back(val);
    }

    bool filterIsAlwaysTrue(const IColumn::Filter & filter)
    {
        /// TODO: improve by SIMD
        for (size_t i = 0, size = filter.size(); i < size; ++i)
            if (filter[i] == 0)
                return false;
        return true;
    }

    bool filterIsAlwaysFalse(const IColumn::Filter & filter)
    {
        /// TODO: improve by SIMD
        for (size_t i = 0, size = filter.size(); i < size; ++i)
            if (filter[i] == 1)
                return false;
        return true;
    }

    ColumnPtr loadFuncColumn(IMergeTreeDataPartPtr part, String column_name)
    {
        if (column_name == StorageInMemoryMetadata::DELETE_FLAG_COLUMN_NAME)
            return dynamic_pointer_cast<const MergeTreeDataPartCNCH>(part)->loadDeleteFlag();
        else if (column_name == StorageInMemoryMetadata::UPDATE_COLUMNS)
            return dynamic_pointer_cast<const MergeTreeDataPartCNCH>(part)->loadUpdateColumns();
        else if (column_name == StorageInMemoryMetadata::DEDUP_SORT_COLUMN)
            return dynamic_pointer_cast<const MergeTreeDataPartCNCH>(part)->loadDedupSort();
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown column name: {}", column_name);
    }

    bool hasSameUpdateColumns(const std::vector<NameSet> & update_column_set_list)
    {
        if (update_column_set_list.size() <= 1)
            return true;

        const auto & first_read_column_set = update_column_set_list[0];
        for (size_t i = 1; i < update_column_set_list.size(); ++i)
        {
            if (first_read_column_set.size() != update_column_set_list[i].size())
                return false;
            for (const auto & read_column : first_read_column_set)
            {
                if (!update_column_set_list[i].contains(read_column))
                    return false;
            }
        }
        return true;
    }

    Block getReadBlockForHistoryData(const MergeTreeMetaBase & data, const bool & optimize_for_same_update_columns, const NameSet & same_update_column_set)
    {
        Block res = data.getInMemoryMetadataPtr()->getSampleBlock();
        if (optimize_for_same_update_columns)
        {
            Names columns_from_metadata = res.getNames();
            NameSet columns_of_replace_if_not_null = data.getInMemoryMetadataPtr()->getColumns().getReplaceIfNotNullColumns();
            for (const auto & col_name : columns_from_metadata)
            {
                if (!same_update_column_set.contains(col_name))
                    continue;

                /// For map type, we still need to read origin value even if it's in update_columns when partial_update_enable_merge_map is true
                bool need_read_for_map_column = data.getInMemoryMetadataPtr()->getColumns().getPhysical(col_name).type->isMap() && data.getSettings()->partial_update_enable_merge_map;
                /// For nullable type, we still need to read origin value when replace_if_not_null is true
                bool need_read_for_nullable_column = data.getInMemoryMetadataPtr()->getColumns().getPhysical(col_name).type->isNullable() &&
                    (columns_of_replace_if_not_null.count(col_name) || data.getSettings()->partial_update_replace_if_not_null);

                if (!need_read_for_map_column && !need_read_for_nullable_column)
                    res.erase(col_name);
            }
        }
        return res;
    }

    void dumpBlockForLogging(const Block & block, String block_stage, LoggerPtr log)
    {
        LOG_DEBUG(log, "{}: {}",  block_stage, block.dumpStructure());
        for (size_t row_num = 0; row_num < block.rows(); row_num++)
        {
            for (size_t col_num = 0; col_num < block.columns(); ++col_num)
            {
                ColumnPtr col = block.getByPosition(col_num).column;
                auto value = (*col)[row_num];
                LOG_DEBUG(log, "Block [row, col]: [{}, {}], field: {}", row_num, col_num, value.dump());
            }
        }
    }
} /// namespace

void MergeTreeDataDeduper::dedupKeysWithParts(
    std::shared_ptr<ReplacingSortedKeysIterator> & keys,
    const IMergeTreeDataPartsVector & parts,
    DeleteBitmapVector & delta_bitmaps,
    DedupTaskProgressReporter reporter,
    DedupTaskPtr & dedup_task)
{
    const IndexFile::Comparator * comparator = IndexFile::BytewiseComparator();

    std::vector<UniqueKeyIndexPtr> key_indices;
    DeleteBitmapGetter delete_bitmap_getter = [](const IMergeTreeDataPartPtr & part) { return part->getDeleteBitmap(); };
    IndexFileIterators base_input_iters = openUniqueKeyIndexIterators(parts, key_indices, /*fill_cache*/ true, delete_bitmap_getter);

    std::vector<UInt64> base_implicit_versions(parts.size(), 0);
    if (version_mode == VersionMode::PartitionValueAsVersion)
    {
        for (size_t i = 0; i < parts.size(); ++i)
            base_implicit_versions[i] = parts[i]->getVersionFromPartition();
    }

    IndexFile::IndexFileMergeIterator base_iter(comparator, std::move(base_input_iters));
    keys->SeekToFirst();
    if (keys->Valid())
        base_iter.Seek(keys->CurrentKey());

    /**
     * check have time complexity limit: O(m)
     * see more dedup time complexity in unique table Tech Share
     */
    bool found_duplicate = false;
    auto check_duplicate_key = [&] () {
        if (!data.getSettings()->enable_duplicate_check_while_writing || found_duplicate)
            return;

        auto potential_duplicate_part_index = base_iter.child_index();
        auto duplicate_part_index = base_iter.checkDuplicateForKey(keys->CurrentKey());
        if (duplicate_part_index != -1)
        {
            found_duplicate = true;
            LOG_TRACE(log, "found duplication in txn {}, dedup task info {}", dedup_task->txn_id, dedup_task->getDedupLevelInfo());
            if (auto unique_table_log = context->getCloudUniqueTableLog())
            {
                LOG_TRACE(log, "try to add cnch_unique_table log(found duplication) in txn {}, dedup task info {}", dedup_task->txn_id, dedup_task->getDedupLevelInfo());
                auto current_log = UniqueTable::createUniqueTableLog(UniqueTableLogElement::ERROR, data.getCnchStorageID());
                current_log.txn_id = dedup_task->txn_id;
                current_log.metric = ErrorCodes::UNIQUE_TABLE_DUPLICATE_KEY_FOUND;
                current_log.event_msg = "Dedup task " + dedup_task->getDedupLevelInfo() + " found duplication";
                current_log.event_info = "Duplicate key: " + UniqueTable::formatUniqueKey(keys->CurrentKey(), data.getInMemoryMetadataPtr()) +
                    ", part names: [" + parts[potential_duplicate_part_index]->get_name() + ", " + parts[duplicate_part_index]->get_name() + "]";
                TaskInfo log_entry_task_info;
                log_entry_task_info.task_type = TaskInfo::DEDUP_TASK;
                log_entry_task_info.dedup_gran.partition_id = dedup_task->partition_id.empty() ? ALL_DEDUP_GRAN_PARTITION_ID : dedup_task->partition_id;
                log_entry_task_info.dedup_gran.bucket_number = dedup_task->bucket_valid ? dedup_task->bucket_number : -1;
                current_log.task_info = std::move(log_entry_task_info);
                unique_table_log->add(current_log);
            }
        }
    };

    Stopwatch watch;
    while (keys->Valid() && !isCancelled())
    {
        if (watch.elapsedSeconds() > data.getSettings()->dedup_worker_progress_log_interval.totalSeconds())
        {
            watch.restart();
            LOG_DEBUG(log, reporter());
        }

        if (data.getSettings()->disable_dedup_parts)
        {
            /// XXX: Whether block the actual dedup progress, Attention: this is only for ci test
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
            continue;
        }

        if (!keys->status().ok())
            throw Exception("Deduper new parts iterator has error " + keys->status().ToString(), ErrorCodes::INCORRECT_DATA);

        if (!base_iter.status().ok())
        {
            auto abnormal_part_index = base_iter.getAbnormalIndex();
            auto part_name = abnormal_part_index == -1 ? "Null" : parts[abnormal_part_index]->get_name();
            throw Exception("Deduper visible parts iterator has error, part name: " + part_name + ", error: " + base_iter.status().ToString(), ErrorCodes::INCORRECT_DATA);
        }

        if (!base_iter.Valid())
        {
            keys->Next();
            /// needs to read `keys` iter to the end in order to remove duplicate keys among new parts
            continue;
        }
        bool exact_match = false;
        int cmp = comparator->Compare(keys->CurrentKey(), base_iter.key());
        if (cmp < 0)
        {
            keys->Next();
            continue;
        }
        else if (cmp > 0)
        {
            base_iter.NextUntil(keys->CurrentKey(), exact_match);
        }
        else
        {
            exact_match = true;
            if (dedup_mode == CnchDedupHelper::DedupMode::THROW)
                throw Exception("Found duplication when insert with setting dedup_key_mode=DedupKeyMode::THROW", ErrorCodes::INCORRECT_DATA);
        }

        while (exact_match && !isCancelled())
        {
            RowPos lhs = ReplacingSortedKeysIterator::decodeCurrentRowPos(base_iter, version_mode, parts, base_implicit_versions);
            const RowPos & rhs = keys->CurrentRowPos();
            if (keys->IsCurrentLowPriority() || dedup_mode == CnchDedupHelper::DedupMode::IGNORE)
                addRowIdToBitmap(delta_bitmaps[rhs.child + parts.size()], rhs.rowid);
            else
            {
                if (rhs.delete_info)
                {
                    if (!rhs.delete_info->delete_version || rhs.delete_info->delete_version >= lhs.version)
                        addRowIdToBitmap(delta_bitmaps[lhs.child], lhs.rowid);
                    else if (!rhs.delete_info->just_delete_row)
                    {
                        if (lhs.version <= rhs.version)
                            addRowIdToBitmap(delta_bitmaps[lhs.child], lhs.rowid);
                        else
                            addRowIdToBitmap(delta_bitmaps[rhs.child + parts.size()], rhs.rowid);
                    }
                }
                else
                {
                    if (lhs.version <= rhs.version)
                        addRowIdToBitmap(delta_bitmaps[lhs.child], lhs.rowid);
                    else
                        addRowIdToBitmap(delta_bitmaps[rhs.child + parts.size()], rhs.rowid);
                }
            }

            /// The write-time check can only check the duplication of newly written keys in visible parts.
            /// This may miss some cases, but it will undoubtedly be of great help in troubleshooting the first scene.
            check_duplicate_key();

            exact_match = false;
            keys->Next();
            if (keys->Valid())
                base_iter.NextUntil(keys->CurrentKey(), exact_match);
        }
    }
}

MergeTreeDataDeduper::DedupTask::DedupTask(
    TxnTimestamp txn_id_,
    String partition_id_,
    bool bucket_valid_,
    Int64 bucket_number_,
    const IMergeTreeDataPartsVector & visible_parts_,
    const IMergeTreeDataPartsVector & new_parts_)
    : txn_id(txn_id_)
    , partition_id(partition_id_)
    , bucket_valid(bucket_valid_)
    , bucket_number(bucket_number_)
    , visible_parts(visible_parts_)
    , new_parts(new_parts_)
{
    for (auto & part : new_parts)
        total_dedup_row_num += part->rows_count;
}

String MergeTreeDataDeduper::DedupTask::getDedupLevelInfo() const
{
    WriteBufferFromOwnString os;
    os << (partition_id.empty() ? "table" : "partition " + partition_id) << (bucket_valid ? ("(bucket " + toString(bucket_number) + ")") : "");
    return os.str();
}

String MergeTreeDataDeduper::DedupTask::getDedupTaskProgress() const
{
    WriteBufferFromOwnString os;
    /// need to judge whether iter is null as it is later initialized
    os << getDedupLevelInfo() + "[" << (iter ? iter->getVisitedRowNum() : 0) << "/" << total_dedup_row_num << "]";
    return os.str();
}

LocalDeleteBitmaps MergeTreeDataDeduper::dedupParts(
    TxnTimestamp txn_id,
    const IMergeTreeDataPartsVector & all_visible_parts,
    const IMergeTreeDataPartsVector & all_staged_parts,
    const IMergeTreeDataPartsVector & all_uncommitted_parts)
{
    if (all_staged_parts.empty() && all_uncommitted_parts.empty())
        return {};

    LocalDeleteBitmaps res;

    Stopwatch watch;
    bool bucket_level_dedup = data.getSettings()->enable_bucket_level_unique_keys;
    {
        std::lock_guard lock(dedup_tasks_mutex);
        dedup_tasks = convertIntoSubDedupTasks(all_visible_parts, all_staged_parts, all_uncommitted_parts, bucket_level_dedup, txn_id);
    }

    size_t dedup_pool_size = std::min(static_cast<size_t>(data.getSettings()->unique_table_dedup_threads), dedup_tasks.size());
    ThreadPool dedup_pool(dedup_pool_size);
    std::mutex mutex;
    for (size_t i = 0; i < dedup_tasks.size(); ++i)
    {
        {
            std::lock_guard lock(dedup_tasks_mutex);
            if (bucket_level_dedup && !dedup_tasks[i]->bucket_valid)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Enable bucket level unique keys, but dedup task is bucket invalid, it's a bug!");
        }

        dedup_pool.scheduleOrThrowOnError([&, i, thread_group = CurrentThread::getGroup()]() {
            SCOPE_EXIT({
                if (thread_group)
                    CurrentThread::detachQueryIfNotDetached();
            });
            if (thread_group)
                CurrentThread::attachToIfDetached(thread_group);
            setThreadName("DedupTask");

            DedupTaskPtr dedup_task_local;
            {
                std::lock_guard lock(dedup_tasks_mutex);
                dedup_task_local = dedup_tasks[i];
            }
            Stopwatch sub_task_watch;
            auto & visible_parts = dedup_task_local->visible_parts;
            auto & new_parts = dedup_task_local->new_parts;
            logDedupDetail(visible_parts, new_parts, dedup_task_local, /*is_sub_iteration*/false);

            /// Check whether new_parts contain partial_update_part
            auto it = std::find_if(new_parts.begin(), new_parts.end(), [&](const auto & part) { return part->needPartialUpdateProcess(); });
            bool has_partial_update_part = it != new_parts.end();

            /// Used to obtain the part name after processDedupSubTaskInPartialUpdateMode, as partial_update_parts iteration may generate new part
            /// Subsequent prepareBitmapsForPartialUpdate depends on this variable
            IMergeTreeDataPartsVector partial_update_processed_parts;
            DeleteBitmapVector bitmaps = has_partial_update_part ?
                processDedupSubTaskInPartialUpdateMode(visible_parts, new_parts, dedup_task_local, partial_update_processed_parts) :
                dedupImpl(visible_parts, new_parts, dedup_task_local);

            std::lock_guard lock(mutex);
            size_t num_bitmaps_to_dump = has_partial_update_part ?
                prepareBitmapsForPartialUpdate(visible_parts, bitmaps, txn_id, res, partial_update_processed_parts) :
                prepareBitmapsToDump(visible_parts, new_parts, bitmaps, txn_id, res);

            LOG_DEBUG(
                log,
                "Dedup {} in {} ms, visible parts={}, new parts={}, result bitmaps={}, txn_id: {}, dedup mode: {}",
                dedup_task_local->getDedupLevelInfo(),
                sub_task_watch.elapsedMilliseconds(),
                visible_parts.size(),
                new_parts.size(),
                num_bitmaps_to_dump,
                txn_id.toUInt64(),
                CnchDedupHelper::typeToString(dedup_mode));
        });
    }
    dedup_pool.wait();

    LOG_DEBUG(
        log,
        "Dedup {} tasks in {} ms, thread pool={}, visible parts={}, staged parts={}, uncommitted_parts = {}, result bitmaps={}, txn_id: "
        "{}, dedup mode: {}",
        dedup_tasks.size(),
        watch.elapsedMilliseconds(),
        dedup_pool_size,
        all_visible_parts.size(),
        all_staged_parts.size(),
        all_uncommitted_parts.size(),
        res.size(),
        txn_id.toUInt64(),
        CnchDedupHelper::typeToString(dedup_mode));
    return res;
}

MergeTreeDataDeduper::DedupTasks MergeTreeDataDeduper::convertIntoSubDedupTasks(
    const IMergeTreeDataPartsVector & all_visible_parts,
    const IMergeTreeDataPartsVector & all_staged_parts,
    const IMergeTreeDataPartsVector & all_uncommitted_parts,
    const bool & bucket_level_dedup,
    TxnTimestamp txn_id)
{
    /// Mark whether we can split dedup tasks into bucket granule.
    bool table_level_valid_bucket = bucket_level_dedup ? true : data.getInMemoryMetadataPtr()->checkIfClusterByKeySameWithUniqueKey();
    auto table_definition_hash = data.getTableHashForClusterBy();
    auto settings = data.getSettings();
    /// Check whether all parts has same table_definition_hash.
    /// Otherwise, we can not split dedup task to bucket granule.
    auto checkBucketTable = [&bucket_level_dedup, &table_definition_hash](const IMergeTreeDataPartsVector & all_parts, bool & valid_bucket) {
        if (!valid_bucket || bucket_level_dedup)
            return;
        auto it = std::find_if(all_parts.begin(), all_parts.end(), [&](const auto & part) {
            return part->bucket_number == -1 || !table_definition_hash.match(part->table_definition_hash);
        });
        if (it != all_parts.end())
            valid_bucket = false;
    };

    DedupTasks res;
    /// Prepare all new parts (staged + uncommitted) that need to be dedupped with visible parts.
    /// NOTE: the order of new parts is significant because it reflects the write order of the same key.
    IMergeTreeDataPartsVector all_new_parts = all_staged_parts;
    if (settings->partition_level_unique_keys)
    {
        /// New parts are first sorted by partition, and then within each partition sorted by part's written time
        std::sort(all_new_parts.begin(), all_new_parts.end(), [](auto & lhs, auto & rhs) {
            return std::forward_as_tuple(lhs->info.partition_id, lhs->commit_time)
                < std::forward_as_tuple(rhs->info.partition_id, rhs->commit_time);
        });
        if (!all_uncommitted_parts.empty())
        {
            all_new_parts.insert(all_new_parts.end(), all_uncommitted_parts.begin(), all_uncommitted_parts.end());
            std::stable_sort(all_new_parts.begin(), all_new_parts.end(), [](auto & lhs, auto & rhs) {
                return lhs->info.partition_id < rhs->info.partition_id;
            });
        }

        size_t i = 0, j = 0;
        while (j < all_new_parts.size())
        {
            String partition_id = all_new_parts[j]->info.partition_id;
            IMergeTreeDataPartsVector visible_parts;
            IMergeTreeDataPartsVector new_parts;
            while (i < all_visible_parts.size() && all_visible_parts[i]->info.partition_id == partition_id)
                visible_parts.push_back(all_visible_parts[i++]);
            while (j < all_new_parts.size() && all_new_parts[j]->info.partition_id == partition_id)
                new_parts.push_back(all_new_parts[j++]);

            bool partition_level_valid_bucket = table_level_valid_bucket;
            checkBucketTable(visible_parts, partition_level_valid_bucket);
            checkBucketTable(new_parts, partition_level_valid_bucket);
            if (!partition_level_valid_bucket)
                res.emplace_back(std::make_shared<DedupTask>(txn_id, partition_id, /*valid_bucket=*/false, -1, visible_parts, new_parts));
            else
            {
                /// There is no need to use stable sort for visible parts because there has no duplicated key in these parts.
                std::sort(visible_parts.begin(), visible_parts.end(), [](auto & lhs, auto & rhs) {
                    return lhs->bucket_number < rhs->bucket_number;
                });
                /// We must use stable sort here, because we can not change the order when bucket number is the same which is represented as commit order.
                std::stable_sort(
                    new_parts.begin(), new_parts.end(), [](auto & lhs, auto & rhs) { return lhs->bucket_number < rhs->bucket_number; });
                size_t p = 0, q = 0;
                while (q < new_parts.size())
                {
                    Int64 bucket_number = new_parts[q]->bucket_number;
                    IMergeTreeDataPartsVector bucket_visible_parts;
                    IMergeTreeDataPartsVector bucket_new_parts;
                    /// Skip all parts which bucket number is smaller.
                    while (p < visible_parts.size() && visible_parts[p]->bucket_number < bucket_number)
                        p++;
                    while (p < visible_parts.size() && visible_parts[p]->bucket_number == bucket_number)
                        bucket_visible_parts.push_back(visible_parts[p++]);
                    while (q < new_parts.size() && new_parts[q]->bucket_number == bucket_number)
                        bucket_new_parts.push_back(new_parts[q++]);

                    res.emplace_back(std::make_shared<DedupTask>(txn_id, partition_id, /*valid_bucket=*/true, bucket_number, bucket_visible_parts, bucket_new_parts));
                }
            }
        }
    }
    else
    {
        /// New parts are sorted by part's written time
        std::sort(all_new_parts.begin(), all_new_parts.end(), [](auto & lhs, auto & rhs) { return lhs->commit_time < rhs->commit_time; });
        all_new_parts.insert(all_new_parts.end(), all_uncommitted_parts.begin(), all_uncommitted_parts.end());
        checkBucketTable(all_visible_parts, table_level_valid_bucket);
        checkBucketTable(all_new_parts, table_level_valid_bucket);
        if (!table_level_valid_bucket)
            res.emplace_back(std::make_shared<DedupTask>(txn_id, /*partition_id=*/"", /*valid_bucket=*/false, -1, all_visible_parts, all_new_parts));
        else
        {
            IMergeTreeDataPartsVector sorted_visible_parts = all_visible_parts;
            /// There is no need to use stable sort for visible parts because there has no duplicated key in these parts.
            std::sort(sorted_visible_parts.begin(), sorted_visible_parts.end(), [](auto & lhs, auto & rhs) {
                return lhs->bucket_number < rhs->bucket_number;
            });
            /// We must use stable sort here, because we can not change the order when bucket number is the same which is represented as commit order.
            std::stable_sort(
                all_new_parts.begin(), all_new_parts.end(), [](auto & lhs, auto & rhs) { return lhs->bucket_number < rhs->bucket_number; });

            size_t i = 0;
            size_t j = 0;
            while (j < all_new_parts.size())
            {
                Int64 bucket_number = all_new_parts[j]->bucket_number;
                IMergeTreeDataPartsVector visible_parts;
                IMergeTreeDataPartsVector new_parts;
                /// Skip all parts which bucket number is smaller.
                while (i < sorted_visible_parts.size() && sorted_visible_parts[i]->bucket_number < bucket_number)
                    i++;
                while (i < sorted_visible_parts.size() && sorted_visible_parts[i]->bucket_number == bucket_number)
                    visible_parts.push_back(sorted_visible_parts[i++]);
                while (j < all_new_parts.size() && all_new_parts[j]->bucket_number == bucket_number)
                    new_parts.push_back(all_new_parts[j++]);

                res.emplace_back(std::make_shared<DedupTask>(txn_id, /*partition_id=*/"", /*valid_bucket=*/true, bucket_number, visible_parts, new_parts));
            }
        }
    }
    return res;
}

LocalDeleteBitmaps MergeTreeDataDeduper::repairParts(TxnTimestamp txn_id, IMergeTreeDataPartsVector all_visible_parts)
{
    if (all_visible_parts.empty())
        return {};

    LocalDeleteBitmaps res;
    auto prepare_bitmaps_to_dump
        = [txn_id, this, &res](const IMergeTreeDataPartsVector & visible_parts, DeleteBitmapVector & delta_bitmaps) -> size_t {
        size_t num_bitmaps = 0;
        size_t max_delete_bitmap_meta_depth = data.getSettings()->max_delete_bitmap_meta_depth;
        for (size_t i = 0; i < delta_bitmaps.size(); ++i)
        {
            auto base_bitmap = visible_parts[i]->getDeleteBitmap(/*allow_null*/ true);
            if (!delta_bitmaps[i])
            {
                if (base_bitmap)
                    continue;
                /// If delete bitmap is nullptr and base bitmap doesn't exist, bitmap meta is broken, it's necessary to create a new empty bitmap.
                delta_bitmaps[i] = std::make_shared<Roaring>();
            }

            size_t bitmap_meta_depth = visible_parts[i]->getDeleteBitmapMetaDepth();
            res.push_back(LocalDeleteBitmap::createBaseOrDelta(
                visible_parts[i]->info,
                base_bitmap,
                delta_bitmaps[i],
                txn_id.toUInt64(),
                bitmap_meta_depth >= max_delete_bitmap_meta_depth,
                visible_parts[i]->bucket_number));
            num_bitmaps++;
        }
        return num_bitmaps;
    };

    auto log_repair_detail = [&](const DedupTaskPtr & task, const IMergeTreeDataPartsVector & visible_parts) {
        WriteBufferFromOwnString msg;
        msg << "Start to repair in txn_id: " << txn_id.toUInt64() << ", dedup level info: " << task->getDedupLevelInfo()
            << ", visible_parts: [";
        for (size_t i = 0; i < visible_parts.size(); ++i)
        {
            if (i > 0)
                msg << ", ";
            msg << visible_parts[i]->name;
        }
        msg << "].";
        LOG_DEBUG(log, msg.str());
    };

    Stopwatch watch;
    bool bucket_level_dedup = data.getSettings()->enable_bucket_level_unique_keys;
    /// Take visible parts as staged parts here.
    DedupTasks repair_tasks = convertIntoSubDedupTasks({}, all_visible_parts, {}, bucket_level_dedup, txn_id);

    size_t dedup_pool_size = std::min(static_cast<size_t>(data.getSettings()->unique_table_dedup_threads), repair_tasks.size());
    ThreadPool dedup_pool(dedup_pool_size);
    std::mutex mutex;
    for (auto & dedup_task : repair_tasks)
    {
        if (bucket_level_dedup && !dedup_task->bucket_valid)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Enable bucket level unique keys, but dedup task is bucket invalid, it's a bug!");

        dedup_pool.scheduleOrThrowOnError([&, thread_group = CurrentThread::getGroup()]() {
            SCOPE_EXIT({
                if (thread_group)
                    CurrentThread::detachQueryIfNotDetached();
            });
            if (thread_group)
                CurrentThread::attachToIfDetached(thread_group);
            setThreadName("RepairParts");

            Stopwatch sub_task_watch;
            log_repair_detail(dedup_task, dedup_task->new_parts);
            DeleteBitmapVector delta_bitmaps = repairImpl(dedup_task->new_parts);

            std::lock_guard lock(mutex);
            size_t num_bitmaps_to_dump = prepare_bitmaps_to_dump(dedup_task->new_parts, delta_bitmaps);

            LOG_DEBUG(
                log,
                "Repair {} in {} ms, visible parts={}, result bitmaps={}",
                dedup_task->getDedupLevelInfo(),
                sub_task_watch.elapsedMilliseconds(),
                dedup_task->new_parts.size(),
                num_bitmaps_to_dump);
        });
    }
    dedup_pool.wait();

    LOG_DEBUG(
        log,
        "Repair {} tasks in {} ms, thread pool={}, visible parts={}, result bitmaps={}",
        repair_tasks.size(),
        watch.elapsedMilliseconds(),
        dedup_pool_size,
        all_visible_parts.size(),
        res.size());
    return res;
}

size_t MergeTreeDataDeduper::prepareBitmapsToDump(
    const IMergeTreeDataPartsVector & visible_parts,
    const IMergeTreeDataPartsVector & new_parts,
    const DeleteBitmapVector & bitmaps,
    TxnTimestamp txn_id,
    LocalDeleteBitmaps & res)
{
    size_t num_bitmaps = 0;
    size_t max_delete_bitmap_meta_depth = data.getSettings()->max_delete_bitmap_meta_depth;
    for (size_t i = 0; i < bitmaps.size(); ++i)
    {
        DeleteBitmapPtr bitmap = bitmaps[i];
        ImmutableDeleteBitmapPtr base_bitmap;
        if (i < visible_parts.size())
            base_bitmap = visible_parts[i]->getDeleteBitmap();
        else
            base_bitmap = new_parts[i - visible_parts.size()]->getDeleteBitmap(/*allow_null*/ true);

        /// Make sure part has delete bitmap
        if (!bitmap)
        {
            if (base_bitmap)
                continue;
            else
                bitmap = std::make_shared<Roaring>();
        }

        if (i < visible_parts.size())
        {
            LOG_DEBUG(
                log,
                "Preparing bitmap for visible part: {}, base bitmap cardinality: {}, delta bitmap cardinality: {}, txn_id: {}",
                visible_parts[i]->name,
                base_bitmap->cardinality(),
                bitmap->cardinality(),
                txn_id.toUInt64());
            size_t bitmap_meta_depth = visible_parts[i]->getDeleteBitmapMetaDepth();
            res.emplace_back(LocalDeleteBitmap::createBaseOrDelta(
                visible_parts[i]->info,
                base_bitmap,
                bitmap,
                txn_id.toUInt64(),
                bitmap_meta_depth >= max_delete_bitmap_meta_depth,
                visible_parts[i]->bucket_number));
        }
        else /// new part
        {
            LOG_DEBUG(
                log,
                "Preparing bitmap for new part: {}, base bitmap cardinality: {}, delta bitmap cardinality: {}, txn_id: {}",
                new_parts[i - visible_parts.size()]->name,
                (base_bitmap ? base_bitmap->cardinality() : 0),
                bitmap->cardinality(),
                txn_id.toUInt64());
            if (base_bitmap)
            {
                if (new_parts[i - visible_parts.size()]->delete_flag)
                {
                    LOG_TRACE(
                        log,
                        "Part {} already have delete bitmap meta in txn_id {} due to delete flag, here must generate a delta bitmap",
                        new_parts[i - visible_parts.size()]->name,
                        txn_id);
                    res.push_back(LocalDeleteBitmap::createDelta(
                        new_parts[i - visible_parts.size()]->info,
                        bitmap,
                        txn_id.toUInt64(),
                        new_parts[i - visible_parts.size()]->bucket_number));
                }
                else
                {
                    size_t bitmap_meta_depth = new_parts[i - visible_parts.size()]->getDeleteBitmapMetaDepth();
                    res.push_back(LocalDeleteBitmap::createBaseOrDelta(
                        new_parts[i - visible_parts.size()]->info,
                        base_bitmap,
                        bitmap,
                        txn_id.toUInt64(),
                        bitmap_meta_depth >= max_delete_bitmap_meta_depth,
                        new_parts[i - visible_parts.size()]->bucket_number));
                }
            }
            else
                res.push_back(LocalDeleteBitmap::createBase(
                    new_parts[i - visible_parts.size()]->info,
                    bitmap,
                    txn_id.toUInt64(),
                    new_parts[i - visible_parts.size()]->bucket_number));
        }
        num_bitmaps++;
    }
    return num_bitmaps;
}

size_t MergeTreeDataDeduper::prepareBitmapsForPartialUpdate(
    const IMergeTreeDataPartsVector & visible_parts,
    const DeleteBitmapVector & bitmaps,
    TxnTimestamp txn_id,
    LocalDeleteBitmaps & res,
    const IMergeTreeDataPartsVector & partial_update_processed_parts)
{
    size_t num_bitmaps = 0;
    for (size_t i = 0; i < bitmaps.size(); ++i)
    {
        DeleteBitmapPtr bitmap = bitmaps[i];

        if (i < visible_parts.size())
        {
            LOG_DEBUG(
                log,
                "Preparing bitmap for visible part: {}, total bitmap cardinality: {}, txn_id: {}",
                visible_parts[i]->name,
                bitmap->cardinality(),
                txn_id.toUInt64());

            res.push_back(LocalDeleteBitmap::createBase(
                    visible_parts[i]->info,
                    bitmap,
                    txn_id.toUInt64(),
                    visible_parts[i]->bucket_number));
        }
        else /// new part
        {
            LOG_DEBUG(
                log,
                "Preparing bitmap for new part: {}, total bitmap cardinality: {}, txn_id: {}",
                partial_update_processed_parts[i]->name,
                bitmap->cardinality(),
                txn_id.toUInt64());

            UInt64 bitmap_version = partial_update_processed_parts[i]->getDeleteBitmapMetaDepth() > 0 ? partial_update_processed_parts[i]->getDeleteBitmapVersion() : 0;
            if (bitmap_version == txn_id.toUInt64())
            {
                LOG_TRACE(
                    log,
                    "Part {} already have delete bitmap meta in txn_id {} due to delete flag, here must generate a delta bitmap",
                    partial_update_processed_parts[i]->name,
                    txn_id);
                res.push_back(LocalDeleteBitmap::createDelta(
                    partial_update_processed_parts[i]->info,
                    bitmap,
                    txn_id.toUInt64(),
                    partial_update_processed_parts[i]->bucket_number));
            }
            else
            {
                res.push_back(LocalDeleteBitmap::createBase(
                    partial_update_processed_parts[i]->info,
                    bitmap,
                    txn_id.toUInt64(),
                    partial_update_processed_parts[i]->bucket_number));
            }
        }
        num_bitmaps++;
    }
    return num_bitmaps;
}

DeleteBitmapVector MergeTreeDataDeduper::generateNextIterationBitmaps(
    const IMergeTreeDataPartsVector & visible_parts,
    const IMergeTreeDataPartsVector & new_parts,
    const DeleteBitmapVector & bitmaps,
    TxnTimestamp txn_id)
{
    DeleteBitmapVector next_iteration_bitmaps;
    for (size_t i = 0; i < bitmaps.size(); ++i)
    {
        DeleteBitmapPtr bitmap = bitmaps[i];
        ImmutableDeleteBitmapPtr base_bitmap;
        if (i < visible_parts.size())
            base_bitmap = visible_parts[i]->getDeleteBitmap();
        else
            base_bitmap = new_parts[i - visible_parts.size()]->getDeleteBitmap(/*allow_null*/ true);

        /// Make sure part has delete bitmap
        if (!bitmap)
        {
            if (base_bitmap)
            {
                next_iteration_bitmaps.push_back(const_pointer_cast<Roaring>(base_bitmap));
                continue;
            }
            else
                bitmap = std::make_shared<Roaring>();
        }

        DeleteBitmapPtr next_iteration_bitmap_item = std::make_shared<Roaring>();
        *next_iteration_bitmap_item = *bitmap;
        if (base_bitmap)
            *next_iteration_bitmap_item |= *base_bitmap;
        next_iteration_bitmaps.push_back(next_iteration_bitmap_item);

        if (i < visible_parts.size())
        {
            LOG_DEBUG(
                log,
                "Preparing bitmap for visible part: {}, base bitmap cardinality: {}, delta bitmap cardinality: {}, txn_id: {}",
                visible_parts[i]->name,
                base_bitmap->cardinality(),
                bitmap->cardinality(),
                txn_id.toUInt64());
        }
        else /// new part
        {
            LOG_DEBUG(
                log,
                "Preparing bitmap for new part: {}, base bitmap cardinality: {}, delta bitmap cardinality: {}, txn_id: {}",
                new_parts[i - visible_parts.size()]->name,
                (base_bitmap ? base_bitmap->cardinality() : 0),
                bitmap->cardinality(),
                txn_id.toUInt64());
        }
    }
    return next_iteration_bitmaps;
}

void MergeTreeDataDeduper::logDedupDetail(const IMergeTreeDataPartsVector & visible_parts, const IMergeTreeDataPartsVector & new_parts, const DedupTaskPtr & task, bool is_sub_iteration)
{
    WriteBufferFromOwnString msg;
    if (is_sub_iteration)
        msg << "Start to dedup in sub iteration of txn_id: " << task->txn_id.toUInt64() << ", is partial update iteration=" << (!new_parts.empty() ? new_parts[0]->needPartialUpdateProcess() : 0);
    else
        msg << "Start to dedup in txn_id: " << task->txn_id.toUInt64() << ", dedup level info: " << task->getDedupLevelInfo();

    msg << ", visible_parts: [";
    for (size_t i = 0; i < visible_parts.size(); ++i)
    {
        if (i > 0)
            msg << ", ";
        msg << visible_parts[i]->name;
    }
    msg << "], new_parts: [";
    for (size_t i = 0; i < new_parts.size(); ++i)
    {
        if (i > 0)
            msg << ", ";
        msg << new_parts[i]->name;
    }
    msg << "].";
    LOG_DEBUG(log, msg.str());
}

std::vector<Block> MergeTreeDataDeduper::restoreBlockFromNewParts(const IMergeTreeDataPartsVector & current_dedup_new_parts, const DedupTaskPtr & dedup_task, bool & optimize_for_same_update_columns, NameSet & same_update_column_set)
{
    auto metadata_snapshot = data.getInMemoryMetadataPtr();
    auto sample_block = metadata_snapshot->getSampleBlock();
    Names column_names = sample_block.getNames();
    NameSet name_set = sample_block.getNameSet();

    size_t read_new_part_thread_num = std::max(
        static_cast<size_t>(1),
        std::min(current_dedup_new_parts.size(), static_cast<size_t>(data.getSettings()->partial_update_query_parts_thread_size)));
    bool use_thread_pool = read_new_part_thread_num > 1;
    ThreadPool read_new_data_pool(read_new_part_thread_num);
    std::vector<Block> blocks_from_current_dedup_new_parts{current_dedup_new_parts.size()};
    std::vector<NameSet> update_column_set_list{current_dedup_new_parts.size()};
    std::atomic<bool> optimize_for_same_update_columns_atomic = true;

    auto read_new_part = [&](size_t part_id) {
        auto & to_block = blocks_from_current_dedup_new_parts[part_id];
        /// A new sample block is needed here by sample_block.cloneEmpty().
        /// In the case of direct assignment, the sizes of map columns will be affected by different threads, causing checkNumberOfRows to fail.
        to_block = sample_block.cloneEmpty();
        const auto & part = current_dedup_new_parts[part_id];
        Stopwatch inner_watch;
        UInt64 read_func_columns_ms = 0, read_cost_ms = 0;

        NameSet columns_read_set;
        Names columns_read;
        auto update_columns = loadFuncColumn(part, StorageInMemoryMetadata::UPDATE_COLUMNS);
        if (!update_columns->empty() && update_columns->hasEqualValues() && data.getSettings()->partial_update_optimize_for_batch_task)
        {
            String same_update_columns = update_columns->getDataAt(0).toString();
            size_t last_pos = 0, pos = 0, size = same_update_columns.size();
            LOG_DEBUG(log, "Same update columns: {}, txn id: {}, part name: {}", same_update_columns, dedup_task->txn_id, part->name);

            if (size == 0)
                columns_read = column_names;
            else
            {
                /// Restore update columns must use part columns instead of storage columns
                Names part_columns = part->getColumnsPtr()->getNames();
                while (pos < size)
                {
                    last_pos = pos;
                    /// Here we will not handle special characters like space and not support regex
                    while (pos < size && same_update_columns[pos] != ',')
                        pos++;
                    size_t idx = std::stoull(same_update_columns.substr(last_pos, pos - last_pos));
                    if (idx >= part_columns.size())
                        LOG_WARNING(
                            log,
                            "Index `{}` is invalid, ignore it. Txn id: {}, part name: {}, update columns: {}",
                            idx,
                            dedup_task->txn_id,
                            part->name,
                            same_update_columns);
                    else if (!name_set.contains(part_columns[idx]))
                        LOG_WARNING(log, "Update column `{}` doesn't exist, ignore.", part_columns[idx]);
                    else
                        columns_read_set.insert(part_columns[idx]);
                    pos++;
                }

                /// Must read necessary columns, include partition key, unique key and version
                for (const String & col : metadata_snapshot->getColumnsRequiredForPartitionKey())
                    columns_read_set.insert(col);
                for (const String & col : metadata_snapshot->getColumnsRequiredForUniqueKey())
                    columns_read_set.insert(col);
                if (data.merging_params.hasExplicitVersionColumn())
                    columns_read_set.insert(data.merging_params.version_column);

                columns_read = {columns_read_set.begin(), columns_read_set.end()};
            }
        }
        else
        {
            optimize_for_same_update_columns_atomic = false;
            columns_read = column_names;
        }

        if (optimize_for_same_update_columns_atomic)
            update_column_set_list[part_id] = {columns_read.begin(), columns_read.end()};

        size_t total_size = update_columns->size();
        to_block.insert(
            ColumnWithTypeAndName{std::move(update_columns), std::make_shared<DataTypeString>(), StorageInMemoryMetadata::UPDATE_COLUMNS});

        read_func_columns_ms = inner_watch.elapsedMilliseconds();
        inner_watch.restart();

        auto input = std::make_shared<MergeTreeSequentialSource>(
            data,
            data.getStorageSnapshot(metadata_snapshot, context),
            current_dedup_new_parts[part_id],
            /*delete_bitmap_*/ nullptr,
            columns_read,
            /*read_with_direct_io_*/ false,
            /*take_column_types_from_storage*/ true);
        QueryPipeline pipeline;
        pipeline.init(Pipe(std::move(input)));
        BlockInputStreamPtr input_stream = std::make_shared<PipelineExecutingBlockInputStream>(std::move(pipeline));
        input_stream->readPrefix();

        while (Block block = input_stream->read())
        {
            for (size_t j = 0, size = block.columns(); j < size; ++j)
            {
                const auto source_column = block.getByPosition(j).column;
                LOG_TRACE(log, "Column read name: {}", columns_read[j]);
                auto mutable_column = IColumn::mutate(std::move(to_block.getByName(columns_read[j]).column));
                mutable_column->insertRangeFrom(*source_column, 0, source_column->size());

                to_block.getByName(columns_read[j]).column = std::move(mutable_column);
            }
        }

        for (size_t j = 0, size = to_block.columns(); j < size; ++j)
        {
            if (to_block.getByPosition(j).column->empty())
            {
                auto mutable_column = IColumn::mutate(std::move(to_block.getByPosition(j).column));
                mutable_column->insertManyDefaults(total_size);

                to_block.getByPosition(j).column = std::move(mutable_column);
            }
        }
        read_cost_ms = inner_watch.elapsedMilliseconds();
        inner_watch.restart();

        to_block.insert(ColumnWithTypeAndName{
            std::move(loadFuncColumn(part, StorageInMemoryMetadata::DELETE_FLAG_COLUMN_NAME)),
            std::make_shared<DataTypeUInt8>(),
            StorageInMemoryMetadata::DELETE_FLAG_COLUMN_NAME});
        to_block.checkNumberOfRows();

        auto dedup_sort_column = loadFuncColumn(part, StorageInMemoryMetadata::DEDUP_SORT_COLUMN);
        if (dedup_sort_column)
        {
            to_block.insert(ColumnWithTypeAndName{
                std::move(dedup_sort_column), std::make_shared<DataTypeUInt64>(), StorageInMemoryMetadata::DEDUP_SORT_COLUMN});
            SortDescription sort_description;
            sort_description.emplace_back(StorageInMemoryMetadata::DEDUP_SORT_COLUMN, 1 , 1);
            if (!isAlreadySorted(to_block, sort_description))
                stableSortBlock(to_block, sort_description);
            to_block.erase(StorageInMemoryMetadata::DEDUP_SORT_COLUMN);
        }

        /// We need to add a part_id column for replaceColumnsAndFilterData, as different part may have different column order
        auto part_id_column = ColumnUInt64::create();
        part_id_column->insertMany(part_id, to_block.rows());
        to_block.insert(
            ColumnWithTypeAndName{std::move(part_id_column), std::make_shared<DataTypeUInt64>(), StorageInMemoryMetadata::PART_ID_COLUMN});
        read_func_columns_ms += inner_watch.elapsedMilliseconds();

        input_stream->readSuffix();
        LOG_DEBUG(
            log,
            "Read new part {} cost {} ms, read func columns cost {} ms, part id: {}, columns read size: {}, txn id: {}, dedup info: {}",
            part->name,
            read_cost_ms,
            read_func_columns_ms,
            part_id,
            columns_read.size(),
            dedup_task->txn_id,
            dedup_task->getDedupLevelInfo());
    };
    for (size_t part_id = 0; part_id < current_dedup_new_parts.size(); part_id++)
    {
        if (use_thread_pool)
            read_new_data_pool.scheduleOrThrowOnError([&, part_id, thread_group = CurrentThread::getGroup()]() {
                SCOPE_EXIT({
                    if (thread_group)
                        CurrentThread::detachQueryIfNotDetached();
                });
                if (thread_group)
                    CurrentThread::attachToIfDetached(thread_group);
                setThreadName("ReadNewParts");
                read_new_part(part_id);
            });
        else
            read_new_part(part_id);
    }
    if (use_thread_pool)
        read_new_data_pool.wait();

    optimize_for_same_update_columns = optimize_for_same_update_columns_atomic && hasSameUpdateColumns(update_column_set_list);
    if (optimize_for_same_update_columns)
    {
        if (!update_column_set_list.empty())
            same_update_column_set = update_column_set_list[0];
    }
    return blocks_from_current_dedup_new_parts;
}

DeleteBitmapVector MergeTreeDataDeduper::processDedupSubTaskInPartialUpdateMode(const IMergeTreeDataPartsVector & visible_parts, const IMergeTreeDataPartsVector & new_parts, DedupTaskPtr & dedup_task, IMergeTreeDataPartsVector & partial_update_processed_parts)
{
    IMergeTreeDataPartsVector current_dedup_visible_parts;
    current_dedup_visible_parts.insert(current_dedup_visible_parts.end(), visible_parts.begin(), visible_parts.end());
    DeleteBitmapVector current_iteration_bitmaps;
    auto metadata_snapshot = data.getInMemoryMetadataPtr();
    auto sample_block = metadata_snapshot->getSampleBlock();
    Names column_names = sample_block.getNames();

    auto iterate_begin = new_parts.begin(), iterate_end = new_parts.begin();
    /// Since the partial_update_parts is incomplete, the processing logic is different from that of the normal_parts.
    /// The new parts processed in each iteration are either normal_parts or partial_update_parts
    /// After partial_update_parts iteration, the processed partial_update_parts' will be then treated as normal_parts.
    /// It may take several iterations until all new_parts are processed.
    /// A portion of the bitmap information will be generated for each iteration, which needs to be collected and finally integrated together.
    while (true)
    {
        bool partial_update_iteration = false;
        if (iterate_end != new_parts.end())
            partial_update_iteration = iterate_end->get()->needPartialUpdateProcess();
        else
            break;

        iterate_begin = iterate_end;
        iterate_end = std::find_if(iterate_end, new_parts.end(), [&](const auto & part) { return part->needPartialUpdateProcess() != partial_update_iteration; });

        IMergeTreeDataPartsVector current_dedup_new_parts;
        UInt64 current_part_number{0};
        UInt64 current_part_row_count{0};
        auto pick_current_dedup_new_parts = [&](const IMergeTreeDataPartPtr & part) -> void {
            current_part_number++;
            current_part_row_count += part->rows_count;
            current_dedup_new_parts.push_back(part);
        };
        for (auto it = iterate_begin; it != iterate_end;)
        {
            pick_current_dedup_new_parts(*it);
            it++;
            if (current_part_number >= data.getSettings()->partial_update_max_process_parts || current_part_row_count >= data.getSettings()->partial_update_max_process_rows)
            {
                iterate_end = it;
                break;
            }
        }

        logDedupDetail(current_dedup_visible_parts, current_dedup_new_parts, dedup_task, /*is_sub_iteration*/true);

        if (partial_update_iteration)
        {
            /// Record the time taken for each phase for subsequent analysis and performance optimization, metric detail:
            /// 1. Restore block
            /// 2. Concat block
            /// 3. Remove duplicate key in block
            /// 4. Calculate key
            /// 5. Search index
            /// 6. Query history data
            /// 7. Handle index
            /// 8. Replace column
            /// 9. Generate partial part
            std::vector<UInt64> timer;
            Stopwatch phase_watch;
            auto record_and_restart_timer = [&] (Stopwatch & watch) {
                timer.emplace_back(watch.elapsedMilliseconds());
                watch.restart();
            };

            bool optimize_for_same_update_columns = true;
            NameSet same_update_column_set;
            /// Phase 1: restore block from iteration current_dedup_new_parts
            std::vector<Block> blocks_from_current_dedup_new_parts = restoreBlockFromNewParts(current_dedup_new_parts, dedup_task, optimize_for_same_update_columns, same_update_column_set);

            record_and_restart_timer(phase_watch);

            Block block_to_process = concatenateBlocks(blocks_from_current_dedup_new_parts);
            size_t block_size = block_to_process.rows();

            if (data.getSettings()->partial_update_detail_logging)
                dumpBlockForLogging(block_to_process, "Block structure from new parts", log);

            record_and_restart_timer(phase_watch);
            /// Phase 2: Filter duplicate keys in block and get replace info
            /// TODO: To support conditional updates, some modifications may be required here
            ColumnPtr version_column = nullptr;
            /// copy element here because removeDupKeys below may change block,
            /// which will invalidate all pointers and references to its elements.
            /// note that this won't copy the actual data of the column.
            if (data.merging_params.hasExplicitVersionColumn())
                version_column = block_to_process.getByName(data.merging_params.version_column).column;

            /// We can use any partition version of part in partial update mode, because partial update is not supported at the table level.
            UInt64 cur_version = 0;
            if (data.merging_params.partitionValueAsVersion())
                cur_version = current_dedup_visible_parts[0]->getVersionFromPartition();

            DeleteBitmapVector delta_bitmaps(current_dedup_visible_parts.size());
            PaddedPODArray<UInt32> replace_dst_indexes, replace_src_indexes;
            /// XXX: could use FilterInfo struct
            IColumn::Filter filter(block_size, 1);
            size_t num_filtered = removeDupKeysInPartialUpdateMode(block_to_process, version_column, filter, replace_dst_indexes, replace_src_indexes);

            PartRowidPairs part_rowid_pairs {current_dedup_visible_parts.size()};

            auto unique_key_column_names = metadata_snapshot->getUniqueKeyColumns();

            ColumnWithTypeAndName delete_flag_column;
            if (block_to_process.has(StorageInMemoryMetadata::DELETE_FLAG_COLUMN_NAME))
                delete_flag_column = block_to_process.getByName(StorageInMemoryMetadata::DELETE_FLAG_COLUMN_NAME);

            /// In the case that engine has been set version column, if version is set by user(not zero), the delete row will obey the rule of version.
            /// Otherwise, the delete row will ignore comparing version, just doing the deletion directly.
            auto delete_ignore_version = [&](size_t rowid) {
                return delete_flag_column.column && delete_flag_column.column->getBool(rowid) && version_column
                    && !version_column->getUInt(rowid);
            };

            record_and_restart_timer(phase_watch);
            /********************************************************************************************************
            * Phase 3: Get indexes that need to be searched in previous parts.
            * It's necessary to remove those rows whose version is lower than that of previous parts.
            * For example, table has four column, the first column is unique key and second column is version:
            * CREATE TABLE test.example (id UInt8, version UInt8, int0 UInt8, String0 String) Engine=CnchMergeTree...
            * Currently, it has one row (1, 4, 0, '') whose int0 and String0 are both default value.
            * The insert block has two row: (1, 3, 3, 'a') (1, 4, 4, '')
            * After removeDupKeys method, replace_dst_indexes is {1}, replace_src_indexes {0}
            * But version(3) of row 0 is lower than that(4) of previous parts, so row 0 should be discard.
            * The final result should be (1, 4, 4, '') whose String0 is still default value.
            * If version of row 0 is set to 4, the final result would be (1, 4, 4, 'a').
            ********************************************************************************************************/
            /// TODO: For conditional update scenarios, we may need to put the remove judgment of version at the later stage,
            /// because we need to judge the version based on the actual data + _update_version_column_
            PaddedPODArray<UInt32> tmp_replace_dst_indexes, tmp_replace_src_indexes;
            size_t replace_index_id = 0;

            std::vector<SerializationPtr> serializations;
            serializations.resize(unique_key_column_names.size());
            auto calculateKey = [&](size_t & rowid) -> String {
                WriteBufferFromOwnString buf;
                size_t id = 0;
                for (auto & col_name : unique_key_column_names)
                {
                    auto & col = block_to_process.getByName(col_name);
                    if (!serializations[id])
                        serializations[id] = col.type->getDefaultSerialization();
                    serializations[id]->serializeMemComparable(*col.column, rowid, buf);
                    id++;
                }
                return std::move(buf.str());
            };

            UniqueKeys prepared_keys;
            SearchKeysResult prepared_keys_result;
            std::vector<size_t> keys_perm_info;

            /// We need to reorder blocks using unique keys to speed up unique index lookups in visible parts
            prepared_keys.reserve(block_size);
            for (size_t rowid = 0; rowid < block_size; ++rowid)
            {
                if (filter[rowid] == 0)
                    continue;

                prepared_keys.push_back(calculateKey(rowid));
            }

            {
                /// Record the permutation info.
                /// For example, prepared keys are {"5","7","3","1","2","4","6"}, and permutation info is {4,6,2,0,1,3,5}
                std::vector<size_t> keys_perm_info_tmp(prepared_keys.size());
                std::generate(keys_perm_info_tmp.begin(), keys_perm_info_tmp.end(), [n = 0]() mutable { return n++; });
                std::sort(keys_perm_info_tmp.begin(), keys_perm_info_tmp.end(), [&prepared_keys](UInt32 lhs, UInt32 rhs) {
                    return prepared_keys[lhs] < prepared_keys[rhs];
                });
                keys_perm_info.resize(prepared_keys.size());
                for (size_t i = 0; i < prepared_keys.size(); ++i)
                    keys_perm_info[keys_perm_info_tmp[i]] = i;
            }
            record_and_restart_timer(phase_watch);

            prepared_keys_result = searchPartForKeys(current_dedup_visible_parts, prepared_keys);
            record_and_restart_timer(phase_watch);

            for (size_t rowid = 0, prepared_keys_idx = 0; rowid < block_size; ++rowid)
            {
                if (filter[rowid] == 0)
                    continue;

                String key;
                /// search key in existing parts
                SearchKeyResult search_res;

                size_t perm_idx = keys_perm_info[prepared_keys_idx];
                key = std::move(prepared_keys[perm_idx]);
                search_res = std::move(prepared_keys_result[perm_idx]);
                prepared_keys_idx++;

                if (search_res.part_index < 0)
                {
                    /// check if it's just a delete operation
                    if (delete_flag_column.column && delete_flag_column.column->getBool(rowid))
                    {
                        filter[rowid] = 0;
                        num_filtered++;
                    }
                    continue;
                }

                UInt32 part_index;
                UInt32 part_rowid = search_res.part_rowid;
                UInt64 part_version = search_res.part_version;
                try
                {
                    part_index = boost::numeric_cast<UInt32>(search_res.part_index);
                }
                catch (...)
                {
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "part index numeric cast error, part_index is {}", search_res.part_index);
                }

                if (!data.merging_params.version_column.empty() && !delete_ignore_version(rowid))
                {
                    if (version_column)
                        cur_version = version_column->getUInt(rowid);
                    if (cur_version < part_version)
                    {
                        filter[rowid] = 0;
                        num_filtered++;
                        continue;
                    }
                }

                /// check if it's just a delete operation
                if (delete_flag_column.column && delete_flag_column.column->getBool(rowid))
                {
                    filter[rowid] = 0;
                    num_filtered++;
                }
                else
                {
                    part_rowid_pairs[part_index].push_back({part_rowid, static_cast<UInt32>(rowid + block_size)});

                    /// Some rows may be just replace in block and not found in previous parts.
                    while (replace_index_id < replace_dst_indexes.size() && replace_dst_indexes[replace_index_id] < rowid)
                    {
                        if (filter[replace_dst_indexes[replace_index_id]])
                        {
                            tmp_replace_dst_indexes.push_back(replace_dst_indexes[replace_index_id]);
                            tmp_replace_src_indexes.push_back(replace_src_indexes[replace_index_id]);
                        }
                        replace_index_id++;
                    }
                    /// Check whether to delete the previous one, there maybe multiple replaced column for the same
                    while (replace_index_id < replace_dst_indexes.size() && replace_dst_indexes[replace_index_id] == rowid)
                    {
                        if (version_column)
                            cur_version = version_column->getUInt(rowid);
                        if (cur_version >= part_version)
                        {
                            tmp_replace_dst_indexes.push_back(replace_dst_indexes[replace_index_id]);
                            tmp_replace_src_indexes.push_back(replace_src_indexes[replace_index_id]);
                        }
                        replace_index_id++;
                    }
                }

                /// mark delete existing row with the same key
                /// TODO: In conditional update mode, we may need to generate the bitmap after the query of history data.
                addRowIdToBitmap(delta_bitmaps[part_index], part_rowid);
            }

            while (replace_index_id < replace_dst_indexes.size())
            {
                if (filter[replace_dst_indexes[replace_index_id]])
                {
                    tmp_replace_dst_indexes.push_back(replace_dst_indexes[replace_index_id]);
                    tmp_replace_src_indexes.push_back(replace_src_indexes[replace_index_id]);
                }
                replace_index_id++;
            }
            replace_dst_indexes = std::move(tmp_replace_dst_indexes);
            replace_src_indexes = std::move(tmp_replace_src_indexes);

            record_and_restart_timer(phase_watch);

            /// Phase 4: Query data from previous parts parallel
            size_t valid_query_parts_num = 0, total_query_row = 0;
            for (const auto & part_rowid_pair : part_rowid_pairs)
            {
                if (part_rowid_pair.empty())
                    continue;
                valid_query_parts_num++;
                total_query_row += part_rowid_pair.size();
            }
            size_t read_history_part_thread_num = std::max(
                static_cast<size_t>(1), std::min(valid_query_parts_num, static_cast<size_t>(data.getSettings()->partial_update_query_parts_thread_size)));
            bool use_thread_pool = read_history_part_thread_num > 1;
            ThreadPool read_history_data_pool(read_history_part_thread_num);
            std::vector<Block> columns_from_storage_vector(read_history_part_thread_num);
            std::vector<PaddedPODArray<UInt32>> block_rowids_vector(read_history_part_thread_num);

            auto read_history_data = [&](size_t thread_id) {
                size_t j = 0, cnt = 0;
                while (j < part_rowid_pairs.size() && cnt != thread_id)
                {
                    if (!part_rowid_pairs[j].empty())
                        ++cnt;
                    ++j;
                }
                cnt = 0;
                while (j < part_rowid_pairs.size())
                {
                    if (!part_rowid_pairs[j].empty())
                    {
                        if (cnt == 0)
                        {
                            Block columns_to_read = getReadBlockForHistoryData(data, optimize_for_same_update_columns, same_update_column_set);
                            /// Read block only necessary
                            if (columns_to_read.columns() != 0)
                            {
                                if (data.getSettings()->partial_update_query_columns_thread_size == 1)
                                    readColumnsFromStorage(
                                        current_dedup_visible_parts[j],
                                        part_rowid_pairs[j],
                                        columns_to_read,
                                        block_rowids_vector[thread_id],
                                        dedup_task);
                                else
                                    readColumnsFromStorageParallel(
                                        current_dedup_visible_parts[j],
                                        part_rowid_pairs[j],
                                        columns_to_read,
                                        block_rowids_vector[thread_id],
                                        dedup_task);
                            }
                            for (size_t i = 0, size = columns_from_storage_vector[thread_id].columns(); i < size; ++i)
                            {
                                String column_name = columns_from_storage_vector[thread_id].getByPosition(i).name;
                                if (columns_to_read.has(column_name))
                                {
                                    const auto source_column = columns_to_read.getByName(column_name).column;
                                    auto mutable_column = IColumn::mutate(std::move(columns_from_storage_vector[thread_id].getByPosition(i).column));
                                    mutable_column->insertRangeFrom(*source_column, 0, source_column->size());

                                    columns_from_storage_vector[thread_id].getByPosition(i).column = std::move(mutable_column);
                                }
                                else
                                {
                                    auto mutable_column = IColumn::mutate(std::move(columns_from_storage_vector[thread_id].getByPosition(i).column));
                                    mutable_column->insertManyDefaults(part_rowid_pairs[j].size());

                                    columns_from_storage_vector[thread_id].getByPosition(i).column = std::move(mutable_column);
                                }
                            }
                            if (columns_to_read.columns() == 0)
                            {
                                for (auto & pair : part_rowid_pairs[j])
                                    block_rowids_vector[thread_id].push_back(pair.block_rowid);
                            }
                        }

                        if (++cnt == read_history_part_thread_num)
                            cnt = 0;
                    }
                    j++;
                }
            };
            for (size_t i = 0; i < read_history_part_thread_num; ++i)
            {
                columns_from_storage_vector[i] = metadata_snapshot->getSampleBlock();
                if (use_thread_pool)
                    read_history_data_pool.scheduleOrThrowOnError([&, i, thread_group = CurrentThread::getGroup()]() {
                        SCOPE_EXIT({
                            if (thread_group)
                                CurrentThread::detachQueryIfNotDetached();
                        });
                        if (thread_group)
                            CurrentThread::attachToIfDetached(thread_group);
                        setThreadName("ReadOldParts");
                        read_history_data(i);
                    });
                else
                    read_history_data(i);
            }
            if (use_thread_pool)
                read_history_data_pool.wait();

            for (size_t i = 0, size = columns_from_storage_vector[0].columns(); i < size; ++i)
            {
                auto mutable_column = IColumn::mutate(std::move(columns_from_storage_vector[0].getByPosition(i).column));
                for (size_t j = 1; j < read_history_part_thread_num; ++j)
                {
                    const auto source_column = columns_from_storage_vector[j].getByPosition(i).column;
                    mutable_column->insertRangeFrom(*source_column, 0, source_column->size());
                }
                columns_from_storage_vector[0].getByPosition(i).column = std::move(mutable_column);
            }
            for (size_t j = 1; j < read_history_part_thread_num; ++j)
                block_rowids_vector[0].insert(block_rowids_vector[j].begin(), block_rowids_vector[j].end());
            Block & columns_from_storage = columns_from_storage_vector[0];
            PaddedPODArray<UInt32> & block_rowids = block_rowids_vector[0];
            if (columns_from_storage.rows() != block_rowids.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Size of block {} is not equal to replace rows {}", columns_from_storage.rows(), block_rowids.size());
            /// Generate fake _part_id_ column
            auto res = ColumnUInt64::create();
            res->insertMany(0, columns_from_storage.rows());
            columns_from_storage.insert(ColumnWithTypeAndName{std::move(res), std::make_shared<DataTypeUInt64>(), StorageInMemoryMetadata::PART_ID_COLUMN});

            if (data.getSettings()->partial_update_detail_logging)
                dumpBlockForLogging(columns_from_storage, "Block structure from history parts", log);

            record_and_restart_timer(phase_watch);
            /// Phase 5: Replace column and filter data parallel.
            replaceColumnsAndFilterData(block_to_process, columns_from_storage, filter, num_filtered, block_rowids, replace_dst_indexes, replace_src_indexes, current_dedup_new_parts, dedup_task, optimize_for_same_update_columns, same_update_column_set);

            if (data.getSettings()->partial_update_detail_logging)
                dumpBlockForLogging(block_to_process, "Block structure after processing", log);

            record_and_restart_timer(phase_watch);
            /// Phase 6: Generate partial part
            IMergeTreeDataPartPtr iteration_visible_part = generateAndCommitPartInPartialUpdateMode(block_to_process, current_dedup_new_parts, dedup_task);

            current_iteration_bitmaps = generateNextIterationBitmaps(current_dedup_visible_parts, current_dedup_new_parts, delta_bitmaps, dedup_task->txn_id);
            if (!iteration_visible_part->deleted)
            {
                current_dedup_visible_parts.insert(current_dedup_visible_parts.end(), iteration_visible_part);
                current_iteration_bitmaps.push_back(std::make_shared<Roaring>());
            }
            /// Assign bitmaps for next sub iteration
            for (size_t i = 0; i < current_dedup_visible_parts.size(); ++i)
            {
                IMergeTreeMutableDataPartPtr temp_visible_part = std::const_pointer_cast<IMergeTreeDataPart>(current_dedup_visible_parts[i]);
                temp_visible_part->setDeleteBitmap(current_iteration_bitmaps[i]);
            }
            record_and_restart_timer(phase_watch);

            chassert(timer.size() == 9);
            LOG_DEBUG(
                log,
                "Partial update cost detail for txn id {}, dedup info {}: restore block cost {} ms, concat block cost {} ms, filter duplicate keys cost {} "
                "ms, calculate key cost {} ms, search index cost {} ms, handle index cost {} ms, query history data cost {} ms(thread "
                "size: {}, query part num: {}, query total row: {}), replace column cost {} ms, generate partial part cost {} ms",
                dedup_task->txn_id,
                dedup_task->getDedupLevelInfo(),
                timer[0],
                timer[1],
                timer[2],
                timer[3],
                timer[4],
                timer[5],
                timer[6],
                read_history_part_thread_num,
                valid_query_parts_num,
                total_query_row,
                timer[7],
                timer[8]);
        }
        else
        {
            DeleteBitmapVector delta_bitmaps = dedupImpl(current_dedup_visible_parts, current_dedup_new_parts, dedup_task);

            current_iteration_bitmaps = generateNextIterationBitmaps(current_dedup_visible_parts, current_dedup_new_parts, delta_bitmaps, dedup_task->txn_id);
            current_dedup_visible_parts.insert(current_dedup_visible_parts.end(), current_dedup_new_parts.begin(), current_dedup_new_parts.end());
            /// Assign bitmaps for next sub iteration
            for (size_t i = 0; i < current_dedup_visible_parts.size(); ++i)
            {
                IMergeTreeMutableDataPartPtr temp_visible_part = std::const_pointer_cast<IMergeTreeDataPart>(current_dedup_visible_parts[i]);
                temp_visible_part->setDeleteBitmap(current_iteration_bitmaps[i]);
            }
        }
    }
    partial_update_processed_parts = current_dedup_visible_parts;
    return current_iteration_bitmaps;
}

size_t MergeTreeDataDeduper::removeDupKeysInPartialUpdateMode(
    Block & block,
    ColumnPtr version_column,
    IColumn::Filter & filter,
    PaddedPODArray<UInt32> & replace_dst_indexes,
    PaddedPODArray<UInt32> & replace_src_indexes)
{
    auto block_size = block.rows();
    size_t num_filtered = 0;
    if (block_size != filter.size())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Filter size {} doesn't match block size {}", filter.size(), block_size);

    auto metadata_snapshot = data.getInMemoryMetadataPtr();
    auto unique_key_names = metadata_snapshot->getUniqueKeyColumns();
    metadata_snapshot->getUniqueKeyExpression()->execute(block);

    ColumnsWithTypeAndName keys;
    for (auto & name : unique_key_names)
        keys.emplace_back(block.getByName(name));

    BlockUniqueKeyUnorderedComparator comparator(keys);
    BlockUniqueKeyHasher hasher(keys);
    /// first rowid of key -> rowid of the highest version of the same key
    phmap::flat_hash_map<size_t, size_t, decltype(hasher), decltype(comparator)> index(keys[0].column->size(), hasher, comparator);

    ColumnWithTypeAndName delete_flag_column;
    if (block.has(StorageInMemoryMetadata::DELETE_FLAG_COLUMN_NAME))
        delete_flag_column = block.getByName(StorageInMemoryMetadata::DELETE_FLAG_COLUMN_NAME);

    auto is_delete_row = [&](size_t rowid) { return delete_flag_column.column && delete_flag_column.column->getBool(rowid); };
    /// In the case that engine has been set version column, if version is set by user(not zero), the delete row will obey the rule of version.
    /// Otherwise, the delete row will ignore comparing version, just doing the deletion directly.
    auto delete_ignore_version
        = [&](size_t rowid) { return is_delete_row(rowid) && version_column && !version_column->getUInt(rowid); };

    /// if there are duplicated keys, only keep the highest version for each key
    for (size_t rowid = 0; rowid < block_size; ++rowid)
    {
        if (auto it = index.find(rowid); it != index.end())
        {
            /// When there is no explict version column, use rowid as version number,
            /// Otherwise use value from version column
            size_t old_pos = it->second;
            size_t new_pos = rowid;
            if (version_column && !delete_ignore_version(rowid) && version_column->getUInt(old_pos) > version_column->getUInt(new_pos))
                std::swap(old_pos, new_pos);
            else
            {
                // Only when the version of this row is larger than previous row and the row is not delete row, it will apply partial update action.
                if (!is_delete_row(new_pos) && !is_delete_row(old_pos))
                {
                    replace_dst_indexes.push_back(UInt32(new_pos));
                    replace_src_indexes.push_back(UInt32(old_pos));
                }
            }
            filter[old_pos] = 0;
            it->second = new_pos;
            num_filtered++;
        }
        else
            index[rowid] = rowid;
    }
    index.clear();

    /********************************************************************************************************
     * Find the final target row for each source row.
     * For example, block has six rows and the first column is unique key:
     * row id       data
     *    0     (1, 'a', [1, 2])
     *    1     (2, 'b', [3, 4])
     *    2     (1, 'c', [1, 2, 3])
     *    3     (1, 'd', [4, 5])
     *    4     (2, 'e', [4, 5])
     *    5     (1, 'f', [4, 5])
     * Currently, replace_dst_indexes is {2, 3, 4, 5}, replace_src_indexes is {0, 2, 1, 3}
     * Actually, the final target row for the row 0 is 5.
     * So, it needs to reverse the list and record the real target row.
     * For the result, replace_dst_indexes is {5, 4, 5, 5}, replace_src_indexes is {3, 1, 2, 0}
     ********************************************************************************************************/
    std::map<UInt32, UInt32> src_to_dst;
    PaddedPODArray<UInt32> tmp_replace_dst_indexes, tmp_replace_src_indexes;
    size_t size = replace_dst_indexes.size();
    tmp_replace_dst_indexes.reserve(size);
    tmp_replace_src_indexes.reserve(size);
    for (int i = size - 1; i >= 0; --i)
    {
        UInt32 real_dst = replace_dst_indexes[i];
        if (src_to_dst.count(replace_dst_indexes[i]))
            real_dst = src_to_dst[replace_dst_indexes[i]];
        tmp_replace_dst_indexes.push_back(real_dst);
        tmp_replace_src_indexes.push_back(replace_src_indexes[i]);
        src_to_dst[replace_src_indexes[i]] = real_dst;
    }
    replace_dst_indexes = std::move(tmp_replace_dst_indexes);
    replace_src_indexes = std::move(tmp_replace_src_indexes);

    handleCorrectPermutation(replace_dst_indexes, replace_src_indexes);
    return num_filtered;
}

SearchKeysResult MergeTreeDataDeduper::searchPartForKeys(const IMergeTreeDataPartsVector & visible_parts, UniqueKeys & keys)
{
    const IndexFile::Comparator * comparator = IndexFile::BytewiseComparator();
    SearchKeysResult ans;
    ans.resize(keys.size());

    std::sort(keys.begin(), keys.end());

    std::vector<UniqueKeyIndexPtr> key_indices;
    DeleteBitmapGetter delete_bitmap_getter = [](const IMergeTreeDataPartPtr & part) { return part->getDeleteBitmap(); };
    IndexFileIterators base_input_iters = openUniqueKeyIndexIterators(visible_parts, key_indices, /*fill_cache*/ true, delete_bitmap_getter);

    std::vector<UInt64> base_implicit_versions(visible_parts.size(), 0);
    if (version_mode == VersionMode::PartitionValueAsVersion)
    {
        for (size_t i = 0; i < visible_parts.size(); ++i)
            base_implicit_versions[i] = visible_parts[i]->getVersionFromPartition();
    }

    IndexFile::IndexFileMergeIterator base_iter(comparator, std::move(base_input_iters));
    if (!keys.empty())
        base_iter.Seek(keys[0]);

    for (size_t idx = 0; idx < keys.size(); idx++)
    {
        if (!base_iter.status().ok())
        {
            auto abnormal_part_index = base_iter.getAbnormalIndex();
            auto part_name = abnormal_part_index == -1 ? "Null" : visible_parts[abnormal_part_index]->get_name();
            throw Exception("Deduper visible parts iterator has error, part name: " + part_name + ", error: " + base_iter.status().ToString(), ErrorCodes::INCORRECT_DATA);
        }

        /// no need to read `keys` iter as following keys could not be found in visible_parts
        if (!base_iter.Valid())
            break;

        /// keys[idx] already found as keys[idx] == keys[idx - 1]
        if (idx != 0 && keys[idx] == keys[idx - 1])
            ans[idx] = ans[idx - 1];

        bool exact_match = false;
        int cmp = comparator->Compare(keys[idx], base_iter.key());
        if (cmp < 0)
            continue;
        else if (cmp > 0)
            base_iter.NextUntil(keys[idx], exact_match);
        else
            exact_match = true;

        if (exact_match)
        {
            RowPos row_pos = ReplacingSortedKeysIterator::decodeCurrentRowPos(base_iter, version_mode, visible_parts, base_implicit_versions);
            ans[idx].part_index = row_pos.child;
            ans[idx].part_rowid = row_pos.rowid;
            ans[idx].part_version = row_pos.version;
        }
    }
    return ans;
}

void MergeTreeDataDeduper::readColumnsFromStorage(
    const IMergeTreeDataPartPtr & part,
    RowidPairs & rowid_pairs,
    Block & to_block,
    PaddedPODArray<UInt32> & to_block_rowids,
    const DedupTaskPtr & dedup_task)
{
    if (rowid_pairs.empty())
        return;

    size_t block_size_before = to_block.rows();
    Stopwatch timer;
    /// sort by part_rowid so that we can read part sequentially
    std::sort(rowid_pairs.begin(), rowid_pairs.end(), [](auto & lhs, auto & rhs) { return lhs.part_rowid < rhs.part_rowid; });

    DeleteBitmapPtr delete_bitmap(new Roaring);
    for (auto & pair : rowid_pairs)
        const_cast<Roaring &>(*delete_bitmap).add(pair.part_rowid);
    const_cast<Roaring &>(*delete_bitmap).flip(0, part->rows_count);

    Names read_columns = to_block.getNames();
    auto source = std::make_unique<MergeTreeSequentialSource>(
        data,
        data.getStorageSnapshot(data.getInMemoryMetadataPtr(), context),
        part,
        delete_bitmap,
        read_columns,
        /*direct_io=*/false,
        /*take_column_types_from_storage=*/true);
    Pipes pipes;
    pipes.emplace_back(Pipe(std::move(source)));

    QueryPipeline pipeline;
    pipeline.init(Pipe::unitePipes(std::move(pipes)));

    BlockInputStreamPtr input = std::make_shared<PipelineExecutingBlockInputStream>(std::move(pipeline));
    input->readPrefix();
    while (const Block block = input->read())
    {
        if (!blocksHaveEqualStructure(to_block, block))
            throw Exception("Block structure mismatch", ErrorCodes::LOGICAL_ERROR);

        for (size_t i = 0, size = to_block.columns(); i < size; ++i)
        {
            const auto source_column = block.getByPosition(i).column;
            auto mutable_column = IColumn::mutate(std::move(to_block.getByPosition(i).column));
            mutable_column->insertRangeFrom(*source_column, 0, source_column->size());
            to_block.getByPosition(i).column = std::move(mutable_column);
        }
    }
    input->readSuffix();

    for (auto & pair : rowid_pairs)
        to_block_rowids.push_back(pair.block_rowid);

    if (to_block.rows() - block_size_before != rowid_pairs.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Block size {} is not equal to expected size {}",
            to_block.rows() - block_size_before,
            rowid_pairs.size());

    LOG_DEBUG(
        log,
        "Query for {} rows in data part {} from storage, total row {}, thread size: 1, cost {} ms, txn id: {}, dedup info: {}",
        rowid_pairs.size(),
        part->name,
        part->rows_count,
        timer.elapsedMilliseconds(),
        dedup_task->txn_id,
        dedup_task->getDedupLevelInfo());
}

void MergeTreeDataDeduper::readColumnsFromStorageParallel(
    const IMergeTreeDataPartPtr & part,
    const RowidPairs & rowid_pairs,
    Block & to_block,
    PaddedPODArray<UInt32> & to_block_rowids,
    const DedupTaskPtr & dedup_task)
{
    if (rowid_pairs.empty())
        return;

    size_t block_size_before = to_block.rows();
    Stopwatch timer;

    DeleteBitmapPtr delete_bitmap(new Roaring);
    std::unordered_map<UInt32, UInt32> part_rowid_to_block_rowid;
    for (auto & pair : rowid_pairs)
    {
        const_cast<Roaring &>(*delete_bitmap).add(pair.part_rowid);
        part_rowid_to_block_rowid[pair.part_rowid] = pair.block_rowid;
    }
    const_cast<Roaring &>(*delete_bitmap).flip(0, part->rows_count);

    Names read_columns = to_block.getNames();
    read_columns.push_back("_part_row_number");

    RangesInDataParts parts_with_ranges(1);
    {
        RangesInDataPart part_with_ranges{part, 1};
        part_with_ranges.ranges = MarkRanges{MarkRange{0, part->index_granularity.getMarksCount()}};
        parts_with_ranges[0] = std::move(part_with_ranges);
    }
    size_t sum_marks = part->index_granularity.getMarksCount();
    size_t total_rows = part->index_granularity.getTotalRows();
    size_t num_streams = std::max(static_cast<size_t>(1), static_cast<size_t>(data.getSettings()->partial_update_query_columns_thread_size));

    const auto & settings = context->getSettingsRef();
    const auto data_settings = data.getSettings();
    MergeTreeMetaBase::DeleteBitmapGetter delete_bitmap_getter = [&](const auto &) { return delete_bitmap; };
    size_t min_marks_for_concurrent_read = 0;
    {
        size_t index_granularity_bytes = 0;
        if (part->index_granularity_info.is_adaptive)
            index_granularity_bytes = data_settings->index_granularity_bytes;
        min_marks_for_concurrent_read = MergeTreeDataSelectExecutor::minMarksForConcurrentRead(
                settings.merge_tree_min_rows_for_concurrent_read,
                settings.merge_tree_min_bytes_for_concurrent_read,
                data_settings->index_granularity,
                index_granularity_bytes,
                sum_marks);
    }
    SelectQueryInfo query_info;
    auto storage_snapshot = data.getStorageSnapshot(data.getInMemoryMetadataPtr(), context);
    auto pool = std::make_shared<MergeTreeReadPool>(
        num_streams,
        sum_marks,
        min_marks_for_concurrent_read,
        std::move(parts_with_ranges),
        delete_bitmap_getter,
        data,
        storage_snapshot,
        query_info,
        true,
        read_columns,
        MergeTreeReadPool::BackoffSettings(settings),
        settings.preferred_block_size_bytes,
        false);

    Pipes pipes;
    MergeTreeStreamSettings stream_settings {
        .min_marks_for_concurrent_read = min_marks_for_concurrent_read,
        .max_block_size = settings.max_block_size,
        .preferred_block_size_bytes = settings.preferred_block_size_bytes,
        .preferred_max_column_in_block_size_bytes = settings.preferred_max_column_in_block_size_bytes,
        .use_uncompressed_cache = settings.use_uncompressed_cache,
        .actions_settings = ExpressionActionsSettings::fromContext(context)
    };
    for (size_t i = 0; i < num_streams; ++i)
    {
        auto source = std::make_shared<MergeTreeThreadSelectBlockInputProcessor>(
            i, pool, data, storage_snapshot, query_info, stream_settings);

        if (i == 0)
        {
            /// Set the approximate number of rows for the first source only
            source->addTotalRowsApprox(total_rows);
        }

        pipes.emplace_back(std::move(source));
    }

    QueryPipeline pipeline;
    pipeline.init(Pipe::unitePipes(std::move(pipes)));

    BlockInputStreamPtr input = std::make_shared<PipelineExecutingBlockInputStream>(std::move(pipeline));
    input->readPrefix();
    while (Block block = input->read())
    {
        const auto part_row_number_column = block.getByName("_part_row_number").column;
        block.erase("_part_row_number");
        if (!blocksHaveEqualStructure(to_block, block))
            throw Exception("Block structure mismatch", ErrorCodes::LOGICAL_ERROR);

        for (size_t i = 0, size = to_block.columns(); i < size; ++i)
        {
            const auto source_column = block.getByPosition(i).column;
            auto mutable_column = IColumn::mutate(std::move(to_block.getByPosition(i).column));
            mutable_column->insertRangeFrom(*source_column, 0, source_column->size());
            to_block.getByPosition(i).column = std::move(mutable_column);
        }

        for (size_t i = 0;i < part_row_number_column->size(); ++i)
        {
            UInt64 row_number =  part_row_number_column->getUInt(i);
            if (part_rowid_to_block_rowid.count(row_number))
                to_block_rowids.push_back(part_rowid_to_block_rowid[row_number]);
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not find row number {}", row_number);
        }
    }
    input->readSuffix();

    if (to_block.rows() - block_size_before != rowid_pairs.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Block size {} is not equal to expected size {}",
            to_block.rows() - block_size_before,
            rowid_pairs.size());

    LOG_DEBUG(
        log,
        "Query for {} rows in data part {} from storage, total row {}, thread size: {}, cost {} ms, txn id: {}, dedup info: {}",
        rowid_pairs.size(),
        part->name,
        part->rows_count,
        num_streams,
        timer.elapsedMilliseconds(),
        dedup_task->txn_id,
        dedup_task->getDedupLevelInfo());
}

void MergeTreeDataDeduper::replaceColumnsAndFilterData(
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
    const NameSet & same_update_column_set)
{
    size_t block_size = block.rows();
    Stopwatch timer;
    if (!block_rowids.empty())
    {
        PaddedPODArray<UInt32> tmp_src_indexes = getRowidPermutation(block_rowids);
        PaddedPODArray<UInt32> tmp_dst_indexes = permuteRowids(block_rowids, tmp_src_indexes);
        mergeIndices(replace_src_indexes, tmp_src_indexes);
        mergeIndices(replace_dst_indexes, tmp_dst_indexes);
    }

    NameSet non_updatable_columns;
    for (auto & name : data.getInMemoryMetadataPtr()->getColumnsRequiredForUniqueKey())
        non_updatable_columns.insert(name);
    for (auto & name : data.getInMemoryMetadataPtr()->getUniqueKeyColumns())
        non_updatable_columns.insert(name);
    for (auto & name: data.getInMemoryMetadataPtr()->getColumnsRequiredForPartitionKey())
        non_updatable_columns.insert(name);
    NameSet func_column_names = data.getInMemoryMetadataPtr()->getFuncColumnNames();

    std::unordered_map<String, ColumnVector<UInt8>::MutablePtr> default_filters;
    /// Pre-construct default filters based on update columns by forcing set default filter to zero if it belongs to update columns.
    if (!replace_dst_indexes.empty())
    {
        ColumnPtr part_id_column = block.getByName(StorageInMemoryMetadata::PART_ID_COLUMN).column;
        if (!part_id_column)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column part_id_column is nullptr while processing partial update.");

        ColumnPtr update_columns;
        NameSet column_need_complete_update;
        if (optimize_for_same_update_columns)
            column_need_complete_update = same_update_column_set;

        bool update_all = true;
        if (block.has(StorageInMemoryMetadata::UPDATE_COLUMNS))
        {
            update_columns = block.getByName(StorageInMemoryMetadata::UPDATE_COLUMNS).column;
            update_all = false;
            if (optimize_for_same_update_columns && column_need_complete_update.size() == 1 && column_need_complete_update.begin()->empty())
                update_all = true;
        }

        for (size_t i = 0, size = block.columns(); i < size; i++)
        {
            auto & col = block.getByPosition(i);
            /// Skip func columns
            if (non_updatable_columns.count(col.name) || func_column_names.count(col.name))
                continue;
            if (update_all || column_need_complete_update.contains(col.name) || col.name == StorageInMemoryMetadata::PART_ID_COLUMN)
            {
                auto default_filter = ColumnVector<UInt8>::create(block_size, 0);
                default_filters[col.name] = std::move(default_filter);
            }
            else
            {
                auto default_filter = ColumnVector<UInt8>::create(block_size, 1);
                default_filters[col.name] = std::move(default_filter);
            }
        }
        if (!update_all && !optimize_for_same_update_columns)
        {
            auto check_column = [&](String column) { return non_updatable_columns.count(column) || func_column_names.count(column) || column == StorageInMemoryMetadata::PART_ID_COLUMN; };
            std::vector<Names> part_column_list;
            for (const auto & part : current_dedup_new_parts)
                part_column_list.emplace_back(part->getColumnsPtr()->getNames());

            auto get_column_by_index = [&](size_t row_id, size_t index) {
                auto part_id = get<UInt64>((*part_id_column)[row_id]);
                if (part_id >= part_column_list.size())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "part_id {} is greater than current_dedup_new_parts size {}", part_id, current_dedup_new_parts.size());

                return part_column_list[part_id][index];
            };
            for (size_t i = 0; i < block_size; i++)
                parseUpdateColumns(update_columns->getDataAt(i).toString(), default_filters, check_column, get_column_by_index, i);
        }
    }

    NameSet columns_of_replace_if_not_null = data.getInMemoryMetadataPtr()->getColumns().getReplaceIfNotNullColumns();
    auto parse_update_columns_time = timer.elapsedMilliseconds();
    size_t thread_num = std::max(static_cast<size_t>(1), std::min(block.columns(), static_cast<size_t>(data.getSettings()->partial_update_replace_columns_thread_size)));
    ThreadPool replace_column_pool(thread_num);
    for (size_t i = 0; i < thread_num; ++i)
    {
        replace_column_pool.scheduleOrThrowOnError([&, i, thread_group = CurrentThread::getGroup()]() {
            SCOPE_EXIT({
                if (thread_group)
                    CurrentThread::detachQueryIfNotDetached();
            });
            if (thread_group)
                CurrentThread::attachToIfDetached(thread_group);
            setThreadName("ReplaceColumn");

            for (size_t j = i, size = block.columns(); j < size; j += thread_num)
            {
                auto & col = block.getByPosition(j);
                /// Skip func columns
                if (func_column_names.count(col.name))
                    continue;

                if (replace_dst_indexes.empty() || non_updatable_columns.count(col.name))
                {
                    if (num_filtered > 0)
                    {
                        ssize_t new_size_hint = 0;
                        try
                        {
                            new_size_hint = boost::numeric_cast<ssize_t>(block_size - num_filtered);
                        }
                        catch (...)
                        {
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "new_size_hint numeric cast error, block_size is {}, num_filtered is {}", block_size, num_filtered);
                        }
                        col.column = col.column->filter(filter, new_size_hint);
                    }
                }
                else
                {
                    ColumnPtr is_default_col;
                    if (!default_filters.count(col.name))
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not find default filter of column {} when partial_update_enable_specify_update_columns is true.", col.name);

                    if (col.type->isNullable() && (columns_of_replace_if_not_null.count(col.name) || data.getSettings()->partial_update_replace_if_not_null))
                    {
                        auto& default_filter_ref = default_filters[col.name];
                        for (size_t row = 0; row < block_size; ++row)
                        {
                            if (col.column->isNullAt(row))
                                default_filter_ref->getData()[row] = 1;
                        }
                    }
                    is_default_col = std::move(default_filters[col.name]);

                    const IColumn::Filter & is_default_filter = assert_cast<const ColumnUInt8 &>(*is_default_col).getData();

                    /// All values are non-default, nothing to replace
                    if (filterIsAlwaysFalse(is_default_filter)
                        && !(col.type->isMap() && data.getSettings()->partial_update_enable_merge_map))
                    {
                        if (num_filtered)
                        {
                            ssize_t new_size_hint = 0;
                            try
                            {
                                new_size_hint = boost::numeric_cast<ssize_t>(block_size - num_filtered);
                            }
                            catch (...)
                            {
                                throw Exception(ErrorCodes::LOGICAL_ERROR, "new_size_hint numeric cast error, block_size is {}, num_filtered is {}", block_size, num_filtered);
                            }
                            col.column = col.column->filter(filter, new_size_hint);
                        }
                        continue;
                    }

                    ColumnPtr column_from_storage = columns_from_storage.getByName(col.name).column;
                    if (!column_from_storage)
                        throw Exception(
                            ErrorCodes::LOGICAL_ERROR,
                            "Column for storage {} is nullptr while replacing column for partial update.",
                            col.name);

                    ColumnPtr new_column = col.column->replaceFrom(
                        replace_dst_indexes,
                        *column_from_storage,
                        replace_src_indexes,
                        filterIsAlwaysTrue(is_default_filter) ? nullptr : &is_default_filter,
                        num_filtered > 0 ? &filter : nullptr,
                        data.getSettings()->partial_update_enable_merge_map);
                    col.column = std::move(new_column);
                }
            }
        });
    }
    replace_column_pool.wait();
    LOG_DEBUG(
        log,
        "Replace columns and filter data cost {} ms, parse update columns const {} ms, txn id: {}, dedup info: {}",
        timer.elapsedMilliseconds(),
        parse_update_columns_time,
        dedup_task->txn_id,
        dedup_task->getDedupLevelInfo());
}

void MergeTreeDataDeduper::parseUpdateColumns(
    String columns_name,
    std::unordered_map<String, ColumnVector<UInt8>::MutablePtr> & default_filters,
    std::function<bool(String)> check_column,
    std::function<String(size_t, size_t)> get_column_by_index,
    size_t idx)
{
    size_t last_pos = 0, pos = 0, size = columns_name.size();

    if (size == 0)
    {
        /// If `_update_columns_` is empty, update all columns
        for (auto & [_, default_filter] : default_filters)
            default_filter->getData()[idx] = 0;
    }
    else
    {
        while (pos < size)
        {
            last_pos = pos;
            /// Here we will not handle special characters like space and not support regex
            while (pos < size && columns_name[pos] != ',')
                pos++;
            String item = get_column_by_index(idx, std::stoull(columns_name.substr(last_pos, pos - last_pos)));
            if (!item.empty())
            {
                if (default_filters.count(item))
                    default_filters[item]->getData()[idx] = 0;
                else if (!check_column(item))
                    LOG_WARNING(log, "Update column `{}` doesn't exist, ignore.", item);
                /// Ignore the name belongs to non_updatable_columns because it may be constructed from input header when update_columns is not specified by 'INSERT QUERY'
            }
            pos++;
        }
    }
}

IMergeTreeDataPartPtr MergeTreeDataDeduper::generateAndCommitPartInPartialUpdateMode(
    Block & block,
    const IMergeTreeDataPartsVector & new_parts,
    const DedupTaskPtr & dedup_task)
{
    MergeTreeDataWriter writer(const_cast<MergeTreeMetaBase &>(data), IStorage::StorageLocation::AUXILITY);
    CnchDataWriter cnch_writer(const_cast<MergeTreeMetaBase &>(data), context, ManipulationType::Insert, dedup_task->txn_id.toString());

    Stopwatch watch;
    UInt64 split_block_ms = 0, write_part_ms = 0, dump_part_ms = 0;

    /// Generate into one large block.
    size_t split_block_num = new_parts.size();
    chassert(split_block_num > 0);
    bool need_dump_part = block.rows() > 0;

    /// Remove column not in header
    auto header_block = data.getInMemoryMetadataPtr()->getSampleBlock();
    for (auto & col_name : block.getNames())
        if (!header_block.has(col_name))
            block.erase(col_name);

    split_block_ms = watch.elapsedMilliseconds();
    watch.restart();

    IMergeTreeDataPartsVector partial_update_parts;
    size_t write_part_thread_num
        = std::max(static_cast<size_t>(1), std::min(split_block_num, static_cast<size_t>(data.getSettings()->cnch_write_part_threads)));
    ThreadPool write_part_pool(write_part_thread_num);
    bool use_thread_pool = write_part_thread_num > 1;

    IMutableMergeTreeDataPartsVector mutable_partial_update_parts{split_block_num};
    auto generate_drop_part = [&](size_t split_block_id) {
        auto drop_part_info = new_parts[split_block_id]->info;
        drop_part_info.level += 1;
        drop_part_info.hint_mutation = 0;
        drop_part_info.mutation = context->getCurrentTransactionID().toUInt64();
        auto disk = data.getStoragePolicy(IStorage::StorageLocation::AUXILITY)->getAnyDisk();
        auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + drop_part_info.getPartName(), disk);

        auto drop_part = std::make_shared<MergeTreeDataPartCNCH>(
            data, drop_part_info.getPartName(), drop_part_info, single_disk_volume, std::nullopt);

        drop_part->partition = std::move(new_parts[split_block_id]->partition);
        drop_part->bucket_number = new_parts[split_block_id]->bucket_number;
        drop_part->deleted = true;
        drop_part->partial_update_state = PartialUpdateState::RWProcessFinished;

        mutable_partial_update_parts[split_block_id] = drop_part;
    };

    for (size_t i = 0; i < split_block_num; ++i)
    {
        if (use_thread_pool)
            write_part_pool.scheduleOrThrowOnError([&, i, thread_group = CurrentThread::getGroup()]() {
                SCOPE_EXIT({
                    if (thread_group)
                        CurrentThread::detachQueryIfNotDetached();
                });
                if (thread_group)
                    CurrentThread::attachToIfDetached(thread_group);
                setThreadName("GenerateDropPart");
                generate_drop_part(i);
            });
        else
            generate_drop_part(i);
    }
    if (use_thread_pool)
        write_part_pool.wait();
    write_part_ms = watch.elapsedMilliseconds();
    watch.restart();

    if (need_dump_part)
    {
        BlockWithPartition partial_processed_block{Block(block), Row(new_parts[0]->partition.value)};
        partial_processed_block.bucket_info.bucket_number = new_parts[0]->bucket_number;
        auto new_part_generated = writer.writeTempPart(
            partial_processed_block,
            data.getInMemoryMetadataPtr(),
            context,
            context->getTimestamp(),
            context->getCurrentTransactionID().toUInt64(),
            /*hint_mutation*/0,
            /*partial_update_state*/PartialUpdateState::RWProcessFinished);

        mutable_partial_update_parts.push_back(new_part_generated);
        LOG_TRACE(
            log,
            "New part name generated: {}, part size: {}, txn id: {}, dedup info: {}",
            new_part_generated->name,
            block.rows(),
            dedup_task->txn_id,
            dedup_task->getDedupLevelInfo());
    }
    auto dumped_data = cnch_writer.dumpAndCommitCnchParts(mutable_partial_update_parts);
    for (size_t i = 0; i < split_block_num; ++i)
    {
        dumped_data.parts[i]->setPreviousPart(new_parts[i]);
        partial_update_parts.push_back(dumped_data.parts[i]);
    }
    if (need_dump_part)
        partial_update_parts[0] = dumped_data.parts[split_block_num];

    dump_part_ms = watch.elapsedMilliseconds();
    LOG_DEBUG(
        log,
        "Commit partial update part detail: split block cost {} ms, write part cost {} ms, thread pool size is {}, dump part cost {} ms, "
        "txn id: {}, dedup info: {}",
        split_block_ms,
        write_part_ms,
        write_part_thread_num,
        dump_part_ms,
        dedup_task->txn_id,
        dedup_task->getDedupLevelInfo());

    return partial_update_parts[0];
}

DeleteBitmapVector
MergeTreeDataDeduper::dedupImpl(const IMergeTreeDataPartsVector & visible_parts, const IMergeTreeDataPartsVector & new_parts, DedupTaskPtr & dedup_task)
{
    if (new_parts.empty())
        throw Exception("Delta part is empty when MergeTreeDataDeduper handles dedup task.", ErrorCodes::LOGICAL_ERROR);

    std::vector<UniqueKeyIndexPtr> new_part_indices;
    DeleteBitmapGetter delete_bitmap_getter = [](const IMergeTreeDataPartPtr & part) -> ImmutableDeleteBitmapPtr {
        if (!part->delete_flag)
            return part->getDeleteBitmap(/*allow_null*/ true);
        return nullptr;
    };
    IndexFileIterators input_iters
        = openUniqueKeyIndexIterators(new_parts, new_part_indices, /*fill_cache*/ true, delete_bitmap_getter);

    DeleteBitmapVector res(visible_parts.size() + new_parts.size());
    DeleteCallback cb = [start = visible_parts.size(), &res](const RowPos & pos) { addRowIdToBitmap(res[start + pos.child], pos.rowid); };

    ImmutableDeleteBitmapVector delete_flag_bitmaps(new_parts.size());
    for (size_t i = 0; i < new_parts.size(); ++i)
    {
        if (new_parts[i]->delete_flag)
            delete_flag_bitmaps[i] = new_parts[i]->getDeleteBitmap(/*allow_null*/ true);
    }
    {
        std::lock_guard lock(dedup_tasks_mutex);
        dedup_task->iter = std::make_shared<ReplacingSortedKeysIterator>(IndexFile::BytewiseComparator(), new_parts, std::move(input_iters), cb, version_mode, delete_flag_bitmaps);
    }

    auto task_progress_reporter = [&] ()
    {
        WriteBufferFromOwnString os;
        os << "Dedup task in txn_id: " << dedup_task->txn_id.toUInt64() << ", progress info: " << dedup_task->getDedupTaskProgress();
        return os.str();
    };

    dedupKeysWithParts(dedup_task->iter, visible_parts, res, task_progress_reporter, dedup_task);
    return res;
}

DeleteBitmapVector MergeTreeDataDeduper::repairImpl(const IMergeTreeDataPartsVector & parts)
{
    if (parts.empty())
        return {};

    std::vector<UniqueKeyIndexPtr> key_indices;
    DeleteBitmapGetter delete_bitmap_getter = [](const IMergeTreeDataPartPtr & part) { return part->getDeleteBitmap(/*allow_null*/ true); };
    IndexFileIterators input_iters = openUniqueKeyIndexIterators(parts, key_indices, /*fill_cache*/ false, delete_bitmap_getter);

    DeleteBitmapVector res(parts.size());
    DeleteCallback cb = [&res](const RowPos & pos) { addRowIdToBitmap(res[pos.child], pos.rowid); };
    ReplacingSortedKeysIterator keys_iter(IndexFile::BytewiseComparator(), parts, std::move(input_iters), cb, version_mode);
    keys_iter.SeekToFirst();
    while (keys_iter.Valid())
        // delete_flag_bitmaps is default initialized
        // coverity[use_invalid_in_call]
        keys_iter.Next();
    return res;
}

Names MergeTreeDataDeduper::getTasksProgress()
{
    Names res;
    std::lock_guard lock(dedup_tasks_mutex);
    for (auto & dedup_task : dedup_tasks)
        res.emplace_back(dedup_task->getDedupTaskProgress());
    return res;
}

}
