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

#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/CnchSystemLog.h>
#include <MergeTreeCommon/MergeTreeDataDeduper.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Storages/IndexFile/IndexFileMergeIterator.h>
#include <Storages/UniqueKeyIndex.h>
#include <Common/Coding.h>
#include <CloudServices/CnchDedupHelper.h>

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
    : data(data_), context(context_), log(&Poco::Logger::get(data_.getLogName() + " (Deduper)")), dedup_mode(dedup_mode_)
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
    auto prepare_bitmaps_to_dump = [txn_id, this, &res, log = this->log](
                                       const IMergeTreeDataPartsVector & visible_parts,
                                       const IMergeTreeDataPartsVector & new_parts,
                                       const DeleteBitmapVector & bitmaps) -> size_t {
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
                    UInt64 bitmap_version = new_parts[i - visible_parts.size()]->getDeleteBitmapVersion();
                    if (bitmap_version == txn_id.toUInt64())
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
    };

    auto log_dedup_detail = [&](const DedupTaskPtr & task, const IMergeTreeDataPartsVector & visible_parts, const IMergeTreeDataPartsVector & new_parts) {
        WriteBufferFromOwnString msg;
        msg << "Start to dedup in txn_id: " << txn_id.toUInt64() << ", dedup level info: " << task->getDedupLevelInfo() << ", visible_parts: [";
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
    };

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

        dedup_pool.scheduleOrThrowOnError([&, i]() {
            DedupTaskPtr dedup_task_local;
            {
                std::lock_guard lock(dedup_tasks_mutex);
                dedup_task_local = dedup_tasks[i];
            }
            Stopwatch sub_task_watch;
            auto & visible_parts = dedup_task_local->visible_parts;
            auto & new_parts = dedup_task_local->new_parts;
            log_dedup_detail(dedup_task_local, visible_parts, new_parts);
            DeleteBitmapVector bitmaps = dedupImpl(visible_parts, new_parts, dedup_task_local);

            std::lock_guard lock(mutex);
            size_t num_bitmaps_to_dump = prepare_bitmaps_to_dump(visible_parts, new_parts, bitmaps);
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

        dedup_pool.scheduleOrThrowOnError([&]() {
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
