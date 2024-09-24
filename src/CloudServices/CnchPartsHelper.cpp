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

#include <functional>
#include <sstream>
#include <Catalog/CatalogUtils.h>
#include <Catalog/DataModelPartWrapper.h>
#include <CloudServices/CnchPartsHelper.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <common/defines.h>

namespace DB
{
std::atomic<UInt64> MinimumDataPart::increment = 0;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace DB::CnchPartsHelper
{
LoggingOption getLoggingOption(const Context & c)
{
    return (c.getSettingsRef().send_logs_level < LogsLevel::debug) ? DisableLogging : EnableLogging;
}

IMergeTreeDataPartsVector toIMergeTreeDataPartsVector(const MergeTreeDataPartsCNCHVector & vec)
{
    IMergeTreeDataPartsVector res;
    res.reserve(vec.size());
    for (const auto & p : vec)
        res.push_back(p);
    return res;
}

MergeTreeDataPartsCNCHVector toMergeTreeDataPartsCNCHVector(const IMergeTreeDataPartsVector & vec)
{
    MergeTreeDataPartsCNCHVector res;
    res.reserve(vec.size());
    for (const auto & p : vec)
        res.push_back(dynamic_pointer_cast<const MergeTreeDataPartCNCH>(p));
    return res;
}

namespace
{
    struct ServerDataPartOperation
    {
        static String getName(const ServerDataPartPtr & part) { return part->name(); }

        static Int64 getMaxBlockNumber(const ServerDataPartPtr & part) { return part->get_info().max_block; }

        static UInt64 getCommitTime(const ServerDataPartPtr & part) { return part->get_commit_time(); }

        static UInt64 getEndTime(const ServerDataPartPtr & part) { return part->getEndTime(); }

        static void setEndTime(const ServerDataPartPtr & part, UInt64 end_time)
        {
            if (!end_time)
                return;
            /// TODO: need change to support multiple base parts in MVCC chain
            for (auto curr = part; curr; curr = curr->tryGetPreviousPart())
                curr->setEndTime(end_time);
        }

        /// note that range tombstone is also a tombstone
        static bool isTombstone(const ServerDataPartPtr & part) { return part->get_deleted(); }

        static bool isRangeTombstone(const ServerDataPartPtr & part) { return part->get_info().level == MergeTreePartInfo::MAX_LEVEL; }

        static bool isSamePartition(const ServerDataPartPtr & lhs, const ServerDataPartPtr & rhs)
        {
            return lhs->get_info().partition_id == rhs->get_info().partition_id;
        }

        /// returns whether `lhs` is previous version of `rhs`.
        static bool isPreviousOf(const ServerDataPartPtr & lhs, const ServerDataPartPtr & rhs) { return rhs->containsExactly(*lhs); }

        static void setPrevious(const ServerDataPartPtr & lhs, const ServerDataPartPtr & rhs) { lhs->setPreviousPart(rhs); }

        static const ServerDataPartPtr & tryGetPrevious(const ServerDataPartPtr & part) { return part->tryGetPreviousPart(); }
    };

    struct MinimumDataPartOperation
    {
        static String getName(const MinimumDataPartPtr & part) { return part->name; }

        static Int64 getMaxBlockNumber(const MinimumDataPartPtr & part) { return part->info.max_block; }

        static UInt64 getCommitTime(const MinimumDataPartPtr & part) { return part->commit_time; }

        static UInt64 getEndTime(const MinimumDataPartPtr & part) { return part->end_time; }

        static void setEndTime(const MinimumDataPartPtr & part, UInt64 end_time)
        {
            if (!end_time)
                return;
            /// TODO: need change to support multiple base parts in MVCC chain
            for (auto curr = part; curr; curr = curr->prev)
                curr->end_time = end_time;
        }

        /// note that range tombstone is also a tombstone
        static bool isTombstone(const MinimumDataPartPtr & part) { return part->is_deleted; }

        static bool isRangeTombstone(const MinimumDataPartPtr & part) { return part->info.level == MergeTreePartInfo::MAX_LEVEL; }

        static bool isSamePartition(const MinimumDataPartPtr & lhs, const MinimumDataPartPtr & rhs)
        {
            return lhs->info.partition_id == rhs->info.partition_id;
        }

        /// returns whether `lhs` is previous version of `rhs`.
        static bool isPreviousOf(const MinimumDataPartPtr & lhs, const MinimumDataPartPtr & rhs) { return rhs->containsExactly(*lhs); }

        static void setPrevious(const MinimumDataPartPtr & lhs, const MinimumDataPartPtr & rhs) { lhs->prev = rhs; }

        static const MinimumDataPartPtr & tryGetPrevious(const MinimumDataPartPtr & part) { return part->prev; }
    };

    struct DeleteBitmapOperation
    {
        static String getName(const DeleteBitmapMetaPtr & bitmap) { return bitmap->getNameForLogs(); }

        static Int64 getMaxBlockNumber(const DeleteBitmapMetaPtr & bitmap) { return bitmap->getModel()->part_max_block(); }

        static UInt64 getCommitTime(const DeleteBitmapMetaPtr & bitmap) { return bitmap->getCommitTime(); }

        static UInt64 getEndTime(const DeleteBitmapMetaPtr & bitmap) { return bitmap->getEndTime(); }

        static void setEndTime(const DeleteBitmapMetaPtr & bitmap, UInt64 end_time)
        {
            /// a bitmap's end time is equal to the commit time of the first non-partial bitmap that covers it.
            /// e.g., for the following mvvc chain ("->" means next version)
            ///   base(commit=t1) -> delta(commit=t2) -> delta(commit=t3) -> base(commit=t4) -> base(commit=t5) -> delta(commit=t6)
            /// the end time for each bitmaps are updated to
            ///   base(end=t4)    -> delta(end=t4)    -> delta(end=t4)    -> base(end=t5)    -> base(end=0)     -> delta(end=0)
            for (auto it = bitmap; it; it = it->tryGetPrevious())
            {
                if (end_time)
                    it->setEndTime(end_time);
                if (!it->isPartial())
                    end_time = it->getCommitTime();
            }
        }

        static bool isTombstone(const DeleteBitmapMetaPtr & bitmap) { return bitmap->isTombstone(); }

        static bool isRangeTombstone(const DeleteBitmapMetaPtr & bitmap) { return bitmap->isRangeTombstone(); }

        static bool isSamePartition(const DeleteBitmapMetaPtr & lhs, const DeleteBitmapMetaPtr & rhs)
        {
            return lhs->getPartitionID() == rhs->getPartitionID();
        }

        /// returns whether `lhs` is previous version of `rhs`.
        static bool isPreviousOf(const DeleteBitmapMetaPtr & lhs, const DeleteBitmapMetaPtr & rhs)
        {
            return lhs->sameBlock(*rhs) && lhs->getCommitTime() < rhs->getCommitTime();
        }

        static void setPrevious(const DeleteBitmapMetaPtr & lhs, const DeleteBitmapMetaPtr & rhs) { lhs->setPrevious(rhs); }

        static const DeleteBitmapMetaPtr & tryGetPrevious(const DeleteBitmapMetaPtr & bitmap) { return bitmap->tryGetPrevious(); }
    };

    template <class Vec, typename Operation, typename Comparator>
    void calcForGCImpl(Vec & all_items, Vec * out_to_gc, Vec * out_visible_items)
    {
        chassert(out_to_gc || out_visible_items);

        if (all_items.empty())
            return;

        auto output = [&](auto & elem)
        {
            if (out_to_gc)
            {
                for (auto item = elem; item; item = Operation::tryGetPrevious(item))
                {
                    if (Operation::getEndTime(item))
                        out_to_gc->push_back(item);
                }
            }
            if (out_visible_items)
                out_visible_items->push_back(elem);
        };

        if (all_items.size() == 1)
        {
            auto & item = all_items.front();
            /// set end time for alone tombstone
            if (Operation::isTombstone(item) && !Operation::getEndTime(item))
                Operation::setEndTime(item, Operation::getCommitTime(item));
            output(item);
            return;
        }

        std::sort(all_items.begin(), all_items.end(), Comparator{});

        auto prev_it = all_items.begin();
        auto curr_it = std::next(prev_it);
        auto end = all_items.end();

        /// if non-end, point to the first and last + 1 range tombstone item for a particular partition
        auto range_tombstone_beg_it = end;
        auto range_tombstone_end_it = end;
        /// if non-zero, it's the smallest max_block_number of non-tombstone items for a particular partition
        Int64 min_visible_block_number = 0;

        while (prev_it != end)
        {
            auto & prev = *prev_it;
            if (!Operation::getCommitTime(prev))
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Unexpected input, object {} doesn't have commit time", Operation::getName(prev));
            }

            bool reach_partition_end = (curr_it == end || !Operation::isSamePartition(prev, *curr_it));
            const auto max_block_number = Operation::getMaxBlockNumber(prev);

            /// update min_visible_block_number
            if (!Operation::isRangeTombstone(prev))
                min_visible_block_number = min_visible_block_number ? std::min(min_visible_block_number, max_block_number) : max_block_number;

            if (Operation::isRangeTombstone(prev))
            {
                /// Sort will place range tombstones consecutively at the beginning of each partition.
                /// We'll record theri boundaries during iteration and process them when reaching partition end
                if (range_tombstone_beg_it == end)
                    range_tombstone_beg_it = prev_it;
                range_tombstone_end_it = std::next(prev_it);
            }
            /// find the latest version of MVCC items
            else if (curr_it != end && Operation::isPreviousOf(prev, (*curr_it)))
            {
                Operation::setPrevious((*curr_it), prev);
            }
            /// single tombstone
            else if (Operation::isTombstone(prev))
            {
                if (auto covered = Operation::tryGetPrevious(prev))
                    Operation::setEndTime(covered, Operation::getCommitTime(prev));
                else
                    Operation::setEndTime(prev, Operation::getCommitTime(prev));
                output(prev);
            }
            /// latest version of non-tombstone item
            else
            {
                UInt64 end_time = 0;
                /// find the first covering range tombstone for prev
                for (auto it = range_tombstone_beg_it; it != range_tombstone_end_it; ++it)
                {
                    if (max_block_number <= Operation::getMaxBlockNumber(*it))
                    {
                        end_time = Operation::getCommitTime(*it);
                        break;
                    }
                }
                /// NOTE: mvcc-chain of bitmaps can have multiple base(full) versions,
                /// and `setEndTime(prev, 0)` is required in order to gc old versions
                /// covered by new base bitmap. for parts, it's a no-op when end_time is 0.
                Operation::setEndTime(prev, end_time);
                output(prev);
            }

            prev_it = curr_it;
            if (curr_it != end)
                ++curr_it;

            if (reach_partition_end)
            {
                for (auto it = range_tombstone_beg_it; it != range_tombstone_end_it; ++it)
                {
                    /// set end time for range tombstone iff all covered items have been moved to trash
                    if (!min_visible_block_number || min_visible_block_number > Operation::getMaxBlockNumber(*it))
                        Operation::setEndTime(*it, Operation::getCommitTime(*it));
                    output(*it);
                }
                range_tombstone_beg_it = range_tombstone_end_it = end;
                min_visible_block_number = 0;
            }
        }
    }

} /// anonymouse namespace

namespace
{
    template <class T>
    std::string partsToDebugString(const std::vector<T> & parts)
    {
        std::ostringstream oss;
        for (auto & p : parts)
            oss << p->get_name() << " d=" << p->get_deleted() << " p=" << bool(p->get_info().hint_mutation)
                << " h=" << p->get_info().hint_mutation << '\n';
        return oss.str();
    }

    /** Cnch parts classification:
     *  all parts:
     *  1) invisible parts
     *      a) covered by new version parts
     *      b) covered by DROP_RANGE  (invisible_dropped_parts)
     *  2) visible parts
     *      a) with data
     *      b) without data, i.e. DROP_RANGE
     *          i) alone  (visible_alone_drop_ranges)
     *          ii) not alone
     */

    template <class Vec>
    Vec calcVisiblePartsImpl(
        Vec & all_parts,
        bool flatten,
        bool skip_drop_ranges,
        Vec * visible_alone_drop_ranges,
        Vec * invisible_dropped_parts,
        LoggingOption logging)
    {
        using Part = typename Vec::value_type;

        Vec visible_parts;

        if (all_parts.empty())
            return visible_parts;

        if (all_parts.size() == 1)
        {
            if (skip_drop_ranges && all_parts.front()->get_deleted())
                ; /// do nothing
            else
                visible_parts = all_parts;

            if (visible_alone_drop_ranges && all_parts.front()->get_deleted())
                *visible_alone_drop_ranges = all_parts;
            return visible_parts;
        }

        auto process_parts = [&](Vec & parts, size_t begin_pos, size_t end_pos, Vec & visible_parts_)
        {
            std::sort(parts.begin() + begin_pos, parts.begin() + end_pos, PartComparator<Part>{});

            /// One-pass algorithm to construct delta chains
            auto prev_it = parts.begin() + begin_pos;
            auto curr_it = std::next(prev_it);
            auto parts_end = parts.begin() + end_pos;

            while (prev_it != parts_end)
            {
                auto & prev_part = *prev_it;

                /// 1. prev_part is a DROP RANGE mark
                if (prev_part->get_info().level == MergeTreePartInfo::MAX_LEVEL)
                {
                    /// a. curr_part is in same partition
                    if (curr_it != parts_end && prev_part->get_info().partition_id == (*curr_it)->get_info().partition_id)
                    {
                        /// i) curr_part is also a DROP RANGE mark, and must be the bigger one
                        if ((*curr_it)->get_info().level == MergeTreePartInfo::MAX_LEVEL)
                        {
                            if (invisible_dropped_parts)
                                invisible_dropped_parts->push_back(*prev_it);

                            if (visible_alone_drop_ranges)
                            {
                                (*prev_it)->setPreviousPart(nullptr); /// reset whatever
                                (*curr_it)->setPreviousPart(*prev_it); /// set previous part for visible_alone_drop_ranges
                            }

                            prev_it = curr_it;
                            ++curr_it;
                            continue;
                        }
                        /// ii) curr_part is marked as dropped by prev_part
                        else if ((*curr_it)->get_info().max_block <= prev_part->get_info().max_block)
                        {
                            if (invisible_dropped_parts)
                                invisible_dropped_parts->push_back(*curr_it);

                            if (visible_alone_drop_ranges)
                                prev_part->setPreviousPart(*curr_it); /// set previous part for visible_alone_drop_ranges

                            ++curr_it;
                            continue;
                        }
                    }

                    /// a. iii) [fallthrough] same partition, but curr_part is a new part with data after the DROP RANGE mark

                    /// b) curr_it is in the end
                    /// c) different partition

                    if (skip_drop_ranges)
                        ; /// do nothing
                    else
                        visible_parts_.push_back(prev_part);

                    if (visible_alone_drop_ranges && !prev_part->tryGetPreviousPart())
                        visible_alone_drop_ranges->push_back(prev_part);
                    prev_part->setPreviousPart(nullptr);
                }
                /// 2. curr_part contains the prev_part
                else if (curr_it != parts_end && (*curr_it)->containsExactly(*prev_part))
                {
                    (*curr_it)->setPreviousPart(prev_part);
                }
                /// 3. curr_it is in the end
                /// 4. curr_part is not related to the prev_part which means prev_part must be visible
                else
                {
                    if (skip_drop_ranges && prev_part->get_deleted())
                        ; /// do nothing
                    else
                        visible_parts_.push_back(prev_part);

                    if (visible_alone_drop_ranges && !prev_part->tryGetPreviousPart() && prev_part->get_deleted())
                        visible_alone_drop_ranges->push_back(prev_part);
                }

                prev_it = curr_it;
                if (curr_it != parts_end)
                    ++curr_it;
            }
        };

        /// Try to process parts partition by partition, since this could decrease time complexity from N*log(N) to N*log(P)
        /// First check whether parts are partition aligned and partition sorted
        /// and record the partition parts_begin and parts_end positions
        bool partition_aligned = true;
        bool partition_sorted = true;
        std::vector<size_t> partition_pos;
        std::unordered_set<String> partition_ids;
        /// Have a quick check if there is only one partition to avoid overhead
        if ((*all_parts.begin())->get_info().partition_id == (*all_parts.rbegin())->get_info().partition_id)
        {
            partition_aligned = false;
        }
        else
        {
            partition_pos.push_back(0);
            for (size_t i = 1; i < all_parts.size(); ++i)
            {
                const String & prev_partition_id = all_parts[i-1]->get_info().partition_id;
                const String & partition_id = all_parts[i]->get_info().partition_id;
                if (prev_partition_id < partition_id)
                {
                    if (partition_ids.contains(partition_id))
                    {
                        partition_aligned = false;
                        break;
                    }
                    partition_pos.push_back(i);
                    partition_ids.insert(partition_id);
                }
                else if (prev_partition_id > partition_id)
                {
                    partition_sorted = false;
                    break;
                }
            }
        }

        if (partition_aligned)
            partition_pos.push_back(all_parts.size());

        /// If parts are not partition aligned or partition sorted, could not do partial sort
        /// Since needs to make sure the return parts are sorted
        if (!partition_sorted || !partition_aligned)
        {
            if (!partition_sorted)
                LOG_WARNING(&Poco::Logger::get(__func__), "parts are not partition sorted, this could make calcVisible slow");
            else if (partition_ids.size() > 1)
                LOG_WARNING(&Poco::Logger::get(__func__), "parts are not partition aligned, this could make calcVisible slow");
            process_parts(all_parts, 0, all_parts.size(), visible_parts);
        }
        else
        {
            size_t max_threads = std::min(Catalog::getMaxThreads(), partition_ids.size());
            if (max_threads < 2 || all_parts.size() < Catalog::getMinParts())
            {
                for (size_t i = 0; i < partition_pos.size() - 1; ++i)
                    process_parts(all_parts, partition_pos[i], partition_pos[i+1], visible_parts);
            }
            else
            {
                std::vector<Vec> partition_visible_parts;
                partition_visible_parts.resize(partition_pos.size() - 1);
                ExceptionHandler exception_handler;
                ThreadPool thread_pool(max_threads);
                for (size_t i = 0; i < partition_pos.size() - 1; ++i)
                {
                    thread_pool.scheduleOrThrowOnError(createExceptionHandledJob(
                        [&, i]() {  process_parts(all_parts, partition_pos[i], partition_pos[i+1], partition_visible_parts[i]); },
                        exception_handler));
                }
                thread_pool.wait();
                exception_handler.throwIfException();

                size_t total_visible_parts_number = 0;
                for (auto & parts : partition_visible_parts)
                    total_visible_parts_number += parts.size();
                visible_parts.reserve(total_visible_parts_number);
                for (auto & parts : partition_visible_parts)
                    visible_parts.insert(visible_parts.end(), std::make_move_iterator(parts.begin()), std::make_move_iterator(parts.end()));
            }
        }

        if (flatten)
            flattenPartsVector(visible_parts);

        if (logging == EnableLogging)
        {
            auto log = &Poco::Logger::get(__func__);
            LOG_DEBUG(log, "all_parts:\n {}", partsToDebugString(all_parts));
            LOG_DEBUG(log, "visible_parts (skip_drop_ranges={}):\n{}", skip_drop_ranges, partsToDebugString(visible_parts));
            if (visible_alone_drop_ranges)
                LOG_DEBUG(log, "visible_alone_drop_ranges:\n{}", partsToDebugString(*visible_alone_drop_ranges));
            if (invisible_dropped_parts)
                LOG_DEBUG(log, "invisible_dropped_parts:\n{}", partsToDebugString(*invisible_dropped_parts));
        }

        return visible_parts;
    }

} /// end of namespace

MergeTreeDataPartsVector calcVisibleParts(MergeTreeDataPartsVector & all_parts, bool flatten, LoggingOption logging)
{
    return calcVisiblePartsImpl<MergeTreeDataPartsVector>(all_parts, flatten, /* skip_drop_ranges */ true, nullptr, nullptr, logging);
}

ServerDataPartsVector calcVisibleParts(ServerDataPartsVector & all_parts, bool flatten, LoggingOption logging)
{
    return calcVisiblePartsImpl<ServerDataPartsVector>(all_parts, flatten, /* skip_drop_ranges */ true, nullptr, nullptr, logging);
}

MergeTreeDataPartsCNCHVector calcVisibleParts(MergeTreeDataPartsCNCHVector & all_parts, bool flatten, LoggingOption logging)
{
    return calcVisiblePartsImpl<MergeTreeDataPartsCNCHVector>(all_parts, flatten, /* skip_drop_ranges */ true, nullptr, nullptr, logging);
}

IMergeTreeDataPartsVector calcVisibleParts(IMergeTreeDataPartsVector& all_parts,
    bool collect_on_chain, bool skip_drop_ranges, IMergeTreeDataPartsVector* visible_alone_drop_ranges,
    IMergeTreeDataPartsVector* invisible_dropped_parts, LoggingOption logging)
{
    return calcVisiblePartsImpl(all_parts, collect_on_chain, skip_drop_ranges,
        visible_alone_drop_ranges, invisible_dropped_parts, logging);
}

ServerDataPartsVector calcVisibleParts(ServerDataPartsVector& all_parts,
    bool collect_on_chain, bool skip_drop_ranges, ServerDataPartsVector* visible_alone_drop_ranges,
    ServerDataPartsVector* invisible_dropped_parts, LoggingOption logging)
{
    return calcVisiblePartsImpl(all_parts, collect_on_chain,
        skip_drop_ranges, visible_alone_drop_ranges, invisible_dropped_parts,
        logging);
}

void calcPartsForGC(ServerDataPartsVector & all_parts, ServerDataPartsVector * out_parts_to_gc, ServerDataPartsVector * out_visible_parts)
{
    return calcForGCImpl<ServerDataPartsVector, ServerDataPartOperation, PartComparator<ServerDataPartPtr>>(
        all_parts, out_parts_to_gc, out_visible_parts);
}

void calcMinimumPartsForGC(MinimumDataParts & all_parts, MinimumDataParts * out_parts_to_gc, MinimumDataParts * out_visible_parts)
{
    return calcForGCImpl<MinimumDataParts, MinimumDataPartOperation, PartComparator<MinimumDataPartPtr>>(
        all_parts, out_parts_to_gc, out_visible_parts);
}

void calcBitmapsForGC(
    DeleteBitmapMetaPtrVector & all_bitmaps, DeleteBitmapMetaPtrVector * out_bitmaps_to_gc, DeleteBitmapMetaPtrVector * out_visible_bitmaps)
{
    return calcForGCImpl<DeleteBitmapMetaPtrVector, DeleteBitmapOperation, LessDeleteBitmapMeta>(
        all_bitmaps, out_bitmaps_to_gc, out_visible_bitmaps);
}

/// Input
/// - all_bitmaps: list of bitmaps that have been committed (has non-zero CommitTs)
/// - include_tombstone: whether to include tombstone bitmaps in "visible_bitmaps"
/// Output
/// - visible_bitmaps: contains the latest version for all visible bitmaps
/// - visible_alone_tombstones: contains visible tombstone bitmaps that don't have previous version
/// - bitmaps_covered_by_range_tombstones: contains all bitmaps covered by range tombstones
///
/// Example:
///
/// We notate each bitmap as "PartitionID_MinBlock_MaxBlock_Type_CommitTs" below, where Type is one of
/// - 'b' for base bitmap
/// - 'd' for delta bitmap
/// - 'D' for single tombstone
/// - 'R' for range tombstone
///
/// Suppose we have the following inputs (all_bitmaps)
///     all_0_0_D_t0 (previous version may have been removed by gc thread),
///     all_1_1_b_t1, all_2_2_b_t1, all_3_3_b_t1,
///     all_1_1_D_t2, all_2_2_D_t2, all_1_2_b_t2,
///     all_1_2_d_t3, all_4_4_b_t4
///
/// The function will link bitmaps with the same block name (PartitionID_MinBlock_MaxBlock), from new to old.
/// If there are no range tombstones, all bitmaps are visible and the latest version for each bitmap is added to "visible_bitmaps".
///
/// Output when include_tombstone == true:
///     visible_bitmaps = [
///         all_0_0_D_t0,
///         all_1_1_D_t2 (-> all_1_1_b_t1),
///         all_1_2_d_t3 (-> all_1_2_b_t2),
///         all_2_2_D_t2 (-> all_2_2_b_t1),
///         all_3_3_b_t1,
///         all_4_4_b_t4
///     ]
///     visible_alone_tombstones (if not null) = [ all_0_0_D_t0 ]
///
/// Output when include_tombstone == false:
///     visible_bitmaps = [
///         all_1_2_d_t3 (-> all_1_2_b_t2),
///         all_3_3_b_t1,
///         all_4_4_b_t4
///     ]
///
/// If we add a range tombstone all_0_3_R_t2 to the input, all bitmaps in the same partition whose MaxBlock <= 3
/// are covered by the range tombstone and become invisible, so they are not included in "visible_bitmaps".
///
/// Output:
///     visible_bitmaps = [ all_4_4_b_t4 ],
///     bitmaps_covered_by_range_tombstones (if not null) = [
///         all_0_0_D_t0,
///         all_1_1_b_t1, all_2_2_b_t1, all_3_3_b_t1,
///         all_1_1_D_t2, all_2_2_D_t2, all_1_2_b_t2,
///         all_1_2_d_t3
///     ]
void calcVisibleDeleteBitmaps(
    DeleteBitmapMetaPtrVector & all_bitmaps,
    DeleteBitmapMetaPtrVector & visible_bitmaps,
    bool include_tombstone,
    DeleteBitmapMetaPtrVector * visible_alone_tombstones,
    DeleteBitmapMetaPtrVector * bitmaps_covered_by_range_tombstones)
{
    if (all_bitmaps.empty())
        return;
    if (all_bitmaps.size() == 1)
    {
        if (include_tombstone || !all_bitmaps.front()->isTombstone())
            visible_bitmaps = all_bitmaps;

        if (visible_alone_tombstones && all_bitmaps.front()->isTombstone())
            *visible_alone_tombstones = all_bitmaps;
        return;
    }

    std::sort(all_bitmaps.begin(), all_bitmaps.end(), LessDeleteBitmapMeta());

    auto prev_it = all_bitmaps.begin();
    auto curr_it = std::next(prev_it);

    while (prev_it != all_bitmaps.end())
    {
        auto & prev = *prev_it;
        if (prev->isRangeTombstone() && curr_it != all_bitmaps.end()
            && prev->getModel()->partition_id() == (*curr_it)->getModel()->partition_id())
        {
            if ((*curr_it)->isRangeTombstone())
            {
                if (bitmaps_covered_by_range_tombstones)
                    bitmaps_covered_by_range_tombstones->push_back(prev);
                prev_it = curr_it;
                ++curr_it;
                continue;
            }
            else if ((*curr_it)->getModel()->part_max_block() <= prev->getModel()->part_max_block())
            {
                if (bitmaps_covered_by_range_tombstones)
                    bitmaps_covered_by_range_tombstones->push_back(*curr_it);
                ++curr_it;
                continue;
            }
        }

        if (curr_it != all_bitmaps.end() && (*curr_it)->sameBlock(*prev))
        {
            (*curr_it)->setPrevious(prev);
        }
        else
        {
            if (include_tombstone || !prev->isTombstone())
                visible_bitmaps.push_back(prev);

            if (visible_alone_tombstones && prev->isTombstone() && !prev->tryGetPrevious())
                visible_alone_tombstones->push_back(prev);
        }

        prev_it = curr_it;
        ++curr_it;
    }
}
}
