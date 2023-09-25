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

#include <CloudServices/CnchPartsHelper.h>
#include <Catalog/CatalogUtils.h>
#include <Catalog/DataModelPartWrapper.h>
#include <Common/ThreadPool.h>
#include <Common/Stopwatch.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

#include <sstream>
#include <functional>

namespace DB::CnchPartsHelper
{
LoggingOption getLoggingOption(const Context & c)
{
    return (c.getSettingsRef().send_logs_level == LogsLevel::none) ? DisableLogging : EnableLogging;
}

IMergeTreeDataPartsVector toIMergeTreeDataPartsVector(const MergeTreeDataPartsCNCHVector & vec)
{
    IMergeTreeDataPartsVector res;
    res.reserve(vec.size());
    for (auto & p : vec)
        res.push_back(p);
    return res;
}

MergeTreeDataPartsCNCHVector toMergeTreeDataPartsCNCHVector(const IMergeTreeDataPartsVector & vec)
{
    MergeTreeDataPartsCNCHVector res;
    res.reserve(vec.size());
    for (auto & p : vec)
        res.push_back(dynamic_pointer_cast<const MergeTreeDataPartCNCH>(p));
    return res;
}

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
        LoggingOption logging,
        bool move_source_parts = false /* Reduce the all_parts destruct time */)
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

        if (logging == EnableLogging)
            move_source_parts = false; /// Do not use move operate to enable serverPartsToDebugString(all_parts)

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

ServerDataPartsVector calcVisibleParts(ServerDataPartsVector & all_parts, bool flatten, LoggingOption logging, bool move_source_parts)
{
    return calcVisiblePartsImpl<ServerDataPartsVector>(all_parts, flatten, /* skip_drop_ranges */ true, nullptr, nullptr, logging, move_source_parts);
}

MergeTreeDataPartsCNCHVector calcVisibleParts(MergeTreeDataPartsCNCHVector & all_parts, bool flatten, LoggingOption logging)
{
    return calcVisiblePartsImpl<MergeTreeDataPartsCNCHVector>(all_parts, flatten, /* skip_drop_ranges */ true, nullptr, nullptr, logging);
}

ServerDataPartsVector calcVisiblePartsForGC(
    ServerDataPartsVector & all_parts,
    ServerDataPartsVector * visible_alone_drop_ranges,
    ServerDataPartsVector * invisible_dropped_parts,
    LoggingOption logging)
{
    return calcVisiblePartsImpl(
        all_parts,
        /* flatten */ false,
        /* skip_drop_ranges */ false,
        visible_alone_drop_ranges,
        invisible_dropped_parts,
        logging);
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
