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

#include <Catalog/DataModelPartWrapper_fwd.h>
#include <Storages/MergeTree/DeleteBitmapMeta.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h>

namespace DB
{
class Context;
}

namespace DB
{
/// used in unit tests
struct MinimumDataPart
{
    static std::atomic<UInt64> increment;
    /////////////////////////////
    /// Factory methods
    /////////////////////////////

    static std::shared_ptr<MinimumDataPart> create(String name, bool is_deleted = false)
    {
        auto res = std::make_shared<MinimumDataPart>();
        res->info = MergeTreePartInfo::fromPartName(name, MERGE_TREE_CHCH_DATA_STORAGTE_VERSION);
        res->name = name;
        res->commit_time = increment.fetch_add(1);
        res->is_deleted = is_deleted;
        return res;
    }

    /////////////////////////////
    /// Helper functions
    /////////////////////////////

    bool containsExactly(const MinimumDataPart & rhs) const
    {
        return info.partition_id == rhs.info.partition_id && info.min_block == rhs.info.min_block && info.max_block == rhs.info.max_block
            && (info.level > rhs.info.level || commit_time > rhs.commit_time);
    }

    // for working with PartComparator
    const MergeTreePartInfo & get_info() const { return info; }
    UInt64 get_commit_time() const { return commit_time; }

    /////////////////////////////
    /// Fields
    /////////////////////////////

    String name;
    MergeTreePartInfo info;
    UInt64 commit_time = 0;
    UInt64 end_time = 0;
    std::shared_ptr<MinimumDataPart> prev;
    bool is_deleted = false;
};

using MinimumDataPartPtr = std::shared_ptr<MinimumDataPart>;
using MinimumDataParts = std::vector<MinimumDataPartPtr>;

}

namespace DB::CnchPartsHelper
{

enum LoggingOption
{
    DisableLogging = 0,
    EnableLogging = 1,
};

template <class T>
struct PartComparator
{
    bool operator()(const T & lhs, const T & rhs) const
    {
        auto & l = lhs->get_info();
        auto & r = rhs->get_info();
        return std::forward_as_tuple(l.partition_id, l.min_block, l.max_block, l.level, lhs->get_commit_time(), l.storage_type)
            < std::forward_as_tuple(r.partition_id, r.min_block, r.max_block, r.level, rhs->get_commit_time(), r.storage_type);
    }
};

LoggingOption getLoggingOption(const Context & c);

IMergeTreeDataPartsVector toIMergeTreeDataPartsVector(const MergeTreeDataPartsCNCHVector & vec);
MergeTreeDataPartsCNCHVector toMergeTreeDataPartsCNCHVector(const IMergeTreeDataPartsVector & vec);

MergeTreeDataPartsVector calcVisibleParts(MergeTreeDataPartsVector & all_parts, bool flatten, LoggingOption logging = DisableLogging);
ServerDataPartsVector calcVisibleParts(ServerDataPartsVector & all_parts, bool flatten, LoggingOption logging = DisableLogging);
MergeTreeDataPartsCNCHVector calcVisibleParts(MergeTreeDataPartsCNCHVector & all_parts, bool flatten, LoggingOption logging = DisableLogging);
IMergeTreeDataPartsVector calcVisibleParts(IMergeTreeDataPartsVector& all_parts,
    bool collect_on_chain, bool skip_drop_ranges, IMergeTreeDataPartsVector* visible_alone_drop_ranges,
    IMergeTreeDataPartsVector* invisible_dropped_parts, LoggingOption logging);
ServerDataPartsVector calcVisibleParts(ServerDataPartsVector& all_parts,
    bool collect_on_chain, bool skip_drop_ranges, ServerDataPartsVector* visible_alone_drop_ranges,
    ServerDataPartsVector* invisible_dropped_parts, LoggingOption logging);

/**
 * Compute end time for committed parts.
 * @param all_parts input, should be flatterned, all part should have been committed (w/ commit time)
 * @param out_parts_to_gc if not null, all parts with end time set will be added to it
 * @param out_visible_parts if not null, all visible parts (latest version for mvcc parts) will be added to it
 */
void calcPartsForGC(ServerDataPartsVector & all_parts, ServerDataPartsVector * out_parts_to_gc, ServerDataPartsVector * out_visible_parts);

/// for tests only
void calcMinimumPartsForGC(MinimumDataParts & all_parts, MinimumDataParts * out_parts_to_gc, MinimumDataParts * out_visible_parts);

/// similar to calcPartsForGC, but for delete bitmaps
void calcBitmapsForGC(
    DeleteBitmapMetaPtrVector & all_bitmaps,
    DeleteBitmapMetaPtrVector * out_bitmaps_to_gc,
    DeleteBitmapMetaPtrVector * out_visible_bitmaps);

void calcVisibleDeleteBitmaps(
    DeleteBitmapMetaPtrVector & all_bitmaps,
    DeleteBitmapMetaPtrVector & visible_bitmaps,
    bool include_tombstone = false,
    DeleteBitmapMetaPtrVector * visible_alone_tombstones = nullptr,
    DeleteBitmapMetaPtrVector * bitmaps_covered_by_range_tombstones = nullptr);

template <typename T>
void flattenPartsVector(std::vector<T> & visible_parts)
{
    size_t size = visible_parts.size();
    for (size_t i = 0; i < size; ++i)
    {
        auto prev_part = visible_parts[i]->tryGetPreviousPart();
        while (prev_part)
        {
            if constexpr (std::is_same_v<T, decltype(prev_part)>)
                visible_parts.push_back(prev_part);
            else
                visible_parts.push_back(std::dynamic_pointer_cast<typename T::element_type>(prev_part));

            prev_part = prev_part->tryGetPreviousPart();
        }
    }
}
}
