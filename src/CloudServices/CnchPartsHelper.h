#pragma once

#include <Catalog/DataModelPartWrapper_fwd.h>
#include <Storages/MergeTree/DeleteBitmapMeta.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h>

namespace DB
{
class Context;
}

namespace DB::CnchPartsHelper
{

enum LoggingOption
{
    DisableLogging = 0,
    EnableLogging = 1,
};

LoggingOption getLoggingOption(const Context & c);

IMergeTreeDataPartsVector toIMergeTreeDataPartsVector(const MergeTreeDataPartsCNCHVector & vec);
MergeTreeDataPartsCNCHVector toMergeTreeDataPartsCNCHVector(const IMergeTreeDataPartsVector & vec);

MergeTreeDataPartsVector calcVisibleParts(MergeTreeDataPartsVector & all_parts, bool flatten, LoggingOption logging = DisableLogging);
ServerDataPartsVector calcVisibleParts(ServerDataPartsVector & all_parts, bool flatten, LoggingOption logging = DisableLogging);
MergeTreeDataPartsCNCHVector calcVisibleParts(MergeTreeDataPartsCNCHVector & all_parts, bool flatten, LoggingOption logging = DisableLogging);

ServerDataPartsVector calcVisiblePartsForGC(
    ServerDataPartsVector & all_parts,
    ServerDataPartsVector * visible_alone_drop_ranges,
    ServerDataPartsVector * invisible_dropped_parts,
    LoggingOption logging = DisableLogging);

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
