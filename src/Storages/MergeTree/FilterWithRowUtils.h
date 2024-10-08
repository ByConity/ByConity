#pragma once

#include <memory>
#include <roaring.hh>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

using MutableFilterBitmapPtr = std::shared_ptr<roaring::Roaring>;
using FilterBitmapPtr = std::shared_ptr<const roaring::Roaring>;

DeleteBitmapPtr combineFilterBitmap(const RangesInDataPart & part, MergeTreeData::DeleteBitmapGetter delete_bitmap_getter);

void flipFilterWithMarkRanges(const MarkRanges& mark_ranges_,
    const MergeTreeIndexGranularity& granularity_, roaring::Roaring& filter_);
void setFilterWithMarkRanges(const MarkRanges& mark_ranges_,
    const MergeTreeIndexGranularity& granularity_, roaring::Roaring& filter_);
MarkRanges filterMarkRangesByRowFilter(const MarkRanges& mark_ranges_,
    const roaring::Roaring& row_filter_, const MergeTreeIndexGranularity& granularity_);

}
