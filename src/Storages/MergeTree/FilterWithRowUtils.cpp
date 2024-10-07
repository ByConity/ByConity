#include <memory>
#include <roaring.hh>
#include <Storages/MergeTree/FilterWithRowUtils.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/RangesInDataPart.h>

namespace DB 
{

DeleteBitmapPtr combineFilterBitmap(const RangesInDataPart & part, MergeTreeData::DeleteBitmapGetter delete_bitmap_getter)
{

    auto delete_bitmap = delete_bitmap_getter(part.data_part);

    Roaring filter_delete_bitmap;
    DeleteBitmapPtr filter = nullptr;

    if (delete_bitmap != nullptr)
    {
        filter_delete_bitmap |= *delete_bitmap;
    }

    if (part.filter_bitmap != nullptr)
    {
        for(const auto & mark_range : part.ranges)
        {   
            size_t begin_rowid = part.data_part->index_granularity.getMarkStartingRow(mark_range.begin);
            size_t rowid_length = part.data_part->index_granularity.getRowsCountInRange(mark_range);

            roaring::Roaring range_roaring;
            range_roaring.addRange(begin_rowid, begin_rowid + rowid_length);
            // if not in selected in filter_bitmap, but return mark, filter by other expreession.
            if (part.filter_bitmap->and_cardinality(range_roaring) != 0)
            {
                part.filter_bitmap->flip(begin_rowid, begin_rowid + rowid_length);
            }
        }
        filter_delete_bitmap |= *(part.filter_bitmap);
    }

    if (filter_delete_bitmap.cardinality() != 0)
    {
        filter = std::make_shared<Roaring>(std::move(filter_delete_bitmap));
    }

    return filter;
}

void flipFilterWithMarkRanges(const MarkRanges& mark_ranges_,
    const MergeTreeIndexGranularity& granularity_, roaring::Roaring& filter_)
{
    for (const MarkRange& mark_range : mark_ranges_)
    {
        size_t start_row = granularity_.getMarkStartingRow(mark_range.begin);
        size_t end_row = granularity_.getMarkStartingRow(mark_range.end);
        filter_.flip(start_row, end_row);
    }
}

void setFilterWithMarkRanges(const MarkRanges& mark_ranges_,
    const MergeTreeIndexGranularity& granularity_, roaring::Roaring& filter_)
{
    for (const MarkRange& mark_range : mark_ranges_)
    {
        size_t start_row = granularity_.getMarkStartingRow(mark_range.begin);
        size_t end_row = granularity_.getMarkStartingRow(mark_range.end);
        filter_.addRange(start_row, end_row);
    }
}

MarkRanges filterMarkRangesByRowFilter(
    const MarkRanges& mark_ranges_, const roaring::Roaring& row_filter_,
    const MergeTreeIndexGranularity& granularity_)
{
    MarkRanges result_ranges;
    auto iter = row_filter_.begin();
    const auto end_iter = row_filter_.end();
    for (const MarkRange& range : mark_ranges_)
    {
        for (size_t mark = range.begin; mark < range.end; ++mark)
        {
            size_t granule_begin_row = granularity_.getMarkStartingRow(mark);
            size_t granule_end_row = granule_begin_row + granularity_.getMarkRows(mark);

            if (iter == end_iter)
            {
                break;
            }
            if (*iter < granule_begin_row)
            {
                iter.equalorlarger(granule_begin_row);
            }
            if (iter != end_iter && *iter < granule_end_row)
            {
                if (!result_ranges.empty() && result_ranges.back().end == mark)
                {
                    ++(result_ranges.back().end);
                }
                else
                {
                    result_ranges.push_back({mark, mark + 1});
                }
            }
        }
    }
    return result_ranges;
}

}
