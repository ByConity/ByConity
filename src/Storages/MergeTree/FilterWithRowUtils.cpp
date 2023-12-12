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

}
