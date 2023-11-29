#pragma once
#include <Storages/MergeTree/Index/MergeTreeBitmapIndexReader.h>
#include <Columns/ColumnArray.h>

namespace DB
{
class MergeTreeBitmapIndexExpressionReader : public MergeTreeBitmapIndexReader
{
public:
    MergeTreeBitmapIndexExpressionReader(const IMergeTreeDataPartPtr & part, const BitmapIndexInfoPtr & bitmap_index_info_, const MergeTreeIndexGranularity & index_granularity_, const MarkRanges & mark_ranges_);
    MergeTreeBitmapIndexExpressionReader(const IMergeTreeDataPartPtr & part, const BitmapIndexInfoPtr & bitmap_index_info_, const MergeTreeIndexGranularity & index_granularity_, const size_t & segment_granularity, const size_t & serializing_granularity, const MarkRanges & mark_ranges_);
    
    String getName() const override { return "BitmapIndexExpressionReader"; }
    void getIndexes() override;
    size_t read(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Columns & res) override;

    static String genBitmapKey(const String & string);
};
}
