#pragma once

#include <Storages/MergeTree/Index/MergeTreeBitmapIndexBoolReader.h>

class MergeTreeRangeReader;
/**
 * This reader is used to implement bitmap index scan access method.
 * If it is enabled, hasAll(bitmap, (...)) will be evaluated by bit-AND
 * instead of Reading bitmap columns and evaluation expression actions.
 *
 * This routine could be used in both prewhere handling and where handling.
 */
namespace DB
{
class MergeTreeBitmapIndexIntersectBoolReader : public MergeTreeBitmapIndexBoolReader
{
public:
    MergeTreeBitmapIndexIntersectBoolReader(const IMergeTreeDataPartPtr & part, const BitmapIndexInfoPtr & bitmap_index_info, const MergeTreeIndexGranularity & index_granularity, const MarkRanges & mark_ranges);
    MergeTreeBitmapIndexIntersectBoolReader(const IMergeTreeDataPartPtr & part, const BitmapIndexInfoPtr & bitmap_index_info, const MergeTreeIndexGranularity & index_granularity, const size_t & segment_granularity, const size_t & serializing_granularity, const MarkRanges & mark_ranges);

    String getName() const override { return "BitmapIndexIntersectBoolReader"; }

    BitmapIndexPtr getIndex(const std::shared_ptr<BitmapIndexReader> & bitmap_index_reader, const SetPtr & set_arg) override;
    BitmapIndexPtr getIndex(const std::shared_ptr<SegmentBitmapIndexReader> & bitmap_index_reader, const SetPtr & set_arg) override;
    BitmapIndexPtr getIndex(const std::shared_ptr<SegmentBitmapIndexReader> & bitmap_index_reader, const SetPtr & set_arg, const MarkRanges & mark_ranges_inc) override;

};

}
