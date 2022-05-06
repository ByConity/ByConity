#pragma once

#include <Storages/MergeTree/MergeTreeBitMapIndexReader.h>

class MergeTreeRangeReader;
/**
 * This reader is used to implement bitmap index scan access method.
 * If it is enabled, arraySetCheck(bitmap, (...)) will be evaluated by bit-OR
 * instead of Reading bitmap columns and evaluation expression actions.
 *
 * This routine could be used in both prewhere handling and where handling.
 */
namespace DB
{
class MergeTreeBitMapIndexBoolReader : public MergeTreeBitMapIndexReader
{
public:
    MergeTreeBitMapIndexBoolReader(const String & path, const BitMapIndexInfoPtr & bitmap_index_info, const MergeTreeIndexGranularity & index_granularity, const MarkRanges & mark_ranges);

    String getName() const override { return "BitMapIndexBoolReader"; }

    /* Return the result of index-OR, and materialize it as ColumnUInt8*/
    size_t read(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Columns & res) override;

    void getIndexes() override;
};

}
