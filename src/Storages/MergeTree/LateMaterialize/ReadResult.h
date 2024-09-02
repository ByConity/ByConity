
#pragma once

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Columns/IColumn.h>




namespace DB
{
namespace LateMaterialize
{
    class ReadResult
    {
    public:
        const MarkRanges & startedRanges() const { return started_ranges; }
        /// The number of bytes read from disk.
        size_t numBytesRead() const { return num_bytes_read; }
        /// Number of rows read from disk.
        size_t numReadRows() const { return num_read_rows; }
        /// Filter you need to apply to newly-read columns in order to add them to block.
        const ColumnUInt8 * getRowFilter() const { return row_filter; }
        ColumnPtr & getRowFilterHolder() { return row_filter_holder; }
        const ColumnUInt8 * getFilter() const { return filter; }
        ColumnPtr & getFilterHolder() { return filter_holder; }

        void addRows(size_t rows) { num_read_rows += rows; }
        void addRange(const MarkRange & range) { started_ranges.push_back(range); }

        void clearFilter() { filter = nullptr; }
        /// Set filter or replace old one. Filter must have more zeroes than previous.
        void setFilter(const ColumnPtr & new_filter);
        /// Set filter original
        void setRowFilter(const ColumnPtr & new_filter);
        /// Simple optimize when reading with pipelined early materialization
        size_t optimize();

        void setFilterConstTrue();
        void setFilterConstFalse();

        void addNumBytesRead(size_t count) { num_bytes_read += count; }

        size_t countBytesInResultFilter(const IColumn::Filter & filter);

        Block bitmap_block;
        Columns columns;
        size_t num_rows = 0;
        bool need_filter = false;
        int last_filter_column_pos = -1;
        DataTypePtr filter_type;

    private:
        /// Started ranges
        MarkRanges started_ranges;
        /// The number of rows was read at first step. May be zero if no read columns present in part.
        size_t num_read_rows = 0;
        /// Without any filtration.
        size_t num_bytes_read = 0;
        /// nullptr if prev reader hasn't prewhere_actions. Otherwise filter.size() >= total_rows_per_granule.
        ColumnPtr filter_holder;
        ColumnPtr row_filter_holder;
        const ColumnUInt8 * filter = nullptr;
        const ColumnUInt8 * row_filter = nullptr;
        std::map<const IColumn::Filter *, size_t> filter_bytes_map;
    };

}
}
