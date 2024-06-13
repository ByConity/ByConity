/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Columns/FilterDescription.h>
#include <Columns/ColumnsCommon.h>
#include <common/range.h>
#include <Interpreters/castColumn.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeFactory.h>

#ifdef __SSE2__
#include <emmintrin.h>
#endif

namespace ProfileEvents
{
    extern const Event PrewhereSelectedMarks;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


static void filterColumns(Columns & columns, const IColumn::Filter & filter)
{
    for (auto & column : columns)
    {
        if (column)
        {
            column = column->filter(filter, -1);

            if (column->empty())
            {
                columns.clear();
                return;
            }
        }
    }
}

static void filterColumns(Columns & columns, const ColumnPtr & filter)
{
    ConstantFilterDescription const_descr(*filter);
    if (const_descr.always_true)
        return;

    if (const_descr.always_false)
    {
        for (auto & col : columns)
            if (col)
                col = col->cloneEmpty();

        return;
    }

    FilterDescription descr(*filter);
    filterColumns(columns, *descr.data);
}

size_t MergeTreeRangeReader::ReadResult::getLastMark(const MergeTreeRangeReader::ReadResult::RangesInfo & ranges)
{
    size_t current_task_last_mark = 0;
    for (const auto & mark_range : ranges)
        current_task_last_mark = std::max(current_task_last_mark, mark_range.range.end);
    return current_task_last_mark;
}

static void filterResult(MergeTreeRangeReader::ReadResult & result, const ColumnPtr & filter)
{
    filterColumns(result.columns, filter);
    auto bitmap_columns = result.bitmap_block.getColumns();
    filterColumns(bitmap_columns, filter);
    if (bitmap_columns.empty())
        result.bitmap_block = result.bitmap_block.cloneEmpty();
    else
        result.bitmap_block.setColumns(std::move(bitmap_columns));
}

static void filterResult(MergeTreeRangeReader::ReadResult & result, const IColumn::Filter & filter)
{
    filterColumns(result.columns, filter);
    auto bitmap_columns = result.bitmap_block.getColumns();
    filterColumns(bitmap_columns, filter);
    if (bitmap_columns.empty())
        result.bitmap_block = result.bitmap_block.cloneEmpty();
    else
        result.bitmap_block.setColumns(std::move(bitmap_columns));
}

static void nextRange(const UInt8* filter, size_t offset, size_t limit,
    size_t* rows_to_skip, size_t* rows_to_read)
{
    size_t cursor = offset;
    for (; cursor < limit && *(filter + cursor) == 0; ++cursor) {}
    *rows_to_skip = cursor - offset;

    for (; cursor < limit && *(filter + cursor) != 0; ++cursor) {}
    *rows_to_read = cursor - offset - *rows_to_skip;
}

MergeTreeRangeReader::DelayedStream::DelayedStream(
        size_t from_mark, size_t current_task_last_mark_, IMergeTreeReader * merge_tree_reader_)
        : current_mark(from_mark), current_offset(0), num_delayed_rows(0)
        , current_task_last_mark(current_task_last_mark_)
        , merge_tree_reader(merge_tree_reader_)
        , index_granularity(&(merge_tree_reader->data_part->index_granularity))
        , is_finished(false)
{
}

size_t MergeTreeRangeReader::DelayedStream::position() const
{
    size_t num_rows_before_current_mark = index_granularity->getMarkStartingRow(current_mark);
    return num_rows_before_current_mark + current_offset + num_delayed_rows;
}

size_t MergeTreeRangeReader::DelayedStream::readRows(Columns & columns, size_t num_rows)
{
    if (num_rows)
    {
        size_t rows_read = merge_tree_reader->readRows(current_mark, current_task_last_mark,
            current_offset, num_rows, columns);

        /// Zero rows_read maybe either because reading has finished
        ///  or because there is no columns we can read in current part (for example, all columns are default).
        /// In the last case we can't finish reading, but it's also ok for the first case
        ///  because we can finish reading by calculation the number of pending rows.
        if (0 < rows_read && rows_read < num_rows)
            is_finished = true;

        return rows_read;
    }

    return 0;
}

size_t MergeTreeRangeReader::DelayedStream::read(Columns & columns, size_t from_mark, size_t offset, size_t num_rows)
{
    size_t num_rows_before_from_mark = index_granularity->getMarkStartingRow(from_mark);
    /// We already stand accurately in required position,
    /// so because stream is lazy, we don't read anything
    /// and only increment amount delayed_rows
    if (position() == num_rows_before_from_mark + offset)
    {
        num_delayed_rows += num_rows;
        return 0;
    }
    else
    {
        size_t read_rows = finalize(columns);

        current_mark = from_mark;
        current_offset = offset;
        num_delayed_rows = num_rows;

        return read_rows;
    }
}

size_t MergeTreeRangeReader::DelayedStream::finalize(Columns & columns)
{
    /// Calculate accurate current_mark and current_offset
    if (current_offset)
    {
        for (size_t mark_num : collections::range(current_mark, index_granularity->getMarksCount()))
        {
            size_t mark_index_granularity = index_granularity->getMarkRows(mark_num);
            if (current_offset >= mark_index_granularity)
            {
                current_offset -= mark_index_granularity;
                current_mark++;
            }
            else
                break;
        }
    }

    size_t readed = readRows(columns, num_delayed_rows);
    current_offset += num_delayed_rows;
    num_delayed_rows = 0;
    return readed;
}


MergeTreeRangeReader::Stream::Stream(
        size_t from_mark, size_t to_mark, size_t current_task_last_mark, IMergeTreeReader * merge_tree_reader_)
        : current_mark(from_mark), offset_after_current_mark(0)
        , last_mark(to_mark)
        , merge_tree_reader(merge_tree_reader_)
        , index_granularity(&(merge_tree_reader->data_part->index_granularity))
        , current_mark_index_granularity(index_granularity->getMarkRows(from_mark))
        , stream(from_mark, current_task_last_mark, merge_tree_reader)
{
    size_t marks_count = index_granularity->getMarksCount();
    if (from_mark >= marks_count)
        throw Exception("Trying create stream to read from mark №"+ toString(current_mark) + " but total marks count is "
            + toString(marks_count), ErrorCodes::LOGICAL_ERROR);

    if (last_mark > marks_count)
        throw Exception("Trying create stream to read to mark №"+ toString(current_mark) + " but total marks count is "
            + toString(marks_count), ErrorCodes::LOGICAL_ERROR);
}

void MergeTreeRangeReader::Stream::checkNotFinished() const
{
    if (isFinished())
        throw Exception("Cannot read out of marks range.", ErrorCodes::LOGICAL_ERROR);
}

void MergeTreeRangeReader::Stream::checkEnoughSpaceInCurrentGranule(size_t num_rows) const
{
    if (num_rows + offset_after_current_mark > current_mark_index_granularity)
        throw Exception("Cannot read from granule more than index_granularity.", ErrorCodes::LOGICAL_ERROR);
}

size_t MergeTreeRangeReader::Stream::readRows(Columns & columns, size_t num_rows)
{
    size_t rows_read = stream.read(columns, current_mark, offset_after_current_mark, num_rows);

    if (stream.isFinished())
        finish();

    return rows_read;
}

void MergeTreeRangeReader::Stream::toNextMark()
{
    ++current_mark;

    size_t total_marks_count = index_granularity->getMarksCount();
    if (current_mark < total_marks_count)
        current_mark_index_granularity = index_granularity->getMarkRows(current_mark);
    else if (current_mark == total_marks_count)
        current_mark_index_granularity = 0; /// HACK?
    else
        throw Exception("Trying to read from mark " + toString(current_mark) + ", but total marks count " + toString(total_marks_count), ErrorCodes::LOGICAL_ERROR);

    offset_after_current_mark = 0;
}

size_t MergeTreeRangeReader::Stream::position() const
{
    size_t num_rows_before_current_mark = index_granularity->getMarkStartingRow(current_mark);
    return num_rows_before_current_mark + offset_after_current_mark;
}

size_t MergeTreeRangeReader::Stream::read(Columns & columns, size_t num_rows, bool skip_remaining_rows_in_current_granule)
{
    checkEnoughSpaceInCurrentGranule(num_rows);

    if (num_rows)
    {
        checkNotFinished();

        size_t read_rows = readRows(columns, num_rows);

        offset_after_current_mark += num_rows;

        /// Start new granule; skipped_rows_after_offset is already zero.
        if (offset_after_current_mark == current_mark_index_granularity || skip_remaining_rows_in_current_granule)
            toNextMark();

        return read_rows;
    }
    else
    {
        /// Nothing to read.
        if (skip_remaining_rows_in_current_granule)
        {
            /// Skip the rest of the rows in granule and start new one.
            checkNotFinished();
            toNextMark();
        }

        return 0;
    }
}

void MergeTreeRangeReader::Stream::skip(size_t num_rows)
{
    if (num_rows)
    {
        checkNotFinished();
        checkEnoughSpaceInCurrentGranule(num_rows);

        offset_after_current_mark += num_rows;

        if (offset_after_current_mark == current_mark_index_granularity)
        {
            /// Start new granule; skipped_rows_after_offset is already zero.
            toNextMark();
        }
    }
}

size_t MergeTreeRangeReader::Stream::finalize(Columns & columns)
{
    size_t read_rows = stream.finalize(columns);

    if (stream.isFinished())
        finish();

    return read_rows;
}


void MergeTreeRangeReader::ReadResult::addGranule(size_t num_rows_)
{
    rows_per_granule.push_back(num_rows_);
    total_rows_per_granule += num_rows_;
}

void MergeTreeRangeReader::ReadResult::adjustLastGranule()
{
    size_t num_rows_to_subtract = total_rows_per_granule - num_read_rows;

    if (rows_per_granule.empty())
        throw Exception("Can't adjust last granule because no granules were added.", ErrorCodes::LOGICAL_ERROR);

    if (num_rows_to_subtract > rows_per_granule.back())
        throw Exception("Can't adjust last granule because it has " + toString(rows_per_granule.back())
                        + " rows, but try to subtract " + toString(num_rows_to_subtract) + " rows.",
                        ErrorCodes::LOGICAL_ERROR);

    rows_per_granule.back() -= num_rows_to_subtract;
    total_rows_per_granule -= num_rows_to_subtract;
}

void MergeTreeRangeReader::ReadResult::clear()
{
    /// Need to save information about the number of granules.
    num_rows_to_skip_in_last_granule += rows_per_granule.back();
    rows_per_granule.assign(rows_per_granule.size(), 0);
    total_rows_per_granule = 0;
    filter_holder = nullptr;
    filter = nullptr;
}

void MergeTreeRangeReader::ReadResult::shrink(Columns & old_columns)
{
    for (auto & column : old_columns)
    {
        if (!column)
            continue;

        if (const auto * column_const = typeid_cast<const ColumnConst *>(column.get()))
        {
            column = column_const->cloneResized(total_rows_per_granule);
            continue;
        }

        auto new_column = column->cloneEmpty();
        new_column->reserve(total_rows_per_granule);
        for (size_t j = 0, pos = 0; j < rows_per_granule_original.size(); pos += rows_per_granule_original[j++])
        {
            if (rows_per_granule[j])
                new_column->insertRangeFrom(*column, pos, rows_per_granule[j]);
        }
        column = std::move(new_column);
    }
}

void MergeTreeRangeReader::ReadResult::setFilterConstTrue()
{
    clearFilter();
    filter_holder = DataTypeUInt8().createColumnConst(num_rows, 1u);
}

void MergeTreeRangeReader::ReadResult::setFilterConstFalse()
{
    clearFilter();
    columns.clear();
    bitmap_block.clear();
    num_rows = 0;
}

void MergeTreeRangeReader::ReadResult::optimize(bool can_read_incomplete_granules, bool allow_filter_columns)
{
    if (total_rows_per_granule == 0 || filter == nullptr)
        return;

    NumRows zero_tails;
    auto total_zero_rows_in_tails = countZeroTails(filter->getData(), zero_tails, can_read_incomplete_granules);

    if (total_zero_rows_in_tails == filter->size())
    {
        clear();
        return;
    }
    else if (total_zero_rows_in_tails == 0 && countBytesInResultFilter(filter->getData()) == filter->size())
    {
        setFilterConstTrue();
        return;
    }
    /// Just a guess. If only a few rows may be skipped, it's better not to skip at all.
    else if (2 * total_zero_rows_in_tails > filter->size())
    {
        for (auto i : collections::range(0, rows_per_granule.size()))
        {
            rows_per_granule_original.push_back(rows_per_granule[i]);
            rows_per_granule[i] -= zero_tails[i];
        }
        num_rows_to_skip_in_last_granule += rows_per_granule_original.back() - rows_per_granule.back();

        filter_original = filter;
        filter_holder_original = std::move(filter_holder);

        /// Check if const 1 after shrink
        if (allow_filter_columns && countBytesInResultFilter(filter->getData()) + total_zero_rows_in_tails == total_rows_per_granule)
        {
            total_rows_per_granule = total_rows_per_granule - total_zero_rows_in_tails;
            num_rows = total_rows_per_granule;
            setFilterConstTrue();
            shrink(columns); /// shrink acts as filtering in such case
            auto bitmap_columns = bitmap_block.getColumns();
            shrink(bitmap_columns);
            bitmap_block.setColumns(std::move(bitmap_columns));
        }
        else
        {
            auto new_filter = ColumnUInt8::create(filter->size() - total_zero_rows_in_tails);
            IColumn::Filter & new_data = new_filter->getData();

            collapseZeroTails(filter->getData(), new_data);
            total_rows_per_granule = new_filter->size();
            num_rows = total_rows_per_granule;
            filter = new_filter.get();
            filter_holder = std::move(new_filter);
        }
        need_filter = true;
    }
    /// Another guess, if it's worth filtering at PREWHERE
    else if (countBytesInResultFilter(filter->getData()) < 0.6 * filter->size())
        need_filter = true;
}

size_t MergeTreeRangeReader::ReadResult::countZeroTails(const IColumn::Filter & filter_vec, NumRows & zero_tails, bool can_read_incomplete_granules) const
{
    zero_tails.resize(0);
    zero_tails.reserve(rows_per_granule.size());

    const auto * filter_data = filter_vec.data();

    size_t total_zero_rows_in_tails = 0;

    for (auto rows_to_read : rows_per_granule)
    {
        /// Count the number of zeros at the end of filter for rows were read from current granule.
        size_t zero_tail = numZerosInTail(filter_data, filter_data + rows_to_read);
        if (!can_read_incomplete_granules && zero_tail != rows_to_read)
            zero_tail = 0;
        zero_tails.push_back(zero_tail);
        total_zero_rows_in_tails += zero_tails.back();
        filter_data += rows_to_read;
    }

    return total_zero_rows_in_tails;
}

void MergeTreeRangeReader::ReadResult::collapseZeroTails(const IColumn::Filter & filter_vec, IColumn::Filter & new_filter_vec)
{
    const auto * filter_data = filter_vec.data();
    auto * new_filter_data = new_filter_vec.data();

    for (auto i : collections::range(0, rows_per_granule.size()))
    {
        memcpySmallAllowReadWriteOverflow15(new_filter_data, filter_data, rows_per_granule[i]);
        filter_data += rows_per_granule_original[i];
        new_filter_data += rows_per_granule[i];
    }

    new_filter_vec.resize(new_filter_data - new_filter_vec.data());
}

size_t MergeTreeRangeReader::ReadResult::numZerosInTail(const UInt8 * begin, const UInt8 * end)
{
    size_t count = 0;

#if defined(__SSE2__) && defined(__POPCNT__)
    const __m128i zero16 = _mm_setzero_si128();
    while (end - begin >= 64)
    {
        end -= 64;
        const auto * pos = end;
        UInt64 val =
                static_cast<UInt64>(_mm_movemask_epi8(_mm_cmpeq_epi8(
                        _mm_loadu_si128(reinterpret_cast<const __m128i *>(pos)),
                        zero16)))
                | (static_cast<UInt64>(_mm_movemask_epi8(_mm_cmpeq_epi8(
                        _mm_loadu_si128(reinterpret_cast<const __m128i *>(pos + 16)),
                        zero16))) << 16u)
                | (static_cast<UInt64>(_mm_movemask_epi8(_mm_cmpeq_epi8(
                        _mm_loadu_si128(reinterpret_cast<const __m128i *>(pos + 32)),
                        zero16))) << 32u)
                | (static_cast<UInt64>(_mm_movemask_epi8(_mm_cmpeq_epi8(
                        _mm_loadu_si128(reinterpret_cast<const __m128i *>(pos + 48)),
                        zero16))) << 48u);
        val = ~val;
        if (val == 0)
            count += 64;
        else
        {
            count += __builtin_clzll(val);
            return count;
        }
    }
#endif

    while (end > begin && *(--end) == 0)
    {
        ++count;
    }
    return count;
}

void MergeTreeRangeReader::ReadResult::setFilter(const ColumnPtr & new_filter)
{
    if (!new_filter && filter)
        throw Exception("Can't replace existing filter with empty.", ErrorCodes::LOGICAL_ERROR);

    if (filter)
    {
        size_t new_size = new_filter->size();

        if (new_size != total_rows_per_granule)
            throw Exception("Can't set filter because it's size is " + toString(new_size) + " but "
                            + toString(total_rows_per_granule) + " rows was read.", ErrorCodes::LOGICAL_ERROR);
    }

    ConstantFilterDescription const_description(*new_filter);
    if (const_description.always_true)
    {
        setFilterConstTrue();
    }
    else if (const_description.always_false)
    {
        clear();
    }
    else
    {
        FilterDescription filter_description(*new_filter);
        filter_holder = filter_description.data_holder ? filter_description.data_holder : new_filter;
        filter = typeid_cast<const ColumnUInt8 *>(filter_holder.get());
        if (!filter)
            throw Exception("setFilter function expected ColumnUInt8.", ErrorCodes::LOGICAL_ERROR);
    }
}


size_t MergeTreeRangeReader::ReadResult::countBytesInResultFilter(const IColumn::Filter & filter_)
{
    auto it = filter_bytes_map.find(&filter_);
    if (it == filter_bytes_map.end())
    {
        auto bytes = countBytesInFilter(filter_);
        filter_bytes_map[&filter_] = bytes;
        return bytes;
    }
    else
        return it->second;
}

MergeTreeRangeReader::MergeTreeRangeReader(
    IMergeTreeReader * merge_tree_reader_,
    MergeTreeRangeReader * prev_reader_,
    const PrewhereExprInfo * prewhere_info_,
    ImmutableDeleteBitmapPtr delete_bitmap_,
    bool last_reader_in_chain_,
    size_t filtered_ratio_to_use_skip_read_)
    : merge_tree_reader(merge_tree_reader_)
    , index_granularity(&(merge_tree_reader->data_part->index_granularity))
    , prev_reader(prev_reader_)
    , prewhere_info(prewhere_info_)
    , delete_bitmap(delete_bitmap_)
    , last_reader_in_chain(last_reader_in_chain_)
    , is_initialized(true)
    , filtered_ratio_to_use_skip_read(filtered_ratio_to_use_skip_read_)
{
    if (prev_reader)
        sample_block = prev_reader->getSampleBlock();

    for (const auto & name_and_type : merge_tree_reader->getColumns())
        sample_block.insert({name_and_type.type->createColumn(), name_and_type.type, name_and_type.name});

    Block bitmap_block;
    if (merge_tree_reader->hasBitmapIndexReader())
    {
        for (const auto & name_and_type : merge_tree_reader->getBitmapColumns())
            bitmap_block.insert({name_and_type.type->createColumn(), name_and_type.type, name_and_type.name});
    }

    if (prewhere_info)
    {
        if (prewhere_info->alias_actions)
            prewhere_info->alias_actions->execute(sample_block, true);

        if (prewhere_info->row_level_filter)
        {
            prewhere_info->row_level_filter->execute(sample_block, true);
            sample_block.erase(prewhere_info->row_level_column_name);
        }

        size_t rows = sample_block.rows();
        if (prewhere_info->prewhere_actions)
            prewhere_info->prewhere_actions->execute(sample_block, &bitmap_block, rows, true);
        if (!sample_block)
            sample_block.insert({DataTypeUInt8().createColumnConst(rows, 0), std::make_shared<DataTypeUInt8>(), "_dummy"});

        if (prewhere_info->remove_prewhere_column)
        {
            if (sample_block.has(prewhere_info->prewhere_column_name))
                sample_block.erase(prewhere_info->prewhere_column_name);
        }
        else
        {
            if (!sample_block.has(prewhere_info->prewhere_column_name))
            {
                const auto & prewhere_actions = prewhere_info->prewhere_actions->getActions();
                const auto * last_action_node = prewhere_actions[prewhere_actions.size()-1].node;
                sample_block.insert({last_action_node->result_type->createColumn(),last_action_node->result_type, prewhere_info->prewhere_column_name});
            }
        }
    }
}

bool MergeTreeRangeReader::isReadingFinished() const
{
    return prev_reader ? prev_reader->isReadingFinished() : stream.isFinished();
}

size_t MergeTreeRangeReader::numReadRowsInCurrentGranule() const
{
    return prev_reader ? prev_reader->numReadRowsInCurrentGranule() : stream.numReadRowsInCurrentGranule();
}

size_t MergeTreeRangeReader::numPendingRowsInCurrentGranule() const
{
    if (prev_reader)
        return prev_reader->numPendingRowsInCurrentGranule();

    auto pending_rows = stream.numPendingRowsInCurrentGranule();

    if (pending_rows)
        return pending_rows;

    return numRowsInCurrentGranule();
}


size_t MergeTreeRangeReader::numRowsInCurrentGranule() const
{
    /// If pending_rows is zero, than stream is not initialized.
    if (stream.current_mark_index_granularity)
        return stream.current_mark_index_granularity;

    /// We haven't read anything, return first
    size_t first_mark = merge_tree_reader->getFirstMarkToRead();
    return index_granularity->getMarkRows(first_mark);
}

size_t MergeTreeRangeReader::currentMark() const
{
    return stream.currentMark();
}

size_t MergeTreeRangeReader::Stream::numPendingRows() const
{
    size_t rows_between_marks = index_granularity->getRowsCountInRange(current_mark, last_mark);
    return rows_between_marks - offset_after_current_mark;
}


size_t MergeTreeRangeReader::Stream::ceilRowsToCompleteGranules(size_t rows_num) const
{
    /// FIXME suboptimal
    size_t result = 0;
    size_t from_mark = current_mark;
    while (result < rows_num && from_mark < last_mark)
        result += index_granularity->getMarkRows(from_mark++);

    return result;
}


bool MergeTreeRangeReader::isCurrentRangeFinished() const
{
    return prev_reader ? prev_reader->isCurrentRangeFinished() : stream.isFinished();
}

MergeTreeRangeReader::ReadResult MergeTreeRangeReader::read(size_t max_rows, MarkRanges & ranges)
{
    if (max_rows == 0)
        throw Exception("Expected at least 1 row to read, got 0.", ErrorCodes::LOGICAL_ERROR);

    ReadResult read_result;

    if (prev_reader)
    {
        read_result = prev_reader->read(max_rows, ranges);

        size_t total_rows_to_read = read_result.totalRowsPerGranule();
        size_t filtered_rows = read_result.getFilter() == nullptr ? total_rows_to_read
            : read_result.countBytesInResultFilter(read_result.getFilter()->getData());
        /// Only use skip read if filtered rows is lower than certain percentage
        bool filter_when_read = filtered_ratio_to_use_skip_read && filtered_rows * filtered_ratio_to_use_skip_read < total_rows_to_read
            && merge_tree_reader->canReadIncompleteGranules();

        size_t num_read_rows;
        Columns columns = continueReadingChain(read_result, num_read_rows,
            filter_when_read);

        /// Nothing to do. Return empty result.
        if (read_result.num_rows == 0)
            return read_result;

        bool has_columns = false;
        size_t total_bytes = 0;
        for (auto & column : columns)
        {
            if (column)
            {
                total_bytes += column->byteSize();
                has_columns = true;
            }
        }

        read_result.addNumBytesRead(total_bytes);

        bool should_evaluate_missing_defaults = false;

        if (has_columns)
        {
            /// num_read_rows >= read_result.num_rows
            /// We must filter block before adding columns to read_result.block

            /// Fill missing columns before filtering because some arrays from Nested may have empty data.
            merge_tree_reader->fillMissingColumns(columns, should_evaluate_missing_defaults, num_read_rows);

            if (read_result.getFilter() && !filter_when_read)
            {
                filterColumns(columns, read_result.getFilter()->getData());
            }
        }
        else
        {
            size_t num_rows = read_result.num_rows;

            /// If block is empty, we still may need to add missing columns.
            /// In that case use number of rows in result block and don't filter block.
            if (num_rows)
                merge_tree_reader->fillMissingColumns(columns, should_evaluate_missing_defaults, num_rows);
        }

        if (!columns.empty())
        {
            /// If some columns absent in part, then evaluate default values
            if (should_evaluate_missing_defaults)
            {
                auto block = prev_reader->sample_block.cloneWithColumns(read_result.columns);
                auto block_before_prewhere = read_result.block_before_prewhere;
                for (auto & ctn : block)
                {
                    if (block_before_prewhere.has(ctn.name))
                        block_before_prewhere.erase(ctn.name);
                }

                if (block_before_prewhere)
                {
                    if (read_result.need_filter)
                    {
                        auto old_columns = block_before_prewhere.getColumns();
                        filterColumns(old_columns, read_result.getFilterOriginal()->getData());
                        block_before_prewhere.setColumns(std::move(old_columns));
                    }

                    for (auto && ctn : block_before_prewhere)
                        block.insert(std::move(ctn));
                }
                merge_tree_reader->evaluateMissingDefaults(block, columns);
            }
            /// If columns not empty, then apply on-fly alter conversions if any required
            merge_tree_reader->performRequiredConversions(columns);
        }

        extractBitmapIndexColumns(columns, read_result.bitmap_block);
        read_result.columns.reserve(read_result.columns.size() + columns.size());
        for (auto & column : columns)
            read_result.columns.emplace_back(std::move(column));
    }
    else
    {
        read_result = startReadingChain(max_rows, ranges);
        read_result.num_rows = read_result.numReadRows();

        if (read_result.num_rows)
        {
            bool should_evaluate_missing_defaults;
            merge_tree_reader->fillMissingColumns(read_result.columns, should_evaluate_missing_defaults,
                                                  read_result.num_rows);

            /// If some columns absent in part, then evaluate default values
            if (should_evaluate_missing_defaults)
                merge_tree_reader->evaluateMissingDefaults({}, read_result.columns);

            /// If result not empty, then apply on-fly alter conversions if any required
            merge_tree_reader->performRequiredConversions(read_result.columns);
        }
        else
            read_result.columns.clear();

        size_t total_bytes = 0;
        for (auto & column : read_result.columns)
            total_bytes += column->byteSize();

        read_result.addNumBytesRead(total_bytes);
        extractBitmapIndexColumns(read_result.columns, read_result.bitmap_block);
    }

    if (read_result.num_rows == 0)
        return read_result;

    if (prewhere_info)
    {
        executePrewhereActionsAndFilterColumns(read_result);
    }
    else if (!prev_reader && read_result.getFilter())
    {
        filterResult(read_result, read_result.getFilter()->getData());
        read_result.num_rows = read_result.countBytesInResultFilter(read_result.getFilter()->getData());
    }

    if (last_reader_in_chain)
    {
        for(auto &col: read_result.columns)
        {
            if (col) {
                col->tryToFlushZeroCopyBuffer();
            }
        }
    }

    return read_result;
}


MergeTreeRangeReader::ReadResult MergeTreeRangeReader::startReadingChain(size_t max_rows, MarkRanges & ranges)
{
    ReadResult result;
    result.columns.resize(merge_tree_reader->numColumnsInResult());

    size_t current_task_last_mark = getLastMark(ranges);
    size_t num_selected_marks = 0;

    ColumnUInt8::MutablePtr delete_filter_column;
    bool delete_filter_always_true = true;
    if (delete_bitmap)
    {
        delete_filter_column = ColumnUInt8::create(max_rows, 1);
    }

    /// Stream is lazy. result.num_added_rows is the number of rows added to block which is not equal to
    /// result.num_rows_read until call to stream.finalize(). Also result.num_added_rows may be less than
    /// result.num_rows_read if the last granule in range also the last in part (so we have to adjust last granule).
    {
        size_t space_left = max_rows;
        while (space_left && (!stream.isFinished() || !ranges.empty()))
        {
            if (stream.isFinished())
            {
                result.addRows(stream.finalize(result.columns));
                stream = Stream(ranges.front().begin, ranges.front().end, current_task_last_mark, merge_tree_reader);
                result.addRange(ranges.front());
                ranges.pop_front();
            }

            size_t current_space = space_left;

            /// If reader can't read part of granule, we have to increase number of reading rows
            ///  to read complete granules and exceed max_rows a bit.
            if (!merge_tree_reader->canReadIncompleteGranules())
                current_space = stream.ceilRowsToCompleteGranules(space_left);

            const auto unread_rows_in_current_granule = stream.numPendingRowsInCurrentGranule();
            auto rows_to_read = std::min(current_space, unread_rows_in_current_granule);

            auto read_pos = stream.position();
            auto read_end = read_pos + rows_to_read;
            /// Skip the current granule if all the pending rows have been deleted
            /// TODO: maybe it's better to prune deleted granules when constructing the mark ranges
            if (rows_to_read == unread_rows_in_current_granule &&
                delete_bitmap && delete_bitmap->containsRange(read_pos, read_end))
            {
                /// all pending rows in current granule has been deleted, skip it
                stream.skip(rows_to_read);
                result.addGranule(0);
            }
            /// Otherwise read the next `rows_to_read` rows (maybe < unread_rows_in_current_granule) from
            /// the current granule and prepare the delete filter that'll be used later to remove deleted rows
            else
            {
                size_t filter_column_offset = max_rows - space_left;
                /// populate delete filter for this granule
                if (delete_bitmap)
                {
                    UInt8 * filter_start = delete_filter_column->getData().data() + filter_column_offset;
                    auto iter = delete_bitmap->begin();
                    auto end = delete_bitmap->end();
                    iter.equalorlarger(read_pos);
                    auto saved_iter = iter;
                    while (iter != end && *iter < read_end)
                    {
                        filter_start[*iter - read_pos] = 0;
                        iter++;
                    }
                    if (iter != saved_iter)
                        delete_filter_always_true = false;
                }

                if (last_reader_in_chain)
                {
                    num_selected_marks += (!last_selected_mark.has_value() || *last_selected_mark != stream.currentMark());
                    last_selected_mark = stream.currentMark();
                }

                bool last = rows_to_read == space_left;
                result.addRows(stream.read(result.columns, rows_to_read, !last));
                result.addGranule(rows_to_read);
                space_left = (rows_to_read > space_left ? 0 : space_left - rows_to_read);
            }
        }
    }

    result.addRows(stream.finalize(result.columns));

    /// Last granule may be incomplete.
    result.adjustLastGranule();

    ProfileEvents::increment(ProfileEvents::PrewhereSelectedMarks, num_selected_marks);

    if (delete_bitmap && !delete_filter_always_true)
    {
        /// filter size must match column size
        delete_filter_column->getData().resize(result.numReadRows());
        ColumnPtr filter_column = std::move(delete_filter_column);
        result.setFilter(filter_column);
    }

    return result;
}

Columns MergeTreeRangeReader::continueReadingChain(ReadResult & result,
    size_t & num_rows, bool filter_when_read)
{
    Columns columns;
    num_rows = 0;

    if (result.rowsPerGranule().empty())
    {
        /// If zero rows were read on prev step, than there is no more rows to read.
        /// Last granule may have less rows than index_granularity, so finish reading manually.
        stream.finish();
        return columns;
    }

    columns.resize(merge_tree_reader->numColumnsInResult());

    const auto & rows_per_granule = result.rowsPerGranule();
    const auto & started_ranges = result.startedRanges();

    size_t current_task_last_mark = ReadResult::getLastMark(started_ranges);
    size_t next_range_to_start = 0;

    size_t read_cursor = 0;
    size_t expected_rows = 0;
    auto size = rows_per_granule.size();
    size_t num_selected_marks = 0;
    for (auto i : collections::range(0, size))
    {
        if (next_range_to_start < started_ranges.size()
            && i == started_ranges[next_range_to_start].num_granules_read_before_start)
        {
            num_rows += stream.finalize(columns);
            const auto & range = started_ranges[next_range_to_start].range;
            ++next_range_to_start;
            stream = Stream(range.begin, range.end, current_task_last_mark, merge_tree_reader);
        }

        if (last_reader_in_chain && rows_per_granule[i])
        {
            /// maintain last_selected_mark here to avoid counting the same mark more than once
            num_selected_marks += (!last_selected_mark.has_value() || *last_selected_mark != stream.currentMark());
            last_selected_mark = stream.currentMark();
        }

        if (const ColumnUInt8* filter = result.getFilter(); filter != nullptr && filter_when_read)
        {
            size_t rows_to_skip_at_tail = rows_per_granule[i] < stream.numPendingRowsInCurrentGranule() && (i + 1 != size) ?
                stream.numPendingRowsInCurrentGranule() - rows_per_granule[i] : 0;
            for (size_t rows_to_skip = 0, rows_to_read = 0, read_limit = read_cursor + rows_per_granule[i];
                read_cursor < read_limit; read_cursor += (rows_to_read + rows_to_skip))
            {
                nextRange(filter->getData().data(), read_cursor, read_limit, &rows_to_skip,
                    &rows_to_read);

                expected_rows += rows_to_read;
                stream.skip(rows_to_skip);
                num_rows += stream.read(columns, rows_to_read, false);
            }
            stream.skip(rows_to_skip_at_tail);
        }
        else
        {
            bool last = i + 1 == size;
            num_rows += stream.read(columns, rows_per_granule[i], !last);
            expected_rows += rows_per_granule[i];
        }
    }

    stream.skip(result.numRowsToSkipInLastGranule());
    num_rows += stream.finalize(columns);

    ProfileEvents::increment(ProfileEvents::PrewhereSelectedMarks, num_selected_marks);

    /// added_rows may be zero if all columns were read in prewhere and it's ok.
    if (num_rows)
    {
        if (num_rows != expected_rows)
            throw Exception("RangeReader read " + toString(num_rows) + " rows, but "
                + toString(expected_rows) + " expected.", ErrorCodes::LOGICAL_ERROR);
    }

    return columns;
}

static void checkCombinedFiltersSize(size_t bytes_in_first_filter, size_t second_filter_size)
{
    if (bytes_in_first_filter != second_filter_size)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot combine filters because number of bytes in a first filter ({}) "
            "does not match second filter size ({})", bytes_in_first_filter, second_filter_size);
}

static ColumnPtr combineFilters(ColumnPtr first, ColumnPtr second)
{
    ConstantFilterDescription first_const_descr(*first);

    if (first_const_descr.always_true)
    {
        checkCombinedFiltersSize(first->size(), second->size());
        return second;
    }

    if (first_const_descr.always_false)
    {
        checkCombinedFiltersSize(0, second->size());
        return first;
    }

    FilterDescription first_descr(*first);

    size_t bytes_in_first_filter = countBytesInFilter(*first_descr.data);
    checkCombinedFiltersSize(bytes_in_first_filter, second->size());

    ConstantFilterDescription second_const_descr(*second);

    if (second_const_descr.always_true)
        return first;

    if (second_const_descr.always_false)
        return second->cloneResized(first->size());

    FilterDescription second_descr(*second);

    MutableColumnPtr mut_first;
    if (first_descr.data_holder)
        mut_first = IColumn::mutate(std::move(first_descr.data_holder));
    else
        mut_first = IColumn::mutate(std::move(first));

    auto & first_data = typeid_cast<ColumnUInt8 *>(mut_first.get())->getData();
    const auto * second_data = second_descr.data->data();

    for (auto & val : first_data)
    {
        if (val)
        {
            val = *second_data;
            ++second_data;
        }
    }

    return mut_first;
}

static ColumnPtr combineFilterEqualSize(ColumnPtr first, ColumnPtr second)
{
    /// Use first filter to only fiter the rows not been filted in second filter.
    /// in other words, do a & operation like: first = first & second
    ConstantFilterDescription first_const_descr(*first);
    ConstantFilterDescription second_const_descr(*second);

    checkCombinedFiltersSize(first->size(), second->size());

    if (second_const_descr.always_true)
        return first;

    if (second_const_descr.always_false)
        return second;

    if (first_const_descr.always_true)
        return second;

    if (first_const_descr.always_false)
        return first;

    FilterDescription first_descr(*first);
    FilterDescription second_descr(*second);

    MutableColumnPtr mut_first;
    if (first_descr.data_holder)
        mut_first = IColumn::mutate(std::move(first_descr.data_holder));
    else
        mut_first = IColumn::mutate(std::move(first));

    auto dst = typeid_cast<ColumnUInt8 *>(mut_first.get())->getData().data();
    auto end = dst + second->size();
    auto src = second_descr.data->data();

    /// TODO @canhld:
    /// 1. can use aligned load/store?
    /// 2. can be more efficient with bit unpacking instructions
    /// Notes: when using simd instructions, we use `while(dst < end)` without guarding because we assume
    /// that the columns data is padded right at least 32 bytes, look at PODArray implementation.

#if defined (__AVX2__)
    while (dst < end)
    {
        /// *dst &= val
        _mm256_storeu_si256(
            reinterpret_cast<__m256i *>(dst),
            _mm256_and_si256(
                _mm256_loadu_si256(reinterpret_cast<const __m256i *>(dst)), _mm256_loadu_si256(reinterpret_cast<const __m256i *>(src))));
        dst += 32; /// assume using POD Array with 32 bytes padding data
        src += 32;
    }
#elif defined (__SSE2__)
    while (dst < end)
    {
        /// *dst &= val
        _mm_storeu_si128(
            reinterpret_cast<__m128i *>(dst),
            _mm_and_si128(
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(dst)), _mm_loadu_si128(reinterpret_cast<const __m128i *>(src))));
        dst += 16; /// assume using POD Array with 32 bytes padding data
        src += 16;
    }
#endif

    /// guard in case no SIMD instructions are available
    while (dst < end)
    {
        *reinterpret_cast<UInt64 *>(dst) &= *reinterpret_cast<const UInt64 *>(src);
        dst += 8;
        src += 8;
    }

    return mut_first;
}

void MergeTreeRangeReader::executePrewhereActionsAndFilterColumns(ReadResult & result)
{
    if (!prewhere_info)
        return;

    const auto & header = merge_tree_reader->getColumns();
    size_t num_columns = header.size();

    if (result.columns.size() != num_columns)
        throw Exception("Invalid number of columns passed to MergeTreeRangeReader. "
                        "Expected " + toString(num_columns) + ", "
                        "got " + toString(result.columns.size()), ErrorCodes::LOGICAL_ERROR);

    ColumnPtr filter;
    ColumnPtr row_level_filter;
    size_t prewhere_column_pos;

    {
        /// Restore block from columns list.
        Block block;
        size_t pos = 0;

        if (prev_reader)
        {
            for (const auto & col : prev_reader->getSampleBlock())
            {
                block.insert({result.columns[pos], col.type, col.name});
                ++pos;
            }
        }

        for (auto name_and_type = header.begin(); pos < num_columns; ++pos, ++name_and_type)
            block.insert({result.columns[pos], name_and_type->type, name_and_type->name});

        if (prewhere_info->alias_actions)
            prewhere_info->alias_actions->execute(block);

        /// Columns might be projected out. We need to store them here so that default columns can be evaluated later.
        result.block_before_prewhere = block;

        if (prewhere_info->row_level_filter)
        {
            prewhere_info->row_level_filter->execute(block);
            auto row_level_filter_pos = block.getPositionByName(prewhere_info->row_level_column_name);
            row_level_filter = block.getByPosition(row_level_filter_pos).column;
            block.erase(row_level_filter_pos);

            if (result.getFilter())
            {
                /// NOTE: 1 means keep this row, 0 means delete this row
                /// we first use the row_level_filter (if any) to filter the block, then
                /// execute prewhere_actions to create the `filter` on already filtered block.
                //
                /// thus row_level_filter's size always equal to the original block's size.
                /// filter's size might be small if row_level_filter exists
                ///
                /// in essential, deleted_bitmap is also a row_level_filter, so here we combine them together.
                /// row_level_filter &= delete_bitmap
                row_level_filter = combineFilterEqualSize(std::move(row_level_filter), result.getFilterHolder());
            }

            auto columns = block.getColumns();
            filterColumns(columns, row_level_filter);
            if (columns.empty())
                block = block.cloneEmpty();
            else
                block.setColumns(std::move(columns));

            auto bitmap_columns = result.bitmap_block.getColumns();
            filterColumns(bitmap_columns, row_level_filter);
            if (bitmap_columns.empty())
                result.bitmap_block = result.bitmap_block.cloneEmpty();
            else
                result.bitmap_block.setColumns(std::move(bitmap_columns));
        }
        /// block.rows can be empty if we only select bitmap index, e.g.
        /// SELECT arraySetCheck(vid, 1) FROM t WHERE arraySetCheck(vid, 1) OR arraySetCheck(vid, 2)arraySetCheck(vid, 1)
        size_t num_rows = block.rows() ? block.rows() : result.bitmap_block.rows();
        if (prewhere_info->prewhere_actions)
            prewhere_info->prewhere_actions->execute(block, &result.bitmap_block, num_rows);

        prewhere_column_pos = block.getPositionByName(prewhere_info->prewhere_column_name);

        result.columns.clear();
        result.columns.reserve(block.columns());
        for (auto & col : block)
            result.columns.emplace_back(std::move(col.column));

        filter.swap(result.columns[prewhere_column_pos]);
    }

    if (result.getFilter())
    {
        if (filter && !row_level_filter)
        {
            /// filter &= delete_bitmap
            filter = combineFilterEqualSize(std::move(filter), result.getFilterHolder());
        }
    }

    if (filter && row_level_filter)
    {
        row_level_filter = combineFilters(std::move(row_level_filter), filter);
        result.setFilter(row_level_filter);
    }
    else
        result.setFilter(filter);

    /// If there is a WHERE, we filter in there, and only optimize IO and shrink columns here
    if (!last_reader_in_chain)
        result.optimize(merge_tree_reader->canReadIncompleteGranules(), prewhere_info->row_level_filter == nullptr);

    /// If we read nothing or filter gets optimized to nothing
    if (result.totalRowsPerGranule() == 0)
        result.setFilterConstFalse();
    /// If we need to filter in PREWHERE
    else if (prewhere_info->need_filter || result.need_filter || prewhere_info->row_level_filter)
    {
        /// If there is a filter and without optimized
        if (result.getFilter() && last_reader_in_chain)
        {
            const auto * result_filter = result.getFilter();
            /// optimize is not called, need to check const 1 and const 0
            size_t bytes_in_filter = result.countBytesInResultFilter(result_filter->getData());
            if (bytes_in_filter == 0)
                result.setFilterConstFalse();
            else if (bytes_in_filter == result.num_rows)
                result.setFilterConstTrue();
        }

        /// If there is still a filter, do the filtering now
        if (result.getFilter())
        {
            /// filter might be shrunk while columns not
            const auto * result_filter = result.getFilterOriginal();

            if (row_level_filter)
                filterResult(result, filter);
            else
                filterResult(result, result_filter->getData());

            result.need_filter = true;

            bool has_column = false;
            for (auto & column : result.columns)
            {
                if (column)
                {
                    has_column = true;
                    result.num_rows = column->size();
                    break;
                }
            }

            /// There is only one filter column. Record the actual number
            if (!has_column)
                result.num_rows = result.countBytesInResultFilter(result_filter->getData());
        }

        /// Check if the PREWHERE column is needed
        if (!result.columns.empty())
        {
            if (prewhere_info->remove_prewhere_column)
                result.columns.erase(result.columns.begin() + prewhere_column_pos);
            else
                result.columns[prewhere_column_pos] =
                        getSampleBlock().getByName(prewhere_info->prewhere_column_name).type->
                                createColumnConst(result.num_rows, 1u)->convertToFullColumnIfConst();
        }
    }
    /// Filter in WHERE instead
    else
    {
        auto type = getSampleBlock().getByName(prewhere_info->prewhere_column_name).type;
        ColumnWithTypeAndName col(result.getFilterHolder()->convertToFullColumnIfConst(), std::make_shared<DataTypeUInt8>(), "");
        result.columns[prewhere_column_pos] = castColumn(col, type);
        result.clearFilter(); // Acting as a flag to not filter in PREWHERE
    }
}

void MergeTreeRangeReader::extractBitmapIndexColumns(Columns & columns, Block & bitmap_block)
{
    if (!merge_tree_reader->hasBitmapIndexReader())
        return;
    auto num_columns = merge_tree_reader->getColumns().size();
    const auto & name_and_types = merge_tree_reader->getBitmapColumns();
    size_t i = 0;
    for (auto name_and_type = name_and_types.begin(); i < name_and_types.size(); ++i, ++name_and_type)
        bitmap_block.insert({std::move(columns[num_columns+i]), name_and_type->type, name_and_type->name});

    columns.resize(num_columns);
}

}
