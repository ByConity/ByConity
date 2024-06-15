#include <atomic>
#include <cstdint>
#include <stdio.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/FilterDescription.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/castColumn.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/LateMaterialize/MergeTreeRangeReaderLM.h>
#include <fmt/core.h>
#include <fmt/ostream.h>
#include <sys/types.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>
#include <common/range.h>

#ifdef __SSE2__
#    include <emmintrin.h>
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


static size_t filterColumns(Columns & columns, const IColumn::Filter & filter, ssize_t sz = -1)
{
    size_t filter_ret_num = sz;
    for (auto & column : columns)
    {
        if (column)
        {
            column = column->filter(filter, sz);

            if (column->empty())
            {
                columns.clear();
                return filter_ret_num;
            }
            filter_ret_num = filter_ret_num > column->size() ? column->size() : filter_ret_num;
        }
    }
    return filter_ret_num;
}

static size_t filterColumns(Columns & columns, const ColumnPtr & filter, ssize_t sz = -1)
{
    ConstantFilterDescription const_descr(*filter);
    if (const_descr.always_true)
        return sz;

    if (const_descr.always_false)
    {
        for (auto & col : columns)
            if (col)
                col = col->cloneEmpty();

        return 0;
    }

    FilterDescription descr(*filter);
    return filterColumns(columns, *descr.data, sz);
}

static void filterBlock(Block & block, const IColumn::Filter & filter, ssize_t sz = -1)
{
    for (auto & c : block)
    {
        if (c.column)
        {
            c.column = c.column->filter(filter, sz);

            if (c.column->empty())
            {
                block.clear();
                return;
            }
        }
    }
}

static void filterBlock(Block & block, const ColumnPtr & filter, ssize_t sz = -1)
{
    ConstantFilterDescription const_descr(*filter);
    if (const_descr.always_true)
        return;

    if (const_descr.always_false)
    {
        for (auto & c : block)
            if (c.column)
                c.column = c.column->cloneEmpty();

        return;
    }

    FilterDescription descr(*filter);
    filterBlock(block, *descr.data, sz);
}

MergeTreeRangeReaderLM::MergeTreeRangeReaderLM(
    IMergeTreeReader * merge_tree_reader_,
    MergeTreeRangeReaderLM * prev_reader_,
    const AtomicPredicateExpr * atomic_predicate_,
    ImmutableDeleteBitmapPtr delete_bitmap_,
    bool last_reader_in_chain_)
    : merge_tree_reader(merge_tree_reader_)
    , index_granularity(&(merge_tree_reader->data_part->index_granularity))
    , prev_reader(prev_reader_)
    , atomic_predicate(atomic_predicate_)
    , delete_bitmap(delete_bitmap_)
    , last_reader_in_chain(last_reader_in_chain_)
    , has_bitmap_index(merge_tree_reader->hasBitmapIndexReader())
{
    if (prev_reader_)
        sample_block = prev_reader->getSampleBlock();

    for (const auto & name_and_type : merge_tree_reader->getColumns())
        sample_block.insert({name_and_type.type->createColumn(), name_and_type.type, name_and_type.name});

    Block bitmap_block;
    if (has_bitmap_index)
    {
        for (const auto & name_and_type : merge_tree_reader->getBitmapColumns())
        {
            bitmap_block.insert({name_and_type.type->createColumn(), name_and_type.type, name_and_type.name});
        }
    }

    if (atomic_predicate)
    {
        size_t rows = sample_block.rows();

        if (atomic_predicate->predicate_actions)
        {
            atomic_predicate->predicate_actions->execute(sample_block, &bitmap_block, rows, true);
        }

        if (atomic_predicate->remove_filter_column && sample_block.has(atomic_predicate->filter_column_name))
        {
            sample_block.erase(atomic_predicate->filter_column_name);
        }
    }
}

bool MergeTreeRangeReaderLM::isReadingFinished() const
{
    return prev_reader ? prev_reader->isReadingFinished() : stream.isFinished();
}


bool MergeTreeRangeReaderLM::isCurrentRangeFinished() const
{
    return prev_reader ? prev_reader->isCurrentRangeFinished() : stream.isFinished();
}

static void checkCombinedFiltersSize(size_t bytes_in_first_filter, size_t second_filter_size)
{
    if (bytes_in_first_filter != second_filter_size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot combine filters because number of bytes in a first filter ({}) "
            "does not match second filter size ({})",
            bytes_in_first_filter,
            second_filter_size);
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

    auto * dst = typeid_cast<ColumnUInt8 *>(mut_first.get())->getData().data();
    auto * end = dst + second->size();
    const auto * src = second_descr.data->data();

    /// TODO @canhld:
    /// 1. can use aligned load/store?
    /// 2. can be more efficient with bit unpacking instructions
    /// Notes: when using simd instructions, we use `while(dst < end)` without guarding because we assume
    /// that the columns data is padded right at least 32 bytes, look at PODArray implementation.

#if defined(__AVX2__)
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
#elif defined(__SSE2__)
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


int MergeTreeRangeReaderLM::evaluatePredicateAndCombine(ReadResult & result)
{
    if (!atomic_predicate || !atomic_predicate->predicate_actions)
        return -1;

#ifndef NDEBUG
    fmt::print(stderr, "Executing predicates\n{}", atomic_predicate->predicate_actions->dumpActions());
#endif
    const auto & header = merge_tree_reader->getColumns();
    size_t num_columns = header.size();

    ColumnPtr filter;
    int filter_column_pos;
    {
        /// Restore block from columns list
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
#ifndef NDEBUG
        fmt::print(stderr, "Block from prev stages: {}\n", block.dumpStructure());
#endif
        size_t i = 0;
        for (auto name_and_type = header.begin(); i < num_columns; ++pos, ++name_and_type, ++i)
            block.insert({result.columns[pos], name_and_type->type, name_and_type->name});
#ifndef NDEBUG
        fmt::print(stderr, "Block before execute: {}\n", block.dumpStructure());
#endif
        size_t num_rows = result.num_rows;
        if (atomic_predicate->predicate_actions)
            atomic_predicate->predicate_actions->execute(block, &result.bitmap_block, num_rows);
#ifndef NDEBUG
        fmt::print(stderr, "Block after execute: {}\n", block.dumpStructure());
#endif

        filter_column_pos = block.getPositionByName(atomic_predicate->filter_column_name);
        result.columns.clear();
        result.columns.reserve(block.columns());
        for (auto & col : block)
            result.columns.emplace_back(std::move(col.column));

        if (atomic_predicate->remove_filter_column)
        {
            filter.swap(result.columns[filter_column_pos]);
            result.columns.erase(result.columns.begin() + filter_column_pos);
            filter_column_pos = -1;
            result.last_filter_column_pos = -1;
        }
        else
        {
            filter = result.columns[filter_column_pos];
            result.filter_type = block.getByPosition(filter_column_pos).type;
            result.last_filter_column_pos = filter_column_pos;
        }
    }

    /// If this is row-level policy predicate, update the row_filter, otherwise update filter
    if (atomic_predicate->is_row_filter)
    {
        if (result.getRowFilter())
        {
            filter = combineFilterEqualSize(std::move(filter), result.getRowFilterHolder());
        }
        result.setRowFilter(filter);
        if (result.getRowFilter())
        {
            filterColumns(result.columns, result.getRowFilterHolder());
            result.num_rows = result.countBytesInResultFilter(result.getRowFilter()->getData());
        }
    }
    else
    {
        result.setFilter(filter);
    }
    return filter_column_pos;
}

ReadResult MergeTreeRangeReaderLM::read(MarkRanges & ranges, bool need_filter)
{
    ReadResult read_result;
    /// Read 1 granule, if current range is ended then get new range from `ranges`.
    /// The logic is different depending on where we are in the chain
    readImpl(read_result, ranges);

    if (read_result.num_rows > 0)
    {
        /// If this stage has predicate info, then generate new filter base on predicate actions
        /// and merge it wit previous filter
        evaluatePredicateAndCombine(read_result);
        /// Optimize the result again. If the filter is const true, clear the filter, if the filter
        /// is const false, clear all rows and columns; Return popcnt of the fitler
        auto popcnt = read_result.optimize();

        /// In case we cannot skip the whole granule, try to filter out the column before returning to
        /// higher stage in the pipeline (only at the last reader)
        if (last_reader_in_chain)
        {
            /// Just a guess, if we can filter more than 66% of rows then do filtering
            if (read_result.getFilter() && (3 * popcnt < read_result.num_rows || need_filter))
            {
#ifndef NDEBUG
                fmt::print(
                    stderr,
                    "Finish reading chain, will filter colums, size before filter {} rows, expect result in {} rows\n",
                    read_result.num_rows,
                    popcnt);
#endif
                /// Optimization: if we have a filter column, we can just replace it with a constant `1` column
                /// later, so we don't need to filter it
                if (read_result.last_filter_column_pos >= 0)
                    read_result.columns[read_result.last_filter_column_pos] = nullptr;
                size_t filter_ret_num = filterColumns(read_result.columns, read_result.getFilterHolder(), popcnt);
                if (read_result.bitmap_block.rows() > 0)
                    filterBlock(read_result.bitmap_block, read_result.getFilterHolder(), popcnt);
                read_result.num_rows = filter_ret_num == popcnt ? popcnt : filter_ret_num;
                if (read_result.last_filter_column_pos >= 0 && read_result.columns.size() > static_cast<size_t>(read_result.last_filter_column_pos))
                    read_result.columns[read_result.last_filter_column_pos] = read_result.filter_type->createColumnConst(read_result.num_rows, 1u);
                read_result.clearFilter();
            }
        }
    }

    return read_result;
}

void MergeTreeRangeReaderLM::readImpl(ReadResult & read_result, MarkRanges & ranges)
{
    if (prev_reader)
    {
        /// The chain has been started, will use read result from previous reader to
        /// skip granule if possible
        read_result = prev_reader->read(ranges);

        auto columns = continueReadingChain(read_result);

        if (read_result.num_rows == 0)
            return;

        size_t total_bytes = 0;
        for (auto & column : columns)
        {
            if (column)
                total_bytes += column->byteSize();
        }

        read_result.addNumBytesRead(total_bytes);

        bool should_evaluate_missing_defaults = false;

        /// Fill missing columns before filtering because some arrays from Nested may have empty data.
        merge_tree_reader->fillMissingColumns(columns, should_evaluate_missing_defaults, read_result.num_rows);

        if (!columns.empty())
        {
            /// If some columns absent in part, then evaluate default values
            if (should_evaluate_missing_defaults)
            {
                auto block = prev_reader->sample_block.cloneWithColumns(read_result.columns);
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
        /// The chain has not been started yet, just start reading and save read information
        /// to read_result. These information will be used by next stage to optimize reading.
        startReadingChain(read_result, ranges);
        read_result.num_rows = read_result.numReadRows();

        if (read_result.num_rows)
        {
            bool should_evaluate_missing_defaults;
            merge_tree_reader->fillMissingColumns(
                read_result.columns, should_evaluate_missing_defaults, read_result.num_rows);

            /// If some columns absent in part, then evaluate default values
            if (should_evaluate_missing_defaults)
            {
                Block additional_block;
                if (read_result.columns.size() == 1 && read_result.columns[0] == nullptr)
                {
                    /// If there's only 1 columns here, merge_tree_reader won't be able to know how many rows they it should create
                    /// for the column.. So just insert a dummy columns here... TODO @canh: is there any better way, like explicitly
                    /// tell the reader how many row we want to create.
                    additional_block.insert({DataTypeUInt8().createColumnConst(read_result.num_rows, 0), std::make_shared<DataTypeUInt8>(), "_dummy"});
                }
                merge_tree_reader->evaluateMissingDefaults(additional_block, read_result.columns);
            }

            /// If result not empty, then apply on-fly alter conversions if any required
            merge_tree_reader->performRequiredConversions(read_result.columns);
        }
        else
            read_result.columns.clear();

        size_t total_bytes = 0;
        for (auto & column : read_result.columns)
            total_bytes += column->byteSize();

        read_result.addNumBytesRead(total_bytes);

        if (read_result.getRowFilter() && (!atomic_predicate || !atomic_predicate->is_row_filter))
        {
            filterColumns(read_result.columns, read_result.getRowFilterHolder());
            auto num_rows = read_result.countBytesInResultFilter(read_result.getRowFilter()->getData());
            read_result.num_rows = num_rows;
        }
        extractBitmapIndexColumns(read_result.columns, read_result.bitmap_block);
    }
}

// Get one granules from ranges and read it
void MergeTreeRangeReaderLM::startReadingChain(ReadResult & result, MarkRanges & ranges)
{
    result.columns.resize(merge_tree_reader->numColumnsInResult());

    /// Current range is finish, add new range
    if (stream.isFinished())
    {
        stream = GranuledStream(ranges.front().begin, ranges.front().end, merge_tree_reader);
        result.addRange(ranges.front());
        ranges.pop_front();
    }
    size_t num_rows = stream.rowsInCurrentMark();
#ifndef NDEBUG
    std::stringstream ss;
    for (const auto & v : merge_tree_reader->getColumns())
    {
        ss << v.name << " ";
    }
    fmt::print(stderr, "Start reading chain for column {}, granules {}, {} rows\n", ss.str(), stream.current_mark, num_rows);
#endif
    /// Delete bitmap handling
    size_t read_pos = stream.position();
    size_t read_end = read_pos + num_rows;
    ColumnUInt8::MutablePtr delete_filter_column;
    bool delete_filter_always_true = true;
    if (delete_bitmap && delete_bitmap->containsRange(read_pos, read_end))
    {
        skip();
        return; /// Can skip this granule, jay..!
    }
    else if (delete_bitmap)
    {
        delete_filter_column = ColumnUInt8::create(num_rows, 1);
        UInt8 * filter_start = delete_filter_column->getData().data();
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
    if (delete_filter_always_true)
        delete_filter_column = nullptr;
#ifndef NDEBUG
    for (const auto & v : merge_tree_reader->getColumns())
    {
        per_column_read_granules[v.name]++;
    }
    if (has_bitmap_index)
    {
        for (const auto & v : merge_tree_reader->getBitmapColumns())
        {
            per_column_read_granules[v.name]++;
        }
    }
#endif
    /// Read the first granule in the range
    auto actual_rows = stream.read(result.columns).first;

    if (delete_filter_column)
    {
        /// Act just like a row-level policy filter
        ColumnPtr filter_column;
        if (actual_rows < num_rows)
            filter_column = delete_filter_column->cut(0, actual_rows);
        else
            filter_column = std::move(delete_filter_column);
        result.setRowFilter(filter_column);
    }
    result.addRows(actual_rows);
}

Columns MergeTreeRangeReaderLM::continueReadingChain(ReadResult & read_result)
{
#ifndef NDEBUG
    std::stringstream ss;
    for (const auto & v : merge_tree_reader->getColumns())
    {
        ss << v.name << " ";
    }
    fmt::print(stderr, "Continue reading chain for columns {}\n", ss.str());
#endif
    const auto & started_ranges = read_result.startedRanges();

    if (stream.isFinished())
    {
        const auto & range = started_ranges.back();
        stream = GranuledStream(range.begin, range.end, merge_tree_reader);
    }

    if (read_result.num_rows == 0)
    {
        skip();
        return {};
    }

    Columns columns;

    columns.resize(merge_tree_reader->numColumnsInResult());
#ifndef NDEBUG
    for (const auto & v : merge_tree_reader->getColumns())
    {
        per_column_read_granules[v.name]++;
    }
    if (has_bitmap_index)
    {
        for (const auto & v : merge_tree_reader->getBitmapColumns())
        {
            per_column_read_granules[v.name]++;
        }
    }
#endif
    auto num_rows = stream.read(columns).first;
    if (merge_tree_reader->getColumns().empty())
        return columns;
    if (num_rows && num_rows != read_result.numReadRows())
        throw Exception(
            "RangeReader read " + toString(num_rows) + " rows, but " + toString(read_result.numReadRows()) + " expected.",
            ErrorCodes::LOGICAL_ERROR);

    if (read_result.getRowFilter())
        filterColumns(columns, read_result.getRowFilterHolder());

    return columns;
}

void MergeTreeRangeReaderLM::skip()
{
#ifndef NDEBUG
    fmt::print(stderr, "Skip granule {}\n", stream.current_mark);
#endif
    /// Always assume stream is not finished
    stream.skip();
}

void MergeTreeRangeReaderLM::extractBitmapIndexColumns(Columns & columns, Block & bitmap_block)
{
    if (!merge_tree_reader->hasBitmapIndexReader())
        return;
    auto num_columns = merge_tree_reader->getColumns().size();
    const auto & name_and_types = merge_tree_reader->getBitmapColumns();
    size_t i = 0;
    for (auto name_and_type = name_and_types.begin(); i < name_and_types.size(); ++i, ++name_and_type)
        bitmap_block.insert({std::move(columns[num_columns+i]), name_and_type->type, name_and_type->name});

    // fmt::print(stderr, "bitmap block after update: {}\n", bitmap_block.dumpStructure());

    columns.resize(num_columns);
}
}
