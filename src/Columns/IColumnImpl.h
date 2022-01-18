#pragma once
/**
  * This file implements template methods of IColumn that depend on other types
  * we don't want to include.
  * Currently, this is only the scatterImpl method that depends on PODArray
  * implementation.
  */

#include <Columns/IColumn.h>
#include <Common/PODArray.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

template <typename Derived>
std::vector<IColumn::MutablePtr> IColumn::scatterImpl(ColumnIndex num_columns,
                                             const Selector & selector) const
{
    size_t num_rows = size();

    if (num_rows != selector.size())
        throw Exception(
                "Size of selector: " + std::to_string(selector.size()) + " doesn't match size of column: " + std::to_string(num_rows),
                ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    std::vector<MutablePtr> columns(num_columns);
    for (auto & column : columns)
        column = cloneEmpty();

    {
        size_t reserve_size = num_rows * 1.1 / num_columns;    /// 1.1 is just a guess. Better to use n-sigma rule.

        if (reserve_size > 1)
            for (auto & column : columns)
                column->reserve(reserve_size);
    }

    for (size_t i = 0; i < num_rows; ++i)
        static_cast<Derived &>(*columns[selector[i]]).insertFrom(*this, i);

    return columns;
}

template <typename Derived, bool reversed, bool use_indexes>
void IColumn::compareImpl(const Derived & rhs, size_t rhs_row_num,
                          PaddedPODArray<UInt64> * row_indexes [[maybe_unused]],
                          PaddedPODArray<Int8> & compare_results,
                          int nan_direction_hint) const
{
    size_t num_rows = size();
    size_t num_indexes = num_rows;
    UInt64 * indexes [[maybe_unused]];
    UInt64 * next_index [[maybe_unused]];

    if constexpr (use_indexes)
    {
        num_indexes = row_indexes->size();
        next_index = indexes = row_indexes->data();
    }

    compare_results.resize(num_rows);

    if (compare_results.empty())
        compare_results.resize(num_rows);
    else if (compare_results.size() != num_rows)
        throw Exception(
                "Size of compare_results: " + std::to_string(compare_results.size()) + " doesn't match rows_num: " + std::to_string(num_rows),
                ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    for (size_t i = 0; i < num_indexes; ++i)
    {
        UInt64 row = i;

        if constexpr (use_indexes)
            row = indexes[i];

        int res = compareAt(row, rhs_row_num, rhs, nan_direction_hint);

        /// We need to convert int to Int8. Sometimes comparison return values which do not fit in one byte.
        if (res < 0)
            compare_results[row] = -1;
        else if (res > 0)
            compare_results[row] = 1;
        else
            compare_results[row] = 0;

        if constexpr (reversed)
            compare_results[row] = -compare_results[row];

        if constexpr (use_indexes)
        {
            if (compare_results[row] == 0)
            {
                *next_index = row;
                ++next_index;
            }
        }
    }

    if constexpr (use_indexes)
        row_indexes->resize(next_index - row_indexes->data());
}

template <typename Derived>
void IColumn::doCompareColumn(const Derived & rhs, size_t rhs_row_num,
                              PaddedPODArray<UInt64> * row_indexes,
                              PaddedPODArray<Int8> & compare_results,
                              int direction, int nan_direction_hint) const
{
    if (direction < 0)
    {
        if (row_indexes)
            compareImpl<Derived, true, true>(rhs, rhs_row_num, row_indexes, compare_results, nan_direction_hint);
        else
            compareImpl<Derived, true, false>(rhs, rhs_row_num, row_indexes, compare_results, nan_direction_hint);
    }
    else
    {
        if (row_indexes)
            compareImpl<Derived, false, true>(rhs, rhs_row_num, row_indexes, compare_results, nan_direction_hint);
        else
            compareImpl<Derived, false, false>(rhs, rhs_row_num, row_indexes, compare_results, nan_direction_hint);
    }
}

template <typename Derived, bool has_rhs_indexes, bool has_filter>
IColumn::Ptr IColumn::replaceFromImpl(
    const PaddedPODArray<UInt32> & indexes,
    const Derived & rhs, const PaddedPODArray<UInt32> * rhs_indexes,
    const Filter * filter) const
{
    size_t num_rows = size();
    if constexpr (has_rhs_indexes)
    {
        if (indexes.size() != rhs_indexes->size())
            throw Exception(
                    "Size of indexes: " + std::to_string(indexes.size()) + " doesn't match rhs_indexes: " + std::to_string(rhs_indexes->size()),
                    ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
    }
    else
    {
        if (indexes.size() != rhs.size())
            throw Exception(
                    "Size of indexes: " + std::to_string(indexes.size()) + " doesn't match rhs: " + std::to_string(rhs.size()),
                    ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    }
    if constexpr (has_filter)
    {
        if (num_rows != filter->size())
            throw Exception(
                    "Size of filter: " + std::to_string(filter->size()) + " doesn't match num_rows: " + std::to_string(num_rows),
                    ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
    }

    if (indexes.empty())
        return getPtr();

    MutablePtr res = cloneEmpty();
    res->reserve(num_rows);

    size_t pos = 0;
    for (size_t i = 0; i < indexes.size(); ++i)
    {
        const size_t index = indexes[i];
        if (pos < index)
        {
            static_cast<Derived &>(*res).insertRangeFrom(*this, pos, index - pos);
            pos = index;
        }
        if constexpr (has_filter)
        {
            if ((*filter)[pos] == 0)
                continue;
        }
        if constexpr (has_rhs_indexes)
            static_cast<Derived &>(*res).insertFrom(rhs, (*rhs_indexes)[i]);
        else
            static_cast<Derived &>(*res).insertFrom(rhs, i);
        ++pos;
    }
    if (pos < num_rows)
        static_cast<Derived &>(*res).insertRangeFrom(*this, pos, num_rows - pos);

    return res;
}

template <typename Derived>
IColumn::Ptr IColumn::doReplaceFrom(
    const PaddedPODArray<UInt32> & indexes,
    const Derived & rhs, const PaddedPODArray<UInt32> * rhs_indexes,
    const Filter * filter) const
{
    if (rhs_indexes)
    {
        if (filter)
            return replaceFromImpl<Derived, true, true>(indexes, rhs, rhs_indexes, filter);
        else
            return replaceFromImpl<Derived, true, false>(indexes, rhs, rhs_indexes, filter);
    }
    else
    {
        if (filter)
            return replaceFromImpl<Derived, false, true>(indexes, rhs, rhs_indexes, filter);
        else
            return replaceFromImpl<Derived, false, false>(indexes, rhs, rhs_indexes, filter);
    }
}

template <typename Derived>
bool IColumn::hasEqualValuesImpl() const
{
    size_t num_rows = size();
    for (size_t i = 1; i < num_rows; ++i)
    {
        if (compareAt(i, 0, static_cast<const Derived &>(*this), false) != 0)
            return false;
    }
    return true;
}

}
