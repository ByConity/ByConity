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

template <typename Derived>
IColumn::Ptr IColumn::doReplaceFrom(
    const PaddedPODArray<UInt32> & indexes,
    const Derived & rhs,
    const PaddedPODArray<UInt32> & rhs_indexes,
    const Filter * is_default_filter,
    const IColumn::Filter * filter) const
{
    // For more detail of usage, please see the description of method IColumn::replaceFrom.
    size_t num_rows = size();
    if (indexes.size() != rhs_indexes.size())
        throw Exception(
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
            "Size of indexes: {} doesn't match rhs_indexes: {}",
            indexes.size(),
            rhs_indexes.size());
    if (is_default_filter)
    {
        if (num_rows != is_default_filter->size())
            throw Exception(
                ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
                "Size of is_default_filter: {} doesn't match num_rows: {}",
                is_default_filter->size(),
                num_rows);
    }

    if (indexes.empty())
        return getPtr();

    // Find the boundary of the first part and the second part.
    size_t rhs_pos = indexes.size();
    for (size_t i = 0; i < indexes.size(); ++i)
    {
        if (indexes[i] >= num_rows)
        {
            rhs_pos = i;
            break;
        }
    }
    if (rhs_indexes.size() - rhs_pos != rhs.size())
        throw Exception(
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
            "Size of rhs {} doesn't match corresponding size of column {}",
            rhs_indexes.size() - rhs_pos,
            rhs.size());

    MutablePtr res = cloneEmpty();
    res->reserve(num_rows);

    size_t pos = 0, i = 0, j = rhs_pos;
    while (pos < num_rows)
    {
        size_t index = num_rows;
        if (i < rhs_pos && indexes[i] < index)
            index = indexes[i];
        if (j < indexes.size() && indexes[j] - num_rows < index)
            index = indexes[j] - num_rows;
        while (pos < index)
        {
            // Discard values according to filter
            if (filter)
            {
                while (pos < index && (*filter)[pos] == 0)
                    pos++;
                if (pos == index)
                    break;
            }
            // Try to batch insert as much as possible
            size_t max_dis = filter ? pos: index;
            if (filter)
            {
                while (max_dis < index && (*filter)[max_dis] == 1)
                    max_dis++;
            }
            if (max_dis - pos == 1)
                static_cast<Derived &>(*res).insertFrom(*this, pos);
            else
                static_cast<Derived &>(*res).insertRangeFrom(*this, pos, max_dis - pos);
            pos = max_dis;
        }
        if (pos == num_rows)
            break;
        if (filter && (*filter)[pos] == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The row {} to be replaced can not be filtered.", pos);
        else if (!is_default_filter || (*is_default_filter)[pos]) // If it is default value, replace the value with previous one.
        {
            // Check that the replace behavior belongs to the first part or the second part
            // The same index may appear multiple times in first part and only at most once in second part
            bool has_replaced = false;
            while (i < rhs_pos && index == indexes[i])
            {
                if (rhs_indexes[i] >= num_rows)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "rhs_index {} is out of this column size {}", rhs_indexes[i], num_rows);
                if (is_default_filter && !(*is_default_filter)[rhs_indexes[i]]) /// If it's not default value, use it to replace
                {
                    static_cast<Derived &>(*res).insertFrom(*this, rhs_indexes[i]);
                    has_replaced = true;
                    break;
                }
                i++;
            }
            if (!has_replaced && j < indexes.size())
            {
                if (rhs_indexes[j] >= rhs.size())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "rhs_index {} is out of rhs column size {}", rhs_indexes[j], rhs.size());
                static_cast<Derived &>(*res).insertFrom(rhs, rhs_indexes[j]);
                has_replaced = true;
            }
            // If all values to replace are in first part and they are all default-value, it will be false.
            if (has_replaced)
                ++pos;
        }
        // Update the pointer of the first part and the second part
        while (i < rhs_pos && index == indexes[i])
            ++i;
        if (j < indexes.size() && index == indexes[j] - num_rows)
            ++j;
    }
    return res;
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
