#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnConst.h>
#include <Core/Field.h>


namespace DB
{

String IColumn::dumpStructure() const
{
    WriteBufferFromOwnString res;
    res << getFamilyName() << "(size = " << size();

    forEachSubcolumn([&](const auto & subcolumn)
    {
        res << ", " << subcolumn->dumpStructure();
    });

    res << ")";
    return res.str();
}

void IColumn::insertFrom(const IColumn & src, size_t n)
{
    insert(src[n]);
}

ColumnPtr IColumn::createWithOffsets(const Offsets & offsets, const Field & default_field, size_t total_rows, size_t shift) const
{
    if (offsets.size() + shift != size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Incompatible sizes of offsets ({}), shift ({}) and size of column {}", offsets.size(), shift, size());

    auto res = cloneEmpty();
    res->reserve(total_rows);

    ssize_t current_offset = -1;
    for (size_t i = 0; i < offsets.size(); ++i)
    {
        ssize_t offsets_diff = static_cast<ssize_t>(offsets[i]) - current_offset;
        current_offset = offsets[i];

        if (offsets_diff > 1)
            res->insertMany(default_field, offsets_diff - 1);

        res->insertFrom(*this, i + shift);
    }

    ssize_t offsets_diff = static_cast<ssize_t>(total_rows) - current_offset;
    if (offsets_diff > 1)
        res->insertMany(default_field, offsets_diff - 1);

    return res;
}

void IColumn::forEachSubcolumn(ColumnCallback callback) const
{
    const_cast<IColumn*>(this)->forEachSubcolumn([&callback](WrappedPtr & subcolumn)
            {
            callback(std::as_const(subcolumn));
            });
}

void IColumn::forEachSubcolumnRecursively(RecursiveColumnCallback callback) const
{
    std::as_const(*this).forEachSubcolumnRecursively([&callback](const IColumn & subcolumn)
    {
        callback(const_cast<IColumn &>(subcolumn));
    });
}

bool isColumnNullable(const IColumn & column)
{
    return checkColumn<ColumnNullable>(column);
}

bool isColumnConst(const IColumn & column)
{
    return checkColumn<ColumnConst>(column);
}

ColumnPtr IColumn::replaceFrom(
    const PaddedPODArray<UInt32> & indexes,
    const IColumn & rhs,
    const PaddedPODArray<UInt32> & rhs_indexes,
    const Filter * is_default_filter,
    const Filter * filter,
    bool enable_merge_map) const
{
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
                res->insertFrom(*this, pos);
            else
                res->insertRangeFrom(*this, pos, max_dis - pos);
            pos = max_dis;
        }
        if (pos == num_rows)
            break;
        if (filter && (*filter)[pos] == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The row {} to be replaced can not be filtered.", pos);
        else if (replaceRow(indexes, rhs, rhs_indexes, is_default_filter, pos, i, j, res, enable_merge_map))
            ++pos;
        // Update the pointer of the first part and the second part
        while (i < rhs_pos && index == indexes[i])
            ++i;
        if (j < indexes.size() && index == indexes[j] - num_rows)
            ++j;
    }
    return res;
}

bool IColumn::replaceRow(
    const PaddedPODArray<UInt32> & indexes,
    const IColumn & rhs,
    const PaddedPODArray<UInt32> & rhs_indexes,
    const Filter * is_default_filter,
    size_t current_pos,
    size_t & first_part_pos,
    size_t & second_part_pos,
    MutablePtr & res,
    bool) const
{
    // If it is not default value, just keep origin value.
    if (is_default_filter && !(*is_default_filter)[current_pos])
        return false;
    // Check that the replace behavior belongs to the first part or the second part.
    // The same index may appear multiple times in first part and only at most once in second part.
    while (first_part_pos < indexes.size() && indexes[first_part_pos] == current_pos)
    {
        if (rhs_indexes[first_part_pos] >= size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "rhs_index {} is out of this column size {}", rhs_indexes[first_part_pos], size());
        if (is_default_filter && !(*is_default_filter)[rhs_indexes[first_part_pos]]) /// If it's not default value, use it to replace
        {
            res->insertFrom(*this, rhs_indexes[first_part_pos]);
            return true;
        }
        first_part_pos++;
    }
    if (second_part_pos < indexes.size() && indexes[second_part_pos] - size() == current_pos)
    {
        if (rhs_indexes[second_part_pos] >= rhs.size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "rhs_index {} is out of rhs column size {}", rhs_indexes[second_part_pos], rhs.size());
        res->insertFrom(rhs, rhs_indexes[second_part_pos]);
        return true;
    }
    // If there has no previous valid data, use the first value as result.
    if (first_part_pos == 0 || indexes[first_part_pos - 1] != current_pos)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "first_part_pos {} is invalid.", first_part_pos);
    res->insertFrom(*this, rhs_indexes[first_part_pos - 1]);
    return true;
}

ColumnPtr IColumn::replaceWithDefaultValue(Filter & set_to_null_rows)
{
    MutablePtr res = cloneEmpty();
    size_t num_rows = size();
    res->reserve(num_rows);
    size_t pos = 0;
    while (pos < num_rows)
    {
        if (!set_to_null_rows[pos])
            res->insertFrom(*this, pos);
        else
            res->insertDefault();
        pos++;
    }
    return res;
}
}
