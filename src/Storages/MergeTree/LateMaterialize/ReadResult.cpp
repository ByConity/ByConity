#include <Storages/MergeTree/LateMaterialize/ReadResult.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/FilterDescription.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnConst.h>
#include <common/range.h>

#ifdef __SSE2__
#include <emmintrin.h>
#endif

namespace DB
{
namespace LateMaterialize
{


void ReadResult::setFilterConstTrue()
{
    /// filter == nullptr means no filter at all
    filter = nullptr;
    filter_holder = DataTypeUInt8().createColumnConst(num_rows, 1u);
}

void ReadResult::setFilterConstFalse()
{
    filter = nullptr;
    filter_holder = nullptr;
    columns.clear();
    num_rows = 0;
}

size_t ReadResult::optimize()
{
    if (num_rows == 0 || filter == nullptr)
        return 0;
    auto popcnt = countBytesInResultFilter(filter->getData());
    if (popcnt == filter->size())
    {
        /// Filter = null mean no filter at all
        setFilterConstTrue(); 
    }
    else if (popcnt == 0)
    {
        /// Will clear everything
        setFilterConstFalse();
    }
    return popcnt;
}

void ReadResult::setFilter(const ColumnPtr & new_filter)
{
    if (!new_filter && filter)
        throw Exception("Can't replace existing filter with empty.", ErrorCodes::LOGICAL_ERROR);

    if (filter)
    {
        size_t new_size = new_filter->size();

        if (new_size != num_rows)
            throw Exception("Can't set filter because it's size is " + toString(new_size) + " but "
                            + toString(num_rows) + " rows was read.", ErrorCodes::LOGICAL_ERROR);
    }

    FilterDescription filter_description(*new_filter);
    filter_holder = filter_description.data_holder ? filter_description.data_holder : new_filter;
    filter = typeid_cast<const ColumnUInt8 *>(filter_holder.get());
    if (!filter)
        throw Exception("setFilter function expected ColumnUInt8.", ErrorCodes::LOGICAL_ERROR);
}

void ReadResult::setRowFilter(const ColumnPtr & new_filter)
{
    if (!new_filter && row_filter)
        throw Exception("Can't replace existing filter with empty.", ErrorCodes::LOGICAL_ERROR);

    if (row_filter)
    {
        size_t new_size = new_filter->size();

        if (new_size != num_read_rows)
            throw Exception("Can't set filter because it's size is " + toString(new_size) + " but "
                            + toString(num_read_rows) + " rows was read.", ErrorCodes::LOGICAL_ERROR);
    }

    ConstantFilterDescription const_description(*new_filter);
    if (const_description.always_true)
    {
        /// Filter = null mean no filter at all
        row_filter = nullptr;
        row_filter_holder = DataTypeUInt8().createColumnConst(num_rows, 1u);
    }
    else if (const_description.always_false)
    {
        /// Clear everything
        setFilterConstFalse();
        row_filter = nullptr;
        row_filter_holder = nullptr;
    }
    else
    {
        FilterDescription filter_description(*new_filter);
        row_filter_holder = filter_description.data_holder ? filter_description.data_holder : new_filter;
        row_filter = typeid_cast<const ColumnUInt8 *>(row_filter_holder.get());
        if (!row_filter)
            throw Exception("setRowFilter function expected ColumnUInt8.", ErrorCodes::LOGICAL_ERROR);
    }
}

size_t ReadResult::countBytesInResultFilter(const IColumn::Filter & filter_)
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

}
}
