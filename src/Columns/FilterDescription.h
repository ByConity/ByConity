#pragma once

#include <Columns/IColumn.h>


namespace DB
{

/// Support methods for implementation of WHERE, PREWHERE and HAVING.


/// Analyze if the column for filter is constant thus filter is always false or always true.
struct ConstantFilterDescription
{
    bool always_false = false;
    bool always_true = false;

    ConstantFilterDescription() = default;
    explicit ConstantFilterDescription(const IColumn & column);
};

ConstantFilterDescription merge(const ConstantFilterDescription & lhs, const ConstantFilterDescription & rhs);

/// Obtain a filter from non constant Column, that may have type: UInt8, Nullable(UInt8).
struct FilterDescription
{
    inline static std::atomic<size_t> fd_cost = 0;
    const IColumn::Filter * data = nullptr; /// Pointer to filter when it is not always true or always false.
    ColumnPtr data_holder;                  /// If new column was generated, it will be owned by holder.
    ColumnPtr filter(const IColumn & column, ssize_t result_size_hint) const { return column.filter(*data, result_size_hint); }
    bool hasOnes() const;

    explicit FilterDescription(const IColumn & column);
private:
    mutable Int64 has_one = -1;

};


struct ColumnWithTypeAndName;

}
