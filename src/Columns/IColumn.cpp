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
