#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnArray.h>
#include <Core/Field.h>


namespace DB
{

String IColumn::dumpStructure() const
{
    WriteBufferFromOwnString res;
    res << getFamilyName() << "(size = " << size();

    ColumnCallback callback = [&](ColumnPtr & subcolumn)
    {
        res << ", " << subcolumn->dumpStructure();
    };

    const_cast<IColumn*>(this)->forEachSubcolumn(callback);

    res << ")";
    return res.str();
}

void IColumn::insertFrom(const IColumn & src, size_t n)
{
    insert(src[n]);
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
