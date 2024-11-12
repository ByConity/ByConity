#pragma once

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>

namespace DB
{
class UniExtract
{
public:
    Field executeOnColumn(const ColumnPtr & column, const DataTypePtr & type);
    Field executeOnColumnArray(const ColumnPtr & column, const DataTypePtr & type);
};

class Min
{
public:
    Field executeOnColumn(const ColumnPtr & column, const DataTypePtr & type);
};

class Max
{
public:
    Field executeOnColumn(const ColumnPtr & column, const DataTypePtr & type);
};

class CountByGranularity
{
public:
    ColumnPtr executeOnColumn(const ColumnPtr & column, const DataTypePtr & type);
};

}
