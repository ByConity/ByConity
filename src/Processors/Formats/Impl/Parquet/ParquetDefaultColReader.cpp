#include "ParquetDefaultColReader.h"
#include "Core/ColumnWithTypeAndName.h"

namespace DB
{

ParquetDefaultColReader::ParquetDefaultColReader(const DataTypePtr & ch_type_) : ch_type(ch_type_)
{
}

ColumnWithTypeAndName ParquetDefaultColReader::readBatch(const String & column_name, size_t batch_size, const IColumn::Filter *)
{
    ColumnWithTypeAndName col(ch_type, column_name);
    col.column = col.column->cloneResized(batch_size);
    return col;
}

void ParquetDefaultColReader::skip(size_t)
{
}

}
