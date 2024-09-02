#pragma once

#include "ParquetColumnReader.h"

namespace DB
{
class ArrowColumnToCHColumn;

class ParquetDefaultColReader : public ParquetColumnReader
{
public:
    explicit ParquetDefaultColReader(
        const DataTypePtr & ch_type_);

    ~ParquetDefaultColReader() override = default;

    ColumnWithTypeAndName readBatch(const String & column_name, size_t batch_size, const IColumn::Filter * filter) override;
    void skip(size_t batch_size) override;

private:
    DataTypePtr ch_type;
};

}
