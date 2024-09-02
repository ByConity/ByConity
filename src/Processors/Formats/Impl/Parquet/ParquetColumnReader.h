#pragma once

#include <Columns/IColumn.h>
#include <Core/ColumnWithTypeAndName.h>
#include "Columns/FilterDescription.h"
#include "Core/NamesAndTypes.h"

namespace parquet
{

class PageReader;
class ColumnChunkMetaData;
class DataPageV1;
class DataPageV2;
class DictionaryPage;
class Page;

}

namespace DB
{

class ParquetColumnReader
{
public:
    virtual ColumnWithTypeAndName readBatch(const String & column_name, size_t batch_size, const IColumn::Filter * filter) = 0;
    virtual void skip(size_t batch_size) = 0;

    virtual ~ParquetColumnReader() = default;
};

using ParquetColReaderPtr = std::unique_ptr<ParquetColumnReader>;
using ParquetColReaders = std::vector<ParquetColReaderPtr>;

}
