#pragma once

#include <parquet/file_reader.h>
#include <parquet/properties.h>
#include "Core/Block.h"
#include "Core/ColumnWithTypeAndName.h"
#include "Processors/Formats/Impl/Parquet/ParquetColumnReader.h"

namespace arrow { class RecordBatchReader; }
namespace parquet::arrow { class FileReader; }

namespace DB
{
class ArrowColumnToCHColumn;

class ParquetArrowColReader : public ParquetColumnReader
{
public:
    ParquetArrowColReader(
        const ColumnWithTypeAndName & ch_column,
        const std::vector<int> & column_indicies,
        const std::vector<int> & row_group_indicies,
        const parquet::ParquetFileReader & parquet_file_reader,
        std::shared_ptr<arrow::io::RandomAccessFile> arrow_file,
        const parquet::ArrowReaderProperties & properties,
        const FormatSettings & format_settings);

    ~ParquetArrowColReader() override;

    ColumnWithTypeAndName readBatch(const String & column_name, size_t batch_size, const IColumn::Filter * filter) override;
    void skip(size_t batch_size) override;

private:
    Block header;
    std::vector<int> column_indices;
    std::unique_ptr<parquet::arrow::FileReader> file_reader;
    std::shared_ptr<arrow::RecordBatchReader> record_batch_reader;
    std::unique_ptr<ArrowColumnToCHColumn> arrow_column_to_ch_column;
};

}
