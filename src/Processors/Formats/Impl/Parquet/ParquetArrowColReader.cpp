#include "ParquetArrowColReader.h"

#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <parquet/properties.h>
#include "Core/Block.h"
#include "Core/ColumnWithTypeAndName.h"
#include "Formats/FormatSettings.h"
#include "Processors/Formats/Impl/ArrowColumnToCHColumn.h"

namespace DB
{

#define THROW_ARROW_NOT_OK(status)                                     \
    do                                                                 \
    {                                                                  \
        if (::arrow::Status _s = (status); !_s.ok())                   \
            throw Exception(_s.ToString(), ErrorCodes::BAD_ARGUMENTS); \
    } while (false)

ParquetArrowColReader::ParquetArrowColReader(
    const ColumnWithTypeAndName & ch_column,
    const std::vector<int> & column_indicies,
    const std::vector<int> & row_group_indicies,
    const parquet::ParquetFileReader & parquet_file_reader,
    std::shared_ptr<arrow::io::RandomAccessFile> arrow_file,
    const parquet::ArrowReaderProperties & properties,
    const FormatSettings & format_settings)
    : header({ch_column})
{
    parquet::arrow::FileReaderBuilder builder;
    THROW_ARROW_NOT_OK(
        builder.Open(
            arrow_file,
            /* not to be confused with ArrowReaderProperties */
            parquet::default_reader_properties(),
            parquet_file_reader.metadata()));

    builder.properties(properties);
    // TODO: Pass custom memory_pool() to enable memory accounting with non-jemalloc allocators.
    THROW_ARROW_NOT_OK(builder.Build(&file_reader));

    THROW_ARROW_NOT_OK(
        file_reader->GetRecordBatchReader(
            row_group_indicies,
            column_indicies,
            &record_batch_reader));

    arrow_column_to_ch_column = std::make_unique<ArrowColumnToCHColumn>(
        header,
        "Parquet",
        format_settings.parquet.import_nested,
        format_settings.parquet.allow_missing_columns,
        format_settings.null_as_default,
        format_settings.date_time_overflow_behavior,
        format_settings.parquet.case_insensitive_column_matching);
}

ParquetArrowColReader::~ParquetArrowColReader() = default;

ColumnWithTypeAndName ParquetArrowColReader::readBatch([[maybe_unused]] const String & column_name, size_t batch_size, [[maybe_unused]] const IColumn::Filter * filter)
{
    auto batch = record_batch_reader->Next();
    if (!batch.ok())
        throw ParsingException(ErrorCodes::CANNOT_READ_ALL_DATA, "Error while reading Parquet data: {}", batch.status().ToString());

    auto tmp_table = arrow::Table::FromRecordBatches({*batch});
    size_t num_rows = (*tmp_table)->num_rows();
    if (num_rows != batch_size)
        throw Exception("Expected to read {} rows, but got {} rows", batch_size, num_rows);

    Chunk chunk;
    arrow_column_to_ch_column->arrowTableToCHChunk(chunk, *tmp_table, num_rows);
    auto block = header.cloneWithColumns(chunk.detachColumns());
    return block.getByPosition(0);
}

void ParquetArrowColReader::skip(size_t batch_size)
{
    auto batch = record_batch_reader->Next();
    if (!batch.ok())
        throw ParsingException(ErrorCodes::CANNOT_READ_ALL_DATA, "Error while reading Parquet data: {}", batch.status().ToString());

    size_t num_rows = (*batch)->num_rows();
    if (num_rows != batch_size)
        throw Exception("Expected to skip {} rows, but got {} rows", batch_size, num_rows);
}

}
