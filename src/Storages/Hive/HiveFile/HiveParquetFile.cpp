#include "Storages/Hive/HiveFile/HiveParquetFile.h"
#include <memory>
#include <Poco/Logger.h>
#if USE_HIVE

#include "Processors/Formats/Impl/ArrowBufferedStreams.h"
#include "Processors/Formats/Impl/ArrowColumnToCHColumn.h"
#include "Processors/Formats/Impl/ParquetBlockInputFormat.h"
#include "Formats/FormatFactory.h"

#include <parquet/arrow/reader.h>
#include <parquet/statistics.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

#define THROW_ARROW_NOT_OK(status)                                     \
    do                                                                 \
    {                                                                  \
        if (const ::arrow::Status & _s = (status); !_s.ok())           \
            throw Exception(_s.ToString(), ErrorCodes::BAD_ARGUMENTS); \
    } while (false)

HiveParquetFile::HiveParquetFile() = default;
HiveParquetFile::~HiveParquetFile() = default;

std::optional<size_t> HiveParquetFile::numRows()
{
    if (num_rows)
        return *num_rows;

    auto seekable_buffer = readFile(ReadSettings{});
    std::unique_ptr<parquet::arrow::FileReader> file_reader;
    std::atomic<int> stopped;
    THROW_ARROW_NOT_OK(parquet::arrow::OpenFile(
        asArrowFile(*seekable_buffer, FormatSettings{}, stopped, "Parquet", PARQUET_MAGIC_BYTES, /* avoid_buffering */ true),
        arrow::default_memory_pool(), &file_reader));

    auto metadata = file_reader->parquet_reader()->metadata();
    num_rows = metadata->num_rows();
    return *num_rows;
}

SourcePtr HiveParquetFile::getReader(const Block & block, const std::shared_ptr<IHiveFile::ReadParams> & params)
{
    auto read_buf = readFile(params->read_settings);
    auto parquet_format = FormatFactory::instance().getInputFormat(
        "Parquet", *read_buf, block, params->context, params->max_block_size, params->format_settings);

    parquet_format->addBuffer(std::move(read_buf));

    if (params->query_info)
        parquet_format->setQueryInfo(*params->query_info, params->context);
    return parquet_format;
}

}

#endif
