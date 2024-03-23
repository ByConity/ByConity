#include "Storages/Hive/HiveFile/HiveParquetFile.h"
#include <memory>
#include <Poco/Logger.h>
#if USE_HIVE

#include "Processors/Formats/Impl/ArrowBufferedStreams.h"
#include "Processors/Formats/Impl/ArrowColumnToCHColumn.h"
#include "Processors/Formats/Impl/ParquetBlockInputFormat.h"

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

template <class FieldType, class StatisticsType>
Range createRangeFromParquetStatistics(std::shared_ptr<StatisticsType> stats)
{
    if (!stats->HasMinMax())
        return Range::createWholeUniverseWithoutNull();
    return Range(FieldType(stats->min()), true, FieldType(stats->max()), true);
}

Range createRangeFromParquetStatistics(std::shared_ptr<parquet::ByteArrayStatistics> stats)
{
    if (!stats->HasMinMax())
        return Range::createWholeUniverseWithoutNull();

    String min_val(reinterpret_cast<const char *>(stats->min().ptr), stats->min().len);
    String max_val(reinterpret_cast<const char *>(stats->max().ptr), stats->max().len);
    return Range(min_val, true, max_val, true);
}

HiveParquetFile::HiveParquetFile() = default;
HiveParquetFile::~HiveParquetFile() = default;

size_t HiveParquetFile::numSlices() const
{
    openFile();
    return metadata->num_row_groups();
}

std::optional<size_t> HiveParquetFile::numRows() const
{
    openFile();
    return metadata->num_rows();
}

void HiveParquetFile::openFile() const
{
    std::lock_guard lock(mutex);
    /// file_reader and buf may be empty
    if (schema)
        return;

    auto seekable_buffer = readFile(ReadSettings{});
    std::atomic<int> stopped;
    THROW_ARROW_NOT_OK(parquet::arrow::OpenFile(
        asArrowFile(*seekable_buffer, FormatSettings{}, stopped, "Parquet", PARQUET_MAGIC_BYTES, /* avoid_buffering */ true),
        arrow::default_memory_pool(), &file_reader));
    buf = std::move(seekable_buffer);
    metadata = file_reader->parquet_reader()->metadata();
    THROW_ARROW_NOT_OK(file_reader->GetSchema(&schema));
}

void HiveParquetFile::loadSplitMinMaxIndex(const NamesAndTypesList & index_names_and_types)
{
    openFile();

    size_t num_cols = metadata->num_columns();
    size_t num_row_groups = metadata->num_row_groups();
    const auto * schema_descriptor = metadata->schema();

    std::map<String, size_t> parquet_column_positions;
    for (size_t pos = 0; pos < num_cols; ++pos)
    {
        String column{schema_descriptor->Column(static_cast<int>(pos))->name()};
        boost::to_lower(column);
        parquet_column_positions[column] = pos;
    }

    split_minmax_idxes.resize(num_row_groups);
    for (size_t i = 0; i < num_row_groups; ++i)
    {
        auto row_group_meta = metadata->RowGroup(static_cast<int>(i));
        split_minmax_idxes[i] = std::make_shared<MinMaxIndex>();
        split_minmax_idxes[i]->hyperrectangle.resize(num_cols, Range::createWholeUniverseWithoutNull());

        size_t j = 0;
        auto it = index_names_and_types.begin();
        for (; it != index_names_and_types.end(); ++j, ++it)
        {
            String column{it->name};
            boost::to_lower(column);
            auto mit = parquet_column_positions.find(column);
            if (mit == parquet_column_positions.end())
                continue;

            size_t pos = mit->second;
            auto col_chunk = row_group_meta->ColumnChunk(static_cast<int>(pos));
            if (!col_chunk->is_stats_set())
                continue;

            auto stats = col_chunk->statistics();
            if (stats->HasNullCount() && stats->null_count() > 0)
                continue;

            if (auto bool_stats = std::dynamic_pointer_cast<parquet::BoolStatistics>(stats))
            {
                split_minmax_idxes[i]->hyperrectangle[j] = createRangeFromParquetStatistics<UInt8>(bool_stats);
            }
            else if (auto int32_stats = std::dynamic_pointer_cast<parquet::Int32Statistics>(stats))
            {
                split_minmax_idxes[i]->hyperrectangle[j] = createRangeFromParquetStatistics<Int32>(int32_stats);
            }
            else if (auto int64_stats = std::dynamic_pointer_cast<parquet::Int64Statistics>(stats))
            {
                split_minmax_idxes[i]->hyperrectangle[j] = createRangeFromParquetStatistics<Int64>(int64_stats);
            }
            else if (auto float_stats = std::dynamic_pointer_cast<parquet::FloatStatistics>(stats))
            {
                split_minmax_idxes[i]->hyperrectangle[j] = createRangeFromParquetStatistics<Float64>(float_stats);
            }
            else if (auto double_stats = std::dynamic_pointer_cast<parquet::FloatStatistics>(stats))
            {
                split_minmax_idxes[i]->hyperrectangle[j] = createRangeFromParquetStatistics<Float64>(double_stats);
            }
            else if (auto string_stats = std::dynamic_pointer_cast<parquet::ByteArrayStatistics>(stats))
            {
                split_minmax_idxes[i]->hyperrectangle[j] = createRangeFromParquetStatistics(string_stats);
            }
            /// Other types are not supported for minmax index, skip
        }
        split_minmax_idxes[i]->initialized = true;
    }
}

SourcePtr HiveParquetFile::getReader(const Block & block, const std::shared_ptr<IHiveFile::ReadParams> & params)
{
    if (!params->read_buf)
        params->read_buf = readFile(params->read_settings);

    auto parquet_format = std::make_unique<ParquetBlockInputFormat>(
        *params->read_buf,
        block,
        params->format_settings,
        params->read_settings.parquet_decode_threads,
        params->read_settings.remote_read_min_bytes_for_seek);

    if (params->query_info)
        parquet_format->setQueryInfo(*params->query_info, params->context);
    return parquet_format;
}

}

#endif
