#include "Storages/Hive/HiveFile/HiveORCFile.h"
#if USE_HIVE

#include "Processors/Formats/Impl/ArrowBufferedStreams.h"
#include "Processors/Formats/Impl/LMNativeORCBlockInputFormat.h"
#include "Processors/Formats/Impl/ORCBlockInputFormat.h"
#include "IO/ReadSettings.h"

#include <arrow/adapters/orc/adapter.h>
#include <orc/Statistics.hh>

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
Range createRangeFromOrcStatistics(const StatisticsType * stats)
{
    /// Null values or NaN/Inf values of double type.
    if (stats->hasMinimum() && stats->hasMaximum())
    {
        return Range(FieldType(stats->getMinimum()), true, FieldType(stats->getMaximum()), true);
    }
    else if (stats->hasMinimum())
    {
        return Range::createLeftBounded(FieldType(stats->getMinimum()), true);
    }
    else if (stats->hasMaximum())
    {
        return Range::createRightBounded(FieldType(stats->getMaximum()), true);
    }
    else
    {
        return Range::createWholeUniverseWithoutNull();
    }
}

static Range buildRange(const orc::ColumnStatistics * col_stats)
{
    if (!col_stats || col_stats->hasNull())
        return Range::createWholeUniverseWithoutNull();

    if (const auto * int_stats = dynamic_cast<const orc::IntegerColumnStatistics *>(col_stats))
    {
        return createRangeFromOrcStatistics<Int64>(int_stats);
    }
    else if (const auto * double_stats = dynamic_cast<const orc::DoubleColumnStatistics *>(col_stats))
    {
        return createRangeFromOrcStatistics<Float64>(double_stats);
    }
    else if (const auto * string_stats = dynamic_cast<const orc::StringColumnStatistics *>(col_stats))
    {
        return createRangeFromOrcStatistics<String>(string_stats);
    }
    else if (const auto * bool_stats = dynamic_cast<const orc::BooleanColumnStatistics *>(col_stats))
    {
        auto false_cnt = bool_stats->getFalseCount();
        auto true_cnt = bool_stats->getTrueCount();
        if (false_cnt && true_cnt)
        {
            return Range(UInt8(0), true, UInt8(1), true);
        }
        else if (false_cnt)
        {
            return Range::createLeftBounded(UInt8(0), true);
        }
        else if (true_cnt)
        {
            return Range::createRightBounded(UInt8(1), true);
        }
    }
    else if (const auto * timestamp_stats = dynamic_cast<const orc::TimestampColumnStatistics *>(col_stats))
    {
        return createRangeFromOrcStatistics<UInt32>(timestamp_stats);
    }
    else if (const auto * date_stats = dynamic_cast<const orc::DateColumnStatistics *>(col_stats))
    {
        return createRangeFromOrcStatistics<UInt16>(date_stats);
    }
    return Range::createWholeUniverseWithoutNull();
}

HiveORCFile::HiveORCFile() = default;
HiveORCFile::~HiveORCFile() = default;

size_t HiveORCFile::numSlices() const
{
    openFile();
    return file_reader->NumberOfStripes();
}

std::optional<size_t> HiveORCFile::numRows() const
{
    openFile();

    return file_reader->NumberOfRows();
}

HiveORCFile::MinMaxIndexPtr HiveORCFile::buildMinMaxIndex(const orc::Statistics * statistics, const NamesAndTypesList & index_names_and_types) const
{
    if (!statistics)
        return nullptr;

    size_t range_num = index_names_and_types.size();
    auto idx = std::make_shared<MinMaxIndex>();
    idx->hyperrectangle.resize(range_num, Range::createWholeUniverseWithoutNull());

    size_t i = 0;
    for (const auto & name_type : index_names_and_types)
    {
        String column{name_type.name};
        boost::to_lower(column);
        auto it = orc_column_positions.find(column);
        if (it == orc_column_positions.end())
        {
            idx->hyperrectangle[i] = buildRange(nullptr);
        }
        else
        {
            size_t pos = it->second;
            /// Attention: column statistics start from 1. 0 has special purpose.
            const orc::ColumnStatistics * col_stats = statistics->getColumnStatistics(static_cast<unsigned>(pos + 1));
            idx->hyperrectangle[i] = buildRange(col_stats);
        }
        ++i;
    }
    idx->initialized = true;
    return idx;
}

void HiveORCFile::loadSplitMinMaxIndex(const NamesAndTypesList & index_names_and_types)
{
    openFile();

    auto * raw_reader = file_reader->GetRawORCReader();
    auto stripe_num = raw_reader->getNumberOfStripes();
    auto stripe_stats_num = raw_reader->getNumberOfStripeStatistics();
    if (stripe_num != stripe_stats_num)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "orc file:{} has different strip num {} and strip statistics num {}", file_path, stripe_num, stripe_stats_num);

    split_minmax_idxes.resize(stripe_num);
    for (size_t i = 0; i < stripe_num; ++i)
    {
        auto stripe_stats = raw_reader->getStripeStatistics(i);
        split_minmax_idxes[i] = buildMinMaxIndex(stripe_stats.get(), index_names_and_types);
    }
}

void HiveORCFile::openFile() const
{
    std::lock_guard lock(mutex);
    if (file_reader)
        return;

    auto seekable_buffer = readFile(ReadSettings{});
    std::atomic_int stopped = false;
    THROW_ARROW_NOT_OK(arrow::adapters::orc::ORCFileReader::Open(
        asArrowFile(*seekable_buffer, FormatSettings{}, stopped, "ORC", ORC_MAGIC_BYTES, /* avoid_buffering */ true),
        arrow::default_memory_pool())
    .Value(&file_reader));
    buf = std::move(seekable_buffer);

    THROW_ARROW_NOT_OK(file_reader->ReadSchema().Value(&schema));

    const orc::Type & type = file_reader->GetRawORCReader()->getType();
    size_t count = type.getSubtypeCount();
    for (size_t pos = 0; pos < count; pos++)
    {
        /// Column names in hive is case-insensitive.
        String column{type.getFieldName(pos)};
        boost::to_lower(column);
        orc_column_positions[column] = pos;
    }
}

SourcePtr HiveORCFile::getReader(const Block & block, const std::shared_ptr<IHiveFile::ReadParams> & params)
{
    openFile();
    if (!params->read_buf)
        params->read_buf = readFile(params->read_settings);

    if (params->format_settings.orc.use_fast_decoder == 2)
    {
        LOG_TRACE(log, "Orc use native reader");
        auto orc_format = std::make_shared<LMNativeORCBlockInputFormat>(*params->read_buf, block, params->format_settings);
        if (params->query_info)
            orc_format->setQueryInfo(*params->query_info, params->context);
        return orc_format;
    }
    else
    {
        auto arrow_column_to_ch_column = std::make_unique<ArrowColumnToCHColumn>(
            block,
            "ORC",
            params->format_settings.orc.import_nested,
            params->format_settings.orc.allow_missing_columns,
            params->format_settings.null_as_default,
            params->format_settings.orc.case_insensitive_column_matching);

        std::vector<int> column_indices = ORCBlockInputFormat::getColumnIndices(
            schema, block, params->format_settings.orc.case_insensitive_column_matching, params->format_settings.orc.import_nested);

        std::unique_ptr<arrow::adapters::orc::ORCFileReader> reader;
        std::atomic_int stopped = false;
        THROW_ARROW_NOT_OK(
            arrow::adapters::orc::ORCFileReader::Open(
                asArrowFile(*params->read_buf, params->format_settings, stopped, "ORC", ORC_MAGIC_BYTES, /* avoid_buffering */ true),
                arrow::default_memory_pool())
                .Value(&reader));

        return std::make_shared<ORCSliceSource>(std::move(reader), std::move(column_indices), params, std::move(arrow_column_to_ch_column));
    }
}

ORCSliceSource::ORCSliceSource(
    std::shared_ptr<arrow::adapters::orc::ORCFileReader> reader_,
    std::vector<int> column_indices_,
    std::shared_ptr<IHiveFile::ReadParams> read_params_,
    std::shared_ptr<ArrowColumnToCHColumn> arrow_column_to_ch_column_)
    : ISource({})
    , reader(std::move(reader_))
    , column_indices(std::move(column_indices_))
    , read_params(std::move(read_params_))
    , arrow_column_to_ch_column(arrow_column_to_ch_column_)
{
}

ORCSliceSource::~ORCSliceSource() = default;

Chunk ORCSliceSource::generate()
{
    Chunk res;
    if (!read_params->slice)
        return res;

    size_t slice_to_read = read_params->slice.value();
    read_params->slice.reset();

    std::shared_ptr<arrow::Table> table;
    auto batch_status = reader->ReadStripe(slice_to_read, column_indices);
    if (!batch_status.ok())
        throw ParsingException(ErrorCodes::CANNOT_READ_ALL_DATA,
                               "Error while reading batch of ORC data: {}", batch_status.status().ToString());

    THROW_ARROW_NOT_OK(arrow::Table::FromRecordBatches({batch_status.ValueOrDie()}).Value(&table));
    arrow_column_to_ch_column->arrowTableToCHChunk(res, table, table->num_rows());
    return res;
}


};

#endif
