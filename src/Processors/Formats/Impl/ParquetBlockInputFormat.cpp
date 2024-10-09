/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include "ParquetBlockInputFormat.h"
#include <parquet/properties.h>
#if USE_PARQUET

#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/copyData.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/status.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/file_reader.h>
#include <parquet/statistics.h>
#include "Common/ProfileEvents.h"
#include "Common/Stopwatch.h"
#include <Common/setThreadName.h>
#include <common/logger_useful.h>
#include "ArrowBufferedStreams.h"
#include "ArrowColumnToCHColumn.h"
#include "ArrowFieldIndexUtil.h"
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/FieldVisitorsAccurateComparison.h>
#include <Common/LRUCache.h>
#include "Common/threadPoolCallbackRunner.h"
#include <Processors/Formats/Impl/ArrowColumnCache.h>
#include <Processors/Formats/Impl/Parquet/ParquetRecordReader.h>

namespace CurrentMetrics
{
    extern const Metric ParquetDecoderThreads;
    extern const Metric ParquetDecoderThreadsActive;
}

namespace ProfileEvents
{
    extern const int ParquetFileOpened;
    extern const int ParquetReadRowGroups;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int CORRUPTED_DATA;
}

#define THROW_ARROW_NOT_OK(status)                                     \
    do                                                                 \
    {                                                                  \
        if (::arrow::Status _s = (status); !_s.ok())                   \
            throw Exception(_s.ToString(), ErrorCodes::BAD_ARGUMENTS); \
    } while (false)

/// Decode min/max value from column chunk statistics.
///
/// There are two questionable decisions in this implementation:
///  * We parse the value from the encoded byte string instead of casting the parquet::Statistics
///    to parquet::TypedStatistics and taking the value from there.
///  * We dispatch based on the parquet logical+converted+physical type instead of the ClickHouse type.
/// The idea is that this is similar to what we'll have to do when reimplementing Parquet parsing in
/// ClickHouse instead of using Arrow (for speed). So, this is an exercise in parsing Parquet manually.
static std::optional<Field> decodePlainParquetValueSlow(const std::string & data, parquet::Type::type physical_type, const parquet::ColumnDescriptor & descr)
{
    using namespace parquet;

    auto decode_integer = [&](bool signed_) -> UInt64 {
        size_t size;
        switch (physical_type)
        {
            case parquet::Type::type::BOOLEAN: size = 1; break;
            case parquet::Type::type::INT32: size = 4; break;
            case parquet::Type::type::INT64: size = 8; break;
            default: throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Unexpected physical type for number");
        }
        if (data.size() != size)
            throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Unexpected size: {}", data.size());

        UInt64 val = 0;
        memcpy(&val, data.data(), size);

        /// Sign-extend.
        if (signed_ && size < 8 && (val >> (size * 8 - 1)) != 0)
            val |= 0 - (1ul << (size * 8));

        return val;
    };

    /// Decimal.
    do // while (false)
    {
        Int32 scale;
        if (descr.logical_type() && descr.logical_type()->is_decimal())
            scale = assert_cast<const DecimalLogicalType &>(*descr.logical_type()).scale();
        else if (descr.converted_type() == ConvertedType::type::DECIMAL)
            scale = descr.type_scale();
        else
            break;

        size_t size;
        bool big_endian = false;
        switch (physical_type)
        {
            case Type::type::BOOLEAN: size = 1; break;
            case Type::type::INT32: size = 4; break;
            case Type::type::INT64: size = 8; break;

            case Type::type::FIXED_LEN_BYTE_ARRAY:
                big_endian = true;
                size = data.size();
                break;
            default: throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Unexpected decimal physical type");
        }
        /// Note that size is not necessarily a power of two.
        /// E.g. spark turns 8-byte unsigned integers into 9-byte signed decimals.
        if (data.size() != size || size < 1 || size > 32)
            throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Unexpected decimal size: {} (actual {})", size, data.size());

        /// For simplicity, widen all decimals to 256-bit. It should compare correctly with values
        /// of different bitness.
        Int256 val = 0;
        memcpy(&val, data.data(), size);
        if (big_endian)
            std::reverse(reinterpret_cast<char *>(&val), reinterpret_cast<char *>(&val) + size);
        /// Sign-extend.
        if (size < 32 && (val >> (size * 8 - 1)) != 0)
            val |= ~((Int256(1) << (size * 8)) - 1);

        return Field(DecimalField<Decimal256>(Decimal256(val), static_cast<UInt32>(scale)));
    }
    while (false);

    /// Timestamp (decimal).
    {
        Int32 scale = -1;
        bool is_timestamp = true;
        if (descr.logical_type() && (descr.logical_type()->is_time() || descr.logical_type()->is_timestamp()))
        {
            LogicalType::TimeUnit::unit unit = descr.logical_type()->is_time()
                ? assert_cast<const TimeLogicalType &>(*descr.logical_type()).time_unit()
                : assert_cast<const TimestampLogicalType &>(*descr.logical_type()).time_unit();
            switch (unit)
            {
                case LogicalType::TimeUnit::unit::MILLIS: scale = 3; break;
                case LogicalType::TimeUnit::unit::MICROS: scale = 6; break;
                case LogicalType::TimeUnit::unit::NANOS: scale = 9; break;
                default: throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Unknown time unit");
            }
        }
        else switch (descr.converted_type())
        {
            case ConvertedType::type::TIME_MILLIS: scale = 3; break;
            case ConvertedType::type::TIME_MICROS: scale = 6; break;
            case ConvertedType::type::TIMESTAMP_MILLIS: scale = 3; break;
            case ConvertedType::type::TIMESTAMP_MICROS: scale = 6; break;
            default: is_timestamp = false;
        }

        if (is_timestamp)
        {
            Int64 val = static_cast<Int64>(decode_integer(/* signed */ true));
            return Field(DecimalField<Decimal64>(Decimal64(val), scale));
        }
    }

    /// Floats.

    if (physical_type == Type::type::FLOAT)
    {
        if (data.size() != 4)
            throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Unexpected float size");
        Float32 val;
        memcpy(&val, data.data(), data.size());
        return Field(val);
    }

    if (physical_type == Type::type::DOUBLE)
    {
        if (data.size() != 8)
            throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Unexpected float size");
        Float64 val;
        memcpy(&val, data.data(), data.size());
        return Field(val);
    }

    /// Strings.

    if (physical_type == Type::type::BYTE_ARRAY || physical_type == Type::type::FIXED_LEN_BYTE_ARRAY)
    {
        /// Arrow's parquet decoder handles missing min/max values slightly incorrectly.
        /// In a parquet file, min and max have separate is_set flags, i.e. one may be missing even
        /// if the other is set. Arrow decoder ORs (!) these two flags together into one: HasMinMax().
        /// So, if exactly one of {min, max} is missing, Arrow reports it as empty string, with no
        /// indication that it's actually missing.
        ///
        /// How can exactly one of {min, max} be missing? This happens if one of the two strings
        /// exceeds the length limit for stats. Repro:
        ///
        ///   insert into function file('t.parquet') select arrayStringConcat(range(number*1000000)) from numbers(2) settings output_format_parquet_use_custom_encoder=0
        ///   select tupleElement(tupleElement(row_groups[1], 'columns')[1], 'statistics') from file('t.parquet', ParquetMetadata)
        ///
        /// Here the row group contains two strings: one empty, one very long. But the statistics
        /// reported by arrow are indistinguishable from statistics if all strings were empty.
        /// (Min and max are the last two tuple elements in the output of the second query. Notice
        /// how they're empty strings instead of NULLs.)
        ///
        /// So we have to be conservative and treat empty string as unknown.
        /// This is unfortunate because it's probably common for string columns to have lots of empty
        /// values, and filter pushdown would probably often be useful in that case.
        ///
        /// TODO: Remove this workaround either when we implement our own Parquet decoder that
        ///       doesn't have this bug, or if it's fixed in Arrow.
        if (data.empty())
            return std::nullopt;

        return Field(data);
    }

    /// This one's deprecated in Parquet.
    if (physical_type == Type::type::INT96)
        throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Parquet INT96 type is deprecated and not supported");

    /// Integers.

    bool signed_ = true;
    if (descr.logical_type() && descr.logical_type()->is_int())
        signed_ = assert_cast<const IntLogicalType &>(*descr.logical_type()).is_signed();
    else
        signed_ = descr.converted_type() != ConvertedType::type::UINT_8 &&
                  descr.converted_type() != ConvertedType::type::UINT_16 &&
                  descr.converted_type() != ConvertedType::type::UINT_32 &&
                  descr.converted_type() != ConvertedType::type::UINT_64;

    UInt64 val = decode_integer(signed_);
    Field field = signed_ ? Field(static_cast<Int64>(val)) : Field(val);
    return field;
}

/// Range of values for each column, based on statistics in the Parquet metadata.
/// This is lower/upper bounds, not necessarily exact min and max, e.g. the min/max can be just
/// missing in the metadata.
static std::vector<Range> getHyperrectangleForRowGroup(const parquet::FileMetaData & file, int row_group_idx, const Block & header, const FormatSettings & format_settings)
{
    auto column_name_for_lookup = [&](std::string column_name) -> std::string
    {
        if (format_settings.parquet.case_insensitive_column_matching)
            boost::to_lower(column_name);
        return column_name;
    };

    std::unique_ptr<parquet::RowGroupMetaData> row_group = file.RowGroup(row_group_idx);

    std::unordered_map<std::string, std::shared_ptr<parquet::Statistics>> name_to_statistics;
    for (int i = 0; i < row_group->num_columns(); ++i)
    {
        auto c = row_group->ColumnChunk(i);
        auto s = c->statistics();
        if (!s)
            continue;

        auto path = c->path_in_schema()->ToDotVector();
        if (path.size() != 1)
            continue; // compound types not supported

        name_to_statistics.emplace(column_name_for_lookup(path[0]), s);
    }

    ///    +-----+
    ///   /     /|
    ///  +-----+ |
    ///  |     | +
    ///  |     |/
    ///  +-----+
    std::vector<Range> hyperrectangle(header.columns(), Range::createWholeUniverse());

    for (size_t idx = 0; idx < header.columns(); ++idx)
    {
        const std::string & name = header.getByPosition(idx).name;
        auto it = name_to_statistics.find(column_name_for_lookup(name));
        if (it == name_to_statistics.end())
            continue;
        auto stats = it->second;

        auto default_value = [&]() -> Field
        {
            DataTypePtr type = header.getByPosition(idx).type;
            if (type->lowCardinality())
                type = assert_cast<const DataTypeLowCardinality &>(*type).getDictionaryType();
            if (type->isNullable())
                type = assert_cast<const DataTypeNullable &>(*type).getNestedType();
            return type->getDefault();
        };

        /// Only primitive fields are supported, not arrays, maps, tuples, or Nested.
        /// Arrays, maps, and Nested can't be meaningfully supported because Parquet only has min/max
        /// across all *elements* of the array, not min/max array itself.
        /// Same limitation for tuples, but maybe it would make sense to have some kind of tuple
        /// expansion in KeyCondition to accept ranges per element instead of whole tuple.

        std::optional<Field> min;
        std::optional<Field> max;
        if (stats->HasMinMax())
        {
            try
            {
                min = decodePlainParquetValueSlow(stats->EncodeMin(), stats->physical_type(), *stats->descr());
                max = decodePlainParquetValueSlow(stats->EncodeMax(), stats->physical_type(), *stats->descr());
            }
            catch (Exception & e)
            {
                e.addMessage(" (When parsing Parquet statistics for column {}, physical type {}, {}. Please report an issue and use input_format_parquet_filter_push_down = false to work around.)", name, static_cast<int>(stats->physical_type()), stats->descr()->ToString());
                throw;
            }
        }

        /// In Range, NULL is represented as positive or negative infinity (represented by a special
        /// kind of Field, different from floating-point infinities).

        bool always_null = stats->descr()->max_definition_level() != 0 &&
            stats->HasNullCount() && stats->num_values() == 0;
        bool can_be_null = stats->descr()->max_definition_level() != 0 &&
            (!stats->HasNullCount() || stats->null_count() != 0);
        bool null_as_default = format_settings.null_as_default && !isNullableOrLowCardinalityNullable(header.getByPosition(idx).type);

        if (always_null)
        {
            /// Single-point range containing either the default value of one of the infinities.
            if (null_as_default)
                hyperrectangle[idx].right = hyperrectangle[idx].left = default_value();
            else
                hyperrectangle[idx].right = hyperrectangle[idx].left;
            continue;
        }

        if (can_be_null)
        {
            if (null_as_default)
            {
                /// Make sure the range contains the default value.
                Field def = default_value();
                if (min.has_value() && applyVisitor(FieldVisitorAccurateLess(), def, *min))
                    min = def;
                if (max.has_value() && applyVisitor(FieldVisitorAccurateLess(), *max, def))
                    max = def;
            }
            else
            {
                /// Make sure the range reaches infinity on at least one side.
                if (min.has_value() && max.has_value())
                    min.reset();
            }
        }
        else
        {
            /// If the column doesn't have nulls, exclude both infinities.
            if (!min.has_value())
                hyperrectangle[idx].left_included = false;
            if (!max.has_value())
                hyperrectangle[idx].right_included = false;
        }

        if (min.has_value())
            hyperrectangle[idx].left = std::move(min.value());
        if (max.has_value())
            hyperrectangle[idx].right = std::move(max.value());
    }

    return hyperrectangle;
}

ParquetBlockInputFormat::ParquetBlockInputFormat(
    ReadBuffer & buf_,
    const Block & header_,
    const FormatSettings & format_settings_,
    const ReadSettings & read_settings_,
    bool is_remote_fs_,
    size_t max_download_threads_,
    size_t max_parsing_threads_,
    SharedParsingThreadPoolPtr parsing_thread_pool)
    : ParallelDecodingBlockInputFormat(
        buf_,
        header_,
        format_settings_,
        max_download_threads_,
        max_parsing_threads_,
        format_settings_.parquet.preserve_order,
        format_settings_.parquet.skip_row_groups,
        std::move(parsing_thread_pool))
    , read_settings(read_settings_)
    , is_remote_fs(is_remote_fs_)
{
    if (format_settings.parquet.use_native_reader)
        LOG_TRACE(log, "Parquet use native reader");
    else
        LOG_TRACE(log, "Parquet use arrow reader");
}

ParquetBlockInputFormat::~ParquetBlockInputFormat()
{
    close();
}

void ParquetBlockInputFormat::setQueryInfo(const SelectQueryInfo & query_info, ContextPtr query_context)
{
    if (format_settings.parquet.filter_push_down)
    {
        key_condition.emplace(query_info, query_context, getPort().getHeader().getNames(),
            std::make_shared<ExpressionActions>(std::make_shared<ActionsDAG>(
                getPort().getHeader().getColumnsWithTypeAndName())));

        prewhere_info = query_info.prewhere_info;
    }
}

void ParquetBlockInputFormat::initializeFileReader()
{
    if (is_stopped)
        return;

    /// TODO: prefetch scheduler
    ThreadPoolCallbackRunnerUnsafe<void> scheduler = nullptr;
    arrow_file = asArrowFile(in, format_settings, is_stopped, "Parquet", PARQUET_MAGIC_BYTES, /* avoid_buffering */ true, scheduler);

    if (is_stopped)
        return;

    ProfileEvents::increment(ProfileEvents::ParquetFileOpened);

    if (format_settings.parquet.use_footer_cache)
    {
        std::optional<String> file_name;
        /// TODO: better to implement WithFileName
        if (auto * file_base = dynamic_cast<ReadBufferFromFileBase *>(&in); file_base)
        {
            file_name = file_base->getFileName();
        }

        if (auto & footer_cache = ArrowFooterCache::instance(); footer_cache && file_name)
        {
            metadata = footer_cache->getOrSet(ArrowFooterCache::calculateKeyHash(*file_name), [&] {
                auto cell = std::make_shared<ArrowFooterCacheCell>();
                cell->parquet_file_metadata = parquet::ReadMetaData(arrow_file);
                cell->weight = cell->parquet_file_metadata->size();
                return cell;
            })->parquet_file_metadata;
        }
    }

    if (!metadata)
        metadata = parquet::ReadMetaData(arrow_file);

    THROW_ARROW_NOT_OK(parquet::arrow::FromParquetSchema(metadata->schema(), &schema));

    field_util = std::make_shared<ArrowFieldIndexUtil>(
        format_settings.parquet.case_insensitive_column_matching,
        format_settings.parquet.allow_missing_columns,
        *schema);
    column_indices = field_util->findRequiredIndices(getPort().getHeader());

    int num_row_groups = metadata->num_row_groups();
    row_group_readers.resize(num_row_groups);

    for (int row_group = 0; row_group < num_row_groups; ++row_group)
    {
        if (skip_row_groups.contains(row_group))
            continue;

        if (key_condition.has_value()
            && !key_condition
                    ->checkInHyperrectangle(
                        getHyperrectangleForRowGroup(*metadata, row_group, getPort().getHeader(), format_settings),
                        getPort().getHeader().getDataTypes())
                    .can_be_true)
        {
            skip_row_groups.emplace(row_group);
        }
    }

    LOG_TRACE(log, "skip {}/{} row groups", skip_row_groups.size(), num_row_groups);
}

void ParquetBlockInputFormat::initializeRowGroupReaderIfNeeded(size_t row_group_idx)
{
    auto & row_group_reader = row_group_readers[row_group_idx];

    if (std::exchange(row_group_reader.initialized, true))
        return;

    ProfileEvents::increment(ProfileEvents::ParquetReadRowGroups);
    row_group_reader.read_column_indices = column_indices;
    parquet::ArrowReaderProperties arrow_properties;
    parquet::ReaderProperties reader_properties(ArrowMemoryPool::instance());
    arrow_properties.set_use_threads(format_settings.parquet.use_threads);
    arrow_properties.set_batch_size(format_settings.parquet.max_block_size);

    if (format_settings.parquet.coalesce_read)
    {
        /// enable io coalesce read
        arrow_properties.set_pre_buffer(true);
        auto cache_options = arrow::io::CacheOptions{
            .hole_size_limit = static_cast<int64_t>(format_settings.parquet.min_bytes_for_seek),
            .range_size_limit = static_cast<int64_t>(format_settings.parquet.max_buffer_size),
            .lazy = !(is_remote_fs ? read_settings.remote_fs_prefetch : read_settings.local_fs_prefetch),
        };

        arrow_properties.set_cache_options(cache_options);
    }

    // Workaround for a workaround in the parquet library.
    //
    // From ComputeColumnChunkRange() in contrib/arrow/cpp/src/parquet/file_reader.cc:
    //  > The Parquet MR writer had a bug in 1.2.8 and below where it didn't include the
    //  > dictionary page header size in total_compressed_size and total_uncompressed_size
    //  > (see IMPALA-694). We add padding to compensate.
    //
    // That padding breaks the pre-buffered mode because the padded read ranges may overlap each
    // other, failing an assert. So we disable pre-buffering in this case.
    // That version is >10 years old, so this is not very important.
    if (metadata->writer_version().VersionLt(parquet::ApplicationVersion::PARQUET_816_FIXED_VERSION()))
        arrow_properties.set_pre_buffer(false);

    if (format_settings.parquet.use_native_reader)
    {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunreachable-code"
        if constexpr (std::endian::native != std::endian::little)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "parquet native reader only supports little endian system currently");
#pragma clang diagnostic pop

        std::unique_ptr<parquet::ParquetFileReader> raw_file_reader
            = parquet::ParquetFileReader::Open(arrow_file, reader_properties, metadata);

        row_group_reader.native_record_reader = std::make_shared<ParquetRecordReader>(
            getPort().getHeader(),
            arrow_file,
            std::move(raw_file_reader),
            arrow_properties,
            *field_util,
            format_settings,
            std::vector<int>{static_cast<int>(row_group_idx)},
            prewhere_info);
    }
    else
    {
        parquet::arrow::FileReaderBuilder builder;
        THROW_ARROW_NOT_OK(
            builder.Open(arrow_file, /* not to be confused with ArrowReaderProperties */ reader_properties, metadata));
        builder.properties(arrow_properties);
        builder.memory_pool(ArrowMemoryPool::instance());
        THROW_ARROW_NOT_OK(builder.Build(&row_group_reader.file_reader));

        THROW_ARROW_NOT_OK(
            row_group_reader.file_reader->GetRecordBatchReader({static_cast<int>(row_group_idx)}, row_group_reader.read_column_indices, &row_group_reader.record_batch_reader));

        row_group_reader.arrow_column_to_ch_column = std::make_unique<ArrowColumnToCHColumn>(
            getPort().getHeader(),
            "Parquet",
            format_settings.parquet.import_nested,
            format_settings.parquet.allow_missing_columns,
            format_settings.null_as_default,
            format_settings.date_time_overflow_behavior,
            format_settings.parquet.case_insensitive_column_matching);

        // if (auto context = getContext())
        // {
        //     row_group_reader.arrow_column_to_ch_column->setContext(context);
        //     row_group_reader.arrow_column_to_ch_column->setBitEngineDictionaryTableIfNeeded();
        // }
    }

    row_group_reader.row_group_bytes_uncompressed = metadata->RowGroup(static_cast<int>(row_group_idx))->total_compressed_size();
    row_group_reader.row_group_rows = metadata->RowGroup(static_cast<int>(row_group_idx))->num_rows();
}

std::optional<ParallelDecodingBlockInputFormat::PendingChunk> ParquetBlockInputFormat::readBatch(size_t row_group_idx)
{
    auto & row_group_reader = row_group_readers[row_group_idx];
    auto & row_group_status = row_groups[row_group_idx];

    PendingChunk res = {.chunk_idx = row_group_status.next_chunk_idx, .row_group_idx = row_group_idx};
    /// If defaults_for_omitted_fields is true, calculate the default values from default expression for omitted fields.
    /// Otherwise fill the missing columns with zero values of its type.
    BlockMissingValues * block_missing_values_ptr = format_settings.defaults_for_omitted_fields ? &res.block_missing_values : nullptr;

    if (format_settings.parquet.use_native_reader)
    {
        res.chunk = row_group_reader.native_record_reader->readChunk(block_missing_values_ptr);
        /// eof
        if (!res.chunk)
            return {};

        res.approx_original_chunk_size = static_cast<size_t>(std::ceil(static_cast<double>(row_group_reader.row_group_bytes_uncompressed) / row_group_reader.row_group_rows * res.chunk.getNumRows()));
    }
    else
    {
        auto batch = row_group_reader.record_batch_reader->Next();
        if (!batch.ok())
            throw ParsingException(ErrorCodes::CANNOT_READ_ALL_DATA, "Error while reading Parquet data: {}", batch.status().ToString());

        /// eof
        if (!*batch)
            return {};

        auto tmp_table = arrow::Table::FromRecordBatches({*batch});
        res.approx_original_chunk_size = static_cast<size_t>(std::ceil(static_cast<double>(row_group_reader.row_group_bytes_uncompressed) / row_group_reader.row_group_rows * (*tmp_table)->num_rows()));
        row_group_reader.arrow_column_to_ch_column->arrowTableToCHChunk(res.chunk, *tmp_table, (*tmp_table)->num_rows());
    }

    return res;
}

size_t ParquetBlockInputFormat::getRowCount()
{
    initializeFileReaderIfNeeded();
    return metadata->num_rows();
}

void ParquetBlockInputFormat::prefetchRowGroup(size_t row_group_idx)
{
    /// prebuffer will trigger async prefetch
    /// see arrow/io/caching.cc
    initializeRowGroupReaderIfNeeded(row_group_idx);
}

size_t ParquetBlockInputFormat::getNumberOfRowGroups()
{
    assert(metadata);
    return metadata->num_row_groups();
}

void ParquetBlockInputFormat::resetParser()
{
    ParallelDecodingBlockInputFormat::resetParser();

    arrow_file.reset();
    metadata.reset();
    column_indices.clear();
    row_group_readers.clear();
}

IStorage::ColumnSizeByName ParquetBlockInputFormat::getColumnSizes()
{
    initializeFileReader();

    const auto & header = getPort().getHeader();
    IStorage::ColumnSizeByName column_size_by_name;

    for (const auto & col : header)
    {
        ColumnSize column_size;
        // auto & column_size = column_size_by_name.emplace(col.name).first->second;
        std::vector<int> required_indices = field_util->findRequiredIndices(col.name);
        size_t num_row_groups = metadata->num_row_groups();
        for (size_t row_group_idx = 0; row_group_idx < num_row_groups; row_group_idx++)
        {
            for (int col_idx : required_indices)
            {
                auto chunk = metadata->RowGroup(row_group_idx)->ColumnChunk(col_idx);
                column_size.data_uncompressed += chunk->total_uncompressed_size();
                column_size.data_compressed += chunk->total_compressed_size();
            }
        }
        column_size_by_name[col.name] = std::move(column_size);
    }
    return column_size_by_name;
}

ParquetSchemaReader::ParquetSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : ISchemaReader(in_), format_settings(format_settings_)
{
}

NamesAndTypesList ParquetSchemaReader::readSchema()
{
    LOG_TRACE(getLogger("ParquetSchemaReader"), "start readSchema");
    std::atomic<int> is_stopped{0};
    auto file = asArrowFile(in, format_settings, is_stopped, "Parquet", PARQUET_MAGIC_BYTES);

    auto metadata = parquet::ReadMetaData(file);

    std::shared_ptr<arrow::Schema> schema;
    THROW_ARROW_NOT_OK(parquet::arrow::FromParquetSchema(metadata->schema(), &schema));

    auto header = ArrowColumnToCHColumn::arrowSchemaToCHHeader(
        *schema, "Parquet", format_settings.parquet.skip_columns_with_unsupported_types_in_schema_inference);
    // if (format_settings.schema_inference_make_columns_nullable)
    //     return getNamesAndRecursivelyNullableTypes(header);
    return header.getNamesAndTypesList();
}

void registerInputFormatProcessorParquet(FormatFactory & factory)
{
    factory.registerRandomAccessInputFormat(
        "Parquet",
        [](ReadBuffer & buf,
           const Block & sample,
           const FormatSettings & settings,
           const ReadSettings & read_settings,
           bool is_remote_fs,
           size_t max_download_threads,
           size_t max_parsing_threads,
           SharedParsingThreadPoolPtr parsing_thread_pool) {
            return std::make_shared<ParquetBlockInputFormat>(buf, sample, settings, read_settings,
                is_remote_fs, max_download_threads, max_parsing_threads, parsing_thread_pool);
        });

    factory.markFormatAsColumnOriented("Parquet");
}

void registerParquetSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "Parquet",
        [](ReadBuffer & buf, const FormatSettings & settings)
        {
            return std::make_shared<ParquetSchemaReader>(buf, settings);
        }
        );

    factory.registerAdditionalInfoForSchemaCacheGetter("Parquet", [](const FormatSettings & settings)
    {
        return fmt::format("schema_inference_make_columns_nullable={}", settings.schema_inference_make_columns_nullable);
    });
}

}

#else

namespace DB
{
class FormatFactory;
void registerInputFormatProcessorParquet(FormatFactory &)
{
}
void registerParquetSchemaReader(FormatFactory &) {}
}

#endif
