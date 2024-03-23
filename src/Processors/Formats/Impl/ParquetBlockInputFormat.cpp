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
#if USE_PARQUET

#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/copyData.h>
#include "Common/FieldVisitorsAccurateComparison.h"
#include "Common/setThreadName.h"
#include <common/logger_useful.h>
#include "ArrowBufferedStreams.h"
#include "ArrowColumnToCHColumn.h"
#include "ArrowFieldIndexUtil.h"
#include "DataTypes/DataTypeNullable.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/status.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/file_reader.h>
#include <parquet/statistics.h>


namespace CurrentMetrics
{
    extern const Metric ParquetDecoderThreads;
    extern const Metric ParquetDecoderThreadsActive;
    extern const int CANNOT_PARSE_NUMBER;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_READ_ALL_DATA;
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
    ReadBuffer & buf,
    const Block & header_,
    const FormatSettings & format_settings_,
    size_t max_decoding_threads_,
    size_t min_bytes_for_seek_)
    : IInputFormat(header_, buf)
    , format_settings(format_settings_)
    , skip_row_groups(format_settings.parquet.skip_row_groups)
    , max_decoding_threads(max_decoding_threads_)
    , min_bytes_for_seek(min_bytes_for_seek_)
    , pending_chunks(PendingChunk::Compare { .row_group_first = format_settings_.parquet.preserve_order })
{
    if (max_decoding_threads > 1)
        pool = std::make_unique<ThreadPool>(max_decoding_threads);
}

ParquetBlockInputFormat::~ParquetBlockInputFormat()
{
    is_stopped = true;
    if (pool)
        pool->wait();
}

void ParquetBlockInputFormat::setQueryInfo(const SelectQueryInfo & query_info, ContextPtr context)
{
    if (format_settings.parquet.filter_push_down)
        key_condition.emplace(query_info, context, getPort().getHeader().getNames(),
            std::make_shared<ExpressionActions>(std::make_shared<ActionsDAG>(
                getPort().getHeader().getColumnsWithTypeAndName())));
}

void ParquetBlockInputFormat::initializeIfNeeded()
{
    if (std::exchange(is_initialized, true))
        return;

    // Create arrow file adapter.
    // TODO: Make the adapter do prefetching on IO threads, based on the full set of ranges that
    //       we'll need to read (which we know in advance). Use max_download_threads for that.
    arrow_file = asArrowFile(in, format_settings, is_stopped, "Parquet", PARQUET_MAGIC_BYTES, /* avoid_buffering */ true);

    if (is_stopped)
        return;

    metadata = parquet::ReadMetaData(arrow_file);

    std::shared_ptr<arrow::Schema> schema;
    THROW_ARROW_NOT_OK(parquet::arrow::FromParquetSchema(metadata->schema(), &schema));

    ArrowFieldIndexUtil field_util(
        format_settings.parquet.case_insensitive_column_matching,
        format_settings.parquet.allow_missing_columns);
    column_indices = field_util.findRequiredIndices(getPort().getHeader(), *schema);

    int num_row_groups = metadata->num_row_groups();
    row_groups.resize(num_row_groups);
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
    LOG_DEBUG(log, "skip {}/{} row groups", skip_row_groups.size(), num_row_groups);
}

std::vector<int> ParquetBlockInputFormat::getColumnIndices(
    const std::shared_ptr<arrow::Schema> & schema, const Block & header, const FormatSettings & format_settings)
{
    ArrowFieldIndexUtil field_util(format_settings.parquet.case_insensitive_column_matching, format_settings.parquet.allow_missing_columns);
    return field_util.findRequiredIndices(header, *schema);
}

void ParquetBlockInputFormat::initializeRowGroupReader(size_t row_group_idx)
{
    auto & row_group = row_groups[row_group_idx];

    parquet::ArrowReaderProperties properties;
    properties.set_use_threads(false);
    if (format_settings.parquet.max_block_size > 0)
        properties.set_batch_size(format_settings.parquet.max_block_size);
    else
    {
        const auto batch_size = metadata->RowGroup(row_group_idx)->num_rows();
        properties.set_batch_size(batch_size);
    }

    // When reading a row group, arrow will:
    //  1. Look at `metadata` to get all byte ranges it'll need to read from the file (typically one
    //     per requested column in the row group).
    //  2. Coalesce ranges that are close together, trading off seeks vs read amplification.
    //     This is controlled by CacheOptions.
    //  3. Process the columns one by one, issuing the corresponding (coalesced) range reads as
    //     needed. Each range gets its own memory buffer allocated. These buffers stay in memory
    //     (in arrow::io::internal::ReadRangeCache) until the whole row group reading is done.
    //     So the memory usage of a "SELECT *" will be at least the compressed size of a row group
    //     (typically hundreds of MB).
    //
    // With this coalescing, we don't need any readahead on our side, hence avoid_buffering in
    // asArrowFile().
    //
    // This adds one unnecessary copy. We should probably do coalescing and prefetch scheduling on
    // our side instead.
    properties.set_pre_buffer(true);
    auto cache_options = arrow::io::CacheOptions::LazyDefaults();
    cache_options.hole_size_limit = min_bytes_for_seek;
    cache_options.range_size_limit = 1l << 40; // reading the whole row group at once is fine
    properties.set_cache_options(cache_options);

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
        properties.set_pre_buffer(false);

    parquet::arrow::FileReaderBuilder builder;
    THROW_ARROW_NOT_OK(
        builder.Open(arrow_file, /* not to be confused with ArrowReaderProperties */ parquet::default_reader_properties(), metadata));
    builder.properties(properties);
    // TODO: Pass custom memory_pool() to enable memory accounting with non-jemalloc allocators.
    THROW_ARROW_NOT_OK(builder.Build(&row_group.file_reader));

    THROW_ARROW_NOT_OK(
        row_group.file_reader->GetRecordBatchReader({static_cast<int>(row_group_idx)}, column_indices, &row_group.record_batch_reader));
    std::shared_ptr<arrow::Schema> schema;
    THROW_ARROW_NOT_OK(row_group.file_reader->GetSchema(&schema));

    row_group.arrow_column_to_ch_column = std::make_unique<ArrowColumnToCHColumn>(
        getPort().getHeader(),
        "Parquet",
        format_settings.parquet.import_nested,
        format_settings.parquet.allow_missing_columns,
        format_settings.null_as_default,
        format_settings.parquet.case_insensitive_column_matching);

    row_group.row_group_bytes_uncompressed = metadata->RowGroup(static_cast<int>(row_group_idx))->total_compressed_size();
    row_group.row_group_rows = metadata->RowGroup(static_cast<int>(row_group_idx))->num_rows();
}

void ParquetBlockInputFormat::scheduleRowGroup(size_t row_group_idx)
{
    chassert(!mutex.try_lock());

    auto & status = row_groups[row_group_idx].status;
    chassert(status == RowGroupState::Status::NotStarted || status == RowGroupState::Status::Paused);

    status = RowGroupState::Status::Running;

    pool->scheduleOrThrowOnError(
        [this, row_group_idx, thread_group = CurrentThread::getGroup()]()
        {
            if (thread_group)
                CurrentThread::attachToIfDetached(thread_group);
            SCOPE_EXIT({if (thread_group) CurrentThread::detachQueryIfNotDetached();});

            try
            {
                setThreadName("ParquetDecoder");

                threadFunction(row_group_idx);
            }
            catch (...)
            {
                std::lock_guard lock(mutex);
                background_exception = std::current_exception();
                condvar.notify_all();
            }
        });
}

void ParquetBlockInputFormat::threadFunction(size_t row_group_idx)
{
    std::unique_lock lock(mutex);

    auto & row_group = row_groups[row_group_idx];
    chassert(row_group.status == RowGroupState::Status::Running);

    while (true)
    {
        if (is_stopped || row_group.num_pending_chunks >= max_pending_chunks_per_row_group)
        {
            row_group.status = RowGroupState::Status::Paused;
            return;
        }

        decodeOneChunk(row_group_idx, lock);

        if (row_group.status == RowGroupState::Status::Done)
            return;
    }
}

void ParquetBlockInputFormat::decodeOneChunk(size_t row_group_idx, std::unique_lock<std::mutex> & lock)
{
    auto & row_group = row_groups[row_group_idx];
    chassert(row_group.status != RowGroupState::Status::Done);
    chassert(lock.owns_lock());
    SCOPE_EXIT({ chassert(lock.owns_lock() || std::uncaught_exceptions()); });

    lock.unlock();

    auto end_of_row_group = [&] {
        row_group.arrow_column_to_ch_column.reset();
        row_group.record_batch_reader.reset();
        row_group.file_reader.reset();

        lock.lock();
        row_group.status = RowGroupState::Status::Done;

        // We may be able to schedule more work now, but can't call scheduleMoreWorkIfNeeded() right
        // here because we're running on the same thread pool, so it'll deadlock if thread limit is
        // reached. Wake up generate() instead.
        condvar.notify_all();
    };

    if (!row_group.record_batch_reader)
    {
        if (skip_row_groups.contains(static_cast<int>(row_group_idx)))
        {
            // Pretend that the row group is empty.
            // (We could avoid scheduling the row group on a thread in the first place. But the
            // skip_row_groups feature is mostly unused, so it's better to be a little inefficient
            // than to add a bunch of extra mostly-dead code for this.)
            end_of_row_group();
            return;
        }

        initializeRowGroupReader(row_group_idx);
    }


    auto batch = row_group.record_batch_reader->Next();
    if (!batch.ok())
        throw ParsingException(ErrorCodes::CANNOT_READ_ALL_DATA, "Error while reading Parquet data: {}", batch.status().ToString());

    if (!*batch)
    {
        end_of_row_group();
        return;
    }

    auto tmp_table = arrow::Table::FromRecordBatches({*batch});

    size_t approx_chunk_original_size = static_cast<size_t>(std::ceil(static_cast<double>(row_group.row_group_bytes_uncompressed) / row_group.row_group_rows * (*tmp_table)->num_rows()));
    PendingChunk res = {.chunk_idx = row_group.next_chunk_idx, .row_group_idx = row_group_idx, .approx_original_chunk_size = approx_chunk_original_size};

    /// If defaults_for_omitted_fields is true, calculate the default values from default expression for omitted fields.
    /// Otherwise fill the missing columns with zero values of its type.
    BlockMissingValues * block_missing_values_ptr = format_settings.defaults_for_omitted_fields ? &res.block_missing_values : nullptr;

    row_group.arrow_column_to_ch_column->arrowTableToCHChunk(res.chunk, *tmp_table, (*tmp_table)->num_rows(), block_missing_values_ptr);

    lock.lock();

    ++row_group.next_chunk_idx;
    ++row_group.num_pending_chunks;
    pending_chunks.push(std::move(res));
    condvar.notify_all();
}

void ParquetBlockInputFormat::scheduleMoreWorkIfNeeded(std::optional<size_t> row_group_touched)
{
    while (row_groups_completed < row_groups.size())
    {
        auto & row_group = row_groups[row_groups_completed];
        if (row_group.status != RowGroupState::Status::Done || row_group.num_pending_chunks != 0)
            break;
        ++row_groups_completed;
    }

    if (pool)
    {
        while (row_groups_started - row_groups_completed < max_decoding_threads &&
               row_groups_started < row_groups.size())
            scheduleRowGroup(row_groups_started++);

        if (row_group_touched)
        {
            auto & row_group = row_groups[*row_group_touched];
            if (row_group.status == RowGroupState::Status::Paused &&
                row_group.num_pending_chunks < max_pending_chunks_per_row_group)
                scheduleRowGroup(*row_group_touched);
        }
    }
}

Chunk ParquetBlockInputFormat::generate()
{
    initializeIfNeeded();

    std::unique_lock lock(mutex);

    while (true)
    {
        if (background_exception)
        {
            is_stopped = true;
            std::rethrow_exception(background_exception);
        }
        if (is_stopped)
            return {};

        scheduleMoreWorkIfNeeded();

        if (!pending_chunks.empty() &&
            (!format_settings.parquet.preserve_order ||
             pending_chunks.top().row_group_idx == row_groups_completed))
        {
            PendingChunk chunk = std::move(const_cast<PendingChunk&>(pending_chunks.top()));
            pending_chunks.pop();

            auto & row_group = row_groups[chunk.row_group_idx];
            chassert(row_group.num_pending_chunks != 0);
            chassert(chunk.chunk_idx == row_group.next_chunk_idx - row_group.num_pending_chunks);
            --row_group.num_pending_chunks;

            scheduleMoreWorkIfNeeded(chunk.row_group_idx);

            previous_block_missing_values = std::move(chunk.block_missing_values);
            previous_approx_bytes_read_for_chunk = chunk.approx_original_chunk_size;
            return std::move(chunk.chunk);
        }

        if (row_groups_completed == row_groups.size())
            return {};

        if (pool)
            condvar.wait(lock);
        else
            decodeOneChunk(row_groups_completed, lock);
    }
}

void ParquetBlockInputFormat::resetParser()
{
    is_stopped = true;
    if (pool)
        pool->wait();

    arrow_file.reset();
    metadata.reset();
    column_indices.clear();
    row_groups.clear();
    while (!pending_chunks.empty())
        pending_chunks.pop();
    row_groups_completed = 0;
    previous_block_missing_values.clear();
    row_groups_started = 0;
    background_exception = nullptr;

    is_stopped = false;
    is_initialized = false;

    IInputFormat::resetParser();
}

const BlockMissingValues & ParquetBlockInputFormat::getMissingValues() const
{
    return previous_block_missing_values;
}


// TODO(RENMING):: fix this.
void registerInputFormatProcessorParquet(FormatFactory &factory)
{
    factory.registerInputFormatProcessor(
            "Parquet",
            [](ReadBuffer &buf,
                const Block &sample,
                const RowInputFormatParams &,
                const FormatSettings & settings)
            {
                return std::make_shared<ParquetBlockInputFormat>(
                    buf,
                    sample,
                    settings, 1, 1024 * 1024);
            });
    factory.markFormatAsColumnOriented("Parquet");
}


}

#else

namespace DB
{
class FormatFactory;
void registerInputFormatProcessorParquet(FormatFactory &)
{
}
}

#endif
