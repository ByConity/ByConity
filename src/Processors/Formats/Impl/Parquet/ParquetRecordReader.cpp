#include "ParquetRecordReader.h"

#include <bit>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include "Common/ProfileEvents.h"
#include <Common/Stopwatch.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/castColumn.h>

#include <arrow/status.h>
#include <common/logger_useful.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/column_reader.h>
#include <parquet/file_reader.h>
#include <parquet/properties.h>

#include "Columns/ColumnsCommon.h"
#include "Columns/FilterDescription.h"
#include "Core/BlockInfo.h"
#include "Core/DecimalFunctions.h"
#include "Core/Names.h"
#include "ParquetLeafColReader.h"
#include "ParquetArrowColReader.h"
#include "Processors/Formats/Impl/ArrowFieldIndexUtil.h"
#include "Processors/Formats/Impl/Parquet/ParquetDefaultColReader.h"
#include "Storages/MergeTree/MergeTreeRangeReader.h"
#include "Storages/SelectQueryInfo.h"

namespace ProfileEvents
{
    extern const Event ParquetPrewhereSkippedRows;
    extern const Event ParquetReadRows;
    extern const Event ParquetColumnCastElapsedMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int PARQUET_EXCEPTION;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

#define THROW_PARQUET_EXCEPTION(s)                                            \
    do                                                                        \
    {                                                                         \
        try { (s); }                                                          \
        catch (const ::parquet::ParquetException & e)                         \
        {                                                                     \
            throw Exception(ErrorCodes::PARQUET_EXCEPTION,                    \
                "Excepted when reading parquet {}", e.what());                \
        }                                                                     \
    } while (false)

namespace
{

std::unique_ptr<ParquetColumnReader> createLeafColReader(
    const parquet::ColumnDescriptor & col_descriptor,
    DataTypePtr ch_type,
    std::unique_ptr<parquet::ColumnChunkMetaData> meta,
    std::unique_ptr<parquet::PageReader> reader)
{
    if (col_descriptor.logical_type()->is_date() && parquet::Type::INT32 == col_descriptor.physical_type())
    {
        return std::make_unique<ParquetLeafColReader<ColumnInt32>>(
            col_descriptor, std::make_shared<DataTypeDate32>(), std::move(meta), std::move(reader));
    }
    else if (col_descriptor.logical_type()->is_decimal())
    {
        switch (col_descriptor.physical_type())
        {
            case parquet::Type::INT32:
            {
                auto data_type = std::make_shared<DataTypeDecimal32>(
                    col_descriptor.type_precision(), col_descriptor.type_scale());
                return std::make_unique<ParquetLeafColReader<ColumnDecimal<Decimal32>>>(
                    col_descriptor, data_type, std::move(meta), std::move(reader));
            }
            case parquet::Type::INT64:
            {
                auto data_type = std::make_shared<DataTypeDecimal64>(
                    col_descriptor.type_precision(), col_descriptor.type_scale());
                return std::make_unique<ParquetLeafColReader<ColumnDecimal<Decimal64>>>(
                    col_descriptor, data_type, std::move(meta), std::move(reader));
            }
            case parquet::Type::FIXED_LEN_BYTE_ARRAY:
            {
                if (col_descriptor.type_length() <= static_cast<int>(sizeof(Decimal32)))
                {
                    auto data_type = std::make_shared<DataTypeDecimal32>(
                        col_descriptor.type_precision(), col_descriptor.type_scale());
                    return std::make_unique<ParquetLeafColReader<ColumnDecimal<Decimal32>>>(
                    col_descriptor, data_type, std::move(meta), std::move(reader));
                }
                else if (col_descriptor.type_length() <= static_cast<int>(sizeof(Decimal64)))
                {
                    auto data_type = std::make_shared<DataTypeDecimal64>(
                        col_descriptor.type_precision(), col_descriptor.type_scale());
                    return std::make_unique<ParquetLeafColReader<ColumnDecimal<Decimal64>>>(
                    col_descriptor, data_type, std::move(meta), std::move(reader));
                }
                else if (col_descriptor.type_length() <= static_cast<int>(sizeof(Decimal128)))
                {
                    auto data_type = std::make_shared<DataTypeDecimal128>(
                        col_descriptor.type_precision(), col_descriptor.type_scale());
                    return std::make_unique<ParquetLeafColReader<ColumnDecimal<Decimal128>>>(
                        col_descriptor, data_type, std::move(meta), std::move(reader));
                }
                else
                {
                    auto data_type = std::make_shared<DataTypeDecimal256>(
                        col_descriptor.type_precision(), col_descriptor.type_scale());
                    return std::make_unique<ParquetLeafColReader<ColumnDecimal<Decimal256>>>(
                        col_descriptor, data_type, std::move(meta), std::move(reader));
                }
            }
            default:
                throw Exception(
                    ErrorCodes::PARQUET_EXCEPTION,
                    "Type not supported for decimal: {}",
                    col_descriptor.physical_type());
        }
    }
    else
    {
        switch (col_descriptor.physical_type())
        {
            case parquet::Type::BOOLEAN:
                return std::make_unique<ParquetLeafColReader<ColumnUInt8>>(
                    col_descriptor, std::make_shared<DataTypeUInt8>(), std::move(meta), std::move(reader));
            case parquet::Type::INT32:
                return std::make_unique<ParquetLeafColReader<ColumnInt32>>(
                    col_descriptor, std::make_shared<DataTypeInt32>(), std::move(meta), std::move(reader));
            case parquet::Type::INT64:
                return std::make_unique<ParquetLeafColReader<ColumnInt64>>(
                    col_descriptor, std::make_shared<DataTypeInt64>(), std::move(meta), std::move(reader));
            case parquet::Type::FLOAT:
                return std::make_unique<ParquetLeafColReader<ColumnFloat32>>(
                    col_descriptor, std::make_shared<DataTypeFloat32>(), std::move(meta), std::move(reader));
            case parquet::Type::INT96:
            {
                DataTypePtr read_type = ch_type;
                if (!isDateTime64(ch_type))
                {
                    read_type = std::make_shared<DataTypeDateTime64>(ParquetRecordReader::default_datetime64_scale);
                }
                return std::make_unique<ParquetLeafColReader<ColumnDecimal<DateTime64>>>(
                    col_descriptor, read_type, std::move(meta), std::move(reader));
            }
            case parquet::Type::DOUBLE:
                return std::make_unique<ParquetLeafColReader<ColumnFloat64>>(
                    col_descriptor, std::make_shared<DataTypeFloat64>(), std::move(meta), std::move(reader));
            case parquet::Type::BYTE_ARRAY:
                return std::make_unique<ParquetLeafColReader<ColumnString>>(
                    col_descriptor, std::make_shared<DataTypeString>(), std::move(meta), std::move(reader));
            default:
                throw Exception(
                    ErrorCodes::PARQUET_EXCEPTION, "Type not supported: {}", col_descriptor.physical_type());
        }
    }
}

} // anonymous namespace

// copy from MergeTreeBlockReadUtils
NameSet extractRequiredNames(Block & header, const PrewhereInfoPtr & prewhere_info)
{
    Names column_names = header.getNames();
    Names pre_column_names;
    if (prewhere_info->alias_actions)
        pre_column_names = prewhere_info->alias_actions->getRequiredColumnsNames();
    else
    {
        pre_column_names = prewhere_info->prewhere_actions->getRequiredColumnsNames();

        if (prewhere_info->row_level_filter)
        {
            NameSet names(pre_column_names.begin(), pre_column_names.end());

            for (auto & name : prewhere_info->row_level_filter->getRequiredColumnsNames())
            {
                if (names.count(name) == 0)
                    pre_column_names.push_back(name);
            }
        }
    }

    if (pre_column_names.empty())
        pre_column_names.push_back(column_names[0]);

    NameSet pre_name_set(pre_column_names.begin(), pre_column_names.end());
    return pre_name_set;
}

ParquetRecordReader::ParquetRecordReader(
    Block header_,
    std::shared_ptr<arrow::io::RandomAccessFile> source_,
    std::unique_ptr<parquet::ParquetFileReader> file_reader_,
    const parquet::ArrowReaderProperties & reader_properties_,
    const ArrowFieldIndexUtil & field_util_,
    const FormatSettings & format_settings_,
    std::vector<int> row_groups_indices_,
    PrewhereInfoPtr prewhere_info_)
    : header(std::move(header_))
    , source(source_)
    , file_reader(std::move(file_reader_))
    , reader_properties(reader_properties_)
    , field_util(field_util_)
    , format_settings(format_settings_)
    , prewhere_info(std::move(prewhere_info_))
    , max_block_size(format_settings.parquet.max_block_size)
    , row_groups_indices(std::move(row_groups_indices_))
    , log(&Poco::Logger::get("ParquetRecordReader"))
{
    /// TODO: implement our own Schema Manifest
    std::ignore = parquet::arrow::SchemaManifest::Make(
        file_reader->metadata()->schema(), file_reader->metadata()->key_value_metadata(), reader_properties, &manifest);

    NameSet pre_columns;
    if (prewhere_info)
        pre_columns = extractRequiredNames(header, prewhere_info);

    Block active_header, lazy_header;

    for (const auto & col_with_name : header)
    {
        if (pre_columns.count(col_with_name.name)) {
            active_header.insert(col_with_name);
        }
        else {
            lazy_header.insert(col_with_name);
        }
    }

    if (!active_header)
    {
        active_header.swap(lazy_header);
    }

    LOG_TRACE(log, "active_header: [{}], lazy_header: [{}]", active_header.dumpNames(), lazy_header.dumpNames());

    active_chunk_reader = ChunkReader(std::move(active_header));
    lazy_chunk_reader = ChunkReader(std::move(lazy_header));

    if (reader_properties.pre_buffer())
    {
        std::vector<int> col_indicies = field_util.findRequiredIndices(header);
        THROW_PARQUET_EXCEPTION(file_reader->PreBuffer(
            row_groups_indices, col_indicies, reader_properties.io_context(), reader_properties.cache_options()));
    }
}

ParquetRecordReader::~ParquetRecordReader() = default;

Chunk ParquetRecordReader::readChunk(BlockMissingValues * block_missing_values)
{
    auto merge_columns = [block_missing_values, this] (Columns & result, ChunkReader & chunk_reader, Columns && read_columns)
    {
        /// Reorder columns
        for (size_t i = 0; i < chunk_reader.sample_block.columns(); ++i)
        {
            const auto & col_with_name = chunk_reader.sample_block.getByPosition(i);
            size_t idx = header.getPositionByName(col_with_name.name);
            result[idx] = std::move(read_columns[i]);

            if (block_missing_values && chunk_reader.default_col_reader[i])
            {
                block_missing_values->setBits(idx, result[idx]->size());
            }
        }
    };

    while (true)
    {
        if (!cur_row_group_left_rows)
        {
            if (!loadNextRowGroup())
                return {};
        }

        size_t num_rows_read = std::min(max_block_size, cur_row_group_left_rows);
        cur_row_group_left_rows -= num_rows_read;

        Columns result_columns(header.columns());
        Columns active_columns = active_chunk_reader.readBatch(num_rows_read, nullptr);
        Block active_block = active_chunk_reader.sample_block.cloneWithColumns(active_columns);
        ProfileEvents::increment(ProfileEvents::ParquetReadRows, active_block.rows());

        ColumnPtr filter = active_chunk_reader.executePrewhereAction(active_block, prewhere_info);

        if (!filter)
        {
            merge_columns(result_columns, active_chunk_reader, std::move(active_columns));
            return Chunk(std::move(result_columns), num_rows_read);
        }

        Columns lazy_columns;
        ConstantFilterDescription const_filter_desc(*filter);
        if (const_filter_desc.always_true)
        {
            lazy_columns = lazy_chunk_reader.readBatch(num_rows_read, nullptr);
            merge_columns(result_columns, active_chunk_reader, std::move(active_columns));
            merge_columns(result_columns, lazy_chunk_reader, std::move(lazy_columns));
            return Chunk(std::move(result_columns), num_rows_read);
        }

        if (const_filter_desc.always_false)
        {
            lazy_chunk_reader.skip(num_rows_read);
            ProfileEvents::increment(ProfileEvents::ParquetPrewhereSkippedRows, num_rows_read);
            continue;
        }

        FilterDescription filter_desc(*filter);
        if (!filter_desc.hasOnes())
        {
            lazy_chunk_reader.skip(num_rows_read);
            ProfileEvents::increment(ProfileEvents::ParquetPrewhereSkippedRows, num_rows_read);
            continue;
        }

        lazy_columns = lazy_chunk_reader.readBatch(num_rows_read, filter_desc.data);
        for (auto &col : active_columns)
            col = filter_desc.filter(*col, -1);
        for (auto &col : lazy_columns)
            col = filter_desc.filter(*col, -1);

        size_t num_rows = active_columns.empty() ? 0 : active_columns.front()->size();

        merge_columns(result_columns, active_chunk_reader, std::move(active_columns));
        merge_columns(result_columns, lazy_chunk_reader, std::move(lazy_columns));
        return Chunk(std::move(result_columns), num_rows);
    }
}

bool ParquetRecordReader::loadNextRowGroup()
{
    Stopwatch watch(CLOCK_MONOTONIC);
    if (next_row_group_idx >= 0 && static_cast<size_t>(next_row_group_idx) >= row_groups_indices.size())
        return false;

    cur_row_group_reader = file_reader->RowGroup(row_groups_indices[next_row_group_idx]);
    cur_row_group_left_rows = cur_row_group_reader->metadata()->num_rows();

    active_chunk_reader.init(*this);
    lazy_chunk_reader.init(*this);

    ++next_row_group_idx;
    return true;
}

std::unique_ptr<ParquetColumnReader> ParquetRecordReader::createColReader(
    const ColumnWithTypeAndName & ch_column,
    const std::vector<int> & arrow_col_indicies)
{
    /// missing column
    if (arrow_col_indicies.empty())
    {
        return std::make_unique<ParquetDefaultColReader>(ch_column.type);
    }

    std::vector<int> field_indices = manifest.GetFieldIndices(arrow_col_indicies).ValueOrDie();
    const parquet::arrow::SchemaField & schema_field = manifest.schema_fields[field_indices[0]];
    return createColReader(ch_column, schema_field, arrow_col_indicies);
}

std::unique_ptr<ParquetColumnReader> ParquetRecordReader::createColReader(
    const ColumnWithTypeAndName & ch_column,
    const parquet::arrow::SchemaField & schema_field,
    // const std::vector<int> & leaf_field_indicies,
    const std::vector<int> & arrow_col_indicies)
{
    if (schema_field.is_leaf())
    {
        int col_idx = schema_field.column_index;
        return createLeafColReader(
            *file_reader->metadata()->schema()->Column(schema_field.column_index),
            ch_column.type,
            cur_row_group_reader->metadata()->ColumnChunk(col_idx),
            cur_row_group_reader->GetColumnPageReader(col_idx));
    }

    /// just fallback to arrow reader
    return std::make_unique<ParquetArrowColReader>(
        ch_column,
        arrow_col_indicies,
        std::vector<int>{row_groups_indices[next_row_group_idx]},
        *file_reader,
        source,
        reader_properties,
        format_settings);
}

void ParquetRecordReader::ChunkReader::init(ParquetRecordReader & record_reader)
{
    column_readers.resize(sample_block.columns());
    default_col_reader.assign(sample_block.columns(), false);

    for (size_t i = 0; i < sample_block.columns(); ++i)
    {
        auto & col = sample_block.getByPosition(i);
        std::vector<int> arrow_col_indicies = record_reader.field_util.findRequiredIndices(col.name);
        if (arrow_col_indicies.empty())
            default_col_reader[i] = true;

        column_readers[i] = record_reader.createColReader(col, arrow_col_indicies);
    }
}

Columns ParquetRecordReader::ChunkReader::readBatch(size_t num_rows, const IColumn::Filter *filter)
{
    Columns columns(sample_block.columns());
    for (size_t i = 0; i < sample_block.columns(); i++)
    {
        auto res = column_readers[i]->readBatch(sample_block.getByPosition(i).name, num_rows, filter);

        {
            Stopwatch watch;
            columns[i] = castColumn(std::move(res), sample_block.getByPosition(i).type);
            ProfileEvents::increment(ProfileEvents::ParquetColumnCastElapsedMicroseconds, watch.elapsedMicroseconds());
        }
    }

    return columns;
}

void ParquetRecordReader::ChunkReader::skip(size_t num_rows)
{
    for (size_t i = 0; i < sample_block.columns(); i++)
        column_readers[i]->skip(num_rows);
}

ColumnPtr ParquetRecordReader::ChunkReader::executePrewhereAction(Block & block, const PrewhereInfoPtr & prewhere_info_ptr)
{
    if (!prewhere_info_ptr)
        return {};

    if (!prewhere_actions)
    {
        prewhere_actions = std::make_shared<PrewhereExprInfo>();
        if (prewhere_info_ptr->alias_actions)
            prewhere_actions->alias_actions = std::make_shared<ExpressionActions>(prewhere_info_ptr->alias_actions, ExpressionActionsSettings{});
        if (prewhere_info_ptr->row_level_filter)
            prewhere_actions->row_level_filter = std::make_shared<ExpressionActions>(prewhere_info_ptr->row_level_filter, ExpressionActionsSettings{});

        prewhere_actions->prewhere_actions = std::make_shared<ExpressionActions>(prewhere_info_ptr->prewhere_actions, ExpressionActionsSettings{});
        prewhere_actions->row_level_column_name = prewhere_info_ptr->row_level_column_name;
        prewhere_actions->prewhere_column_name = prewhere_info_ptr->prewhere_column_name;
        prewhere_actions->remove_prewhere_column = prewhere_info_ptr->remove_prewhere_column;
        prewhere_actions->need_filter = prewhere_info_ptr->need_filter;
    }

    if (prewhere_actions->alias_actions)
        prewhere_actions->alias_actions->execute(block);
    if (prewhere_actions->row_level_filter)
        prewhere_actions->row_level_filter->execute(block);

    prewhere_actions->prewhere_actions->execute(block);
    return block.getByName(prewhere_info_ptr->prewhere_column_name).column;
}

}
