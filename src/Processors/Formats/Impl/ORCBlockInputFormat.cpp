#include "ORCBlockInputFormat.h"
#include <boost/algorithm/string/case_conv.hpp>
#include <unordered_map>
#include "common/logger_useful.h"
#include "DataTypes/NestedUtils.h"
#include "Processors/Formats/Impl/LMNativeORCBlockInputFormat.h"
#include "Processors/Formats/Impl/OrcCommon.h"
#include "Storages/IStorage.h"
#if USE_ORC

#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <arrow/adapters/orc/adapter.h>
#include <arrow/io/memory.h>
#include "ArrowBufferedStreams.h"
#include "ArrowColumnToCHColumn.h"
#include <DataTypes/NestedUtils.h>

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

ORCBlockInputFormat::ORCBlockInputFormat(ReadBuffer & in_, Block header_, const FormatSettings & format_settings_)
    : IInputFormat(std::move(header_), in_), format_settings(format_settings_)
{
}

Chunk ORCBlockInputFormat::generate()
{
    block_missing_values.clear();

    Chunk res;

    if (!file_reader)
        prepareReader();

    const auto & skip_stripes = format_settings.orc.skip_stripes;
    while (!skip_stripes.empty() && stripe_current < stripe_total && skip_stripes.contains(stripe_current))
        ++stripe_current;

    if (stripe_current >= stripe_total)
        return res;

    auto batch_status = file_reader->ReadStripe(stripe_current, include_indices );
    if (!batch_status.ok())
        throw ParsingException(
            ErrorCodes::CANNOT_READ_ALL_DATA, "Error while reading batch of ORC data: {}", batch_status.status().ToString());

    auto batch = batch_status.ValueOrDie();
    if (!batch)
        return {};

    auto table_result = arrow::Table::FromRecordBatches({batch});
    if (!table_result.ok())
        throw ParsingException(
            ErrorCodes::CANNOT_READ_ALL_DATA, "Error while reading batch of ORC data: {}", table_result.status().ToString());


    /// We should extract the number of rows directly from the stripe, because in case when
    /// record batch contains 0 columns (for example if we requested only columns that
    /// are not presented in data) the number of rows in record batch will be 0.
    size_t num_rows = file_reader->GetRawORCReader()->getStripe(stripe_current)->getNumberOfRows();

    auto table = table_result.ValueOrDie();
    if (!table || !num_rows)
        return {};

    ++stripe_current;
    BlockMissingValues * block_missing_values_ptr = format_settings.defaults_for_omitted_fields ? &block_missing_values : nullptr;
    arrow_column_to_ch_column->arrowTableToCHChunk(res, table, num_rows, block_missing_values_ptr);
    return res;
}

void ORCBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();

    file_reader.reset();
    include_indices.clear();
    stripe_current = 0;
    block_missing_values.clear();
}

const BlockMissingValues & ORCBlockInputFormat::getMissingValues() const
{
    return block_missing_values;
}

static size_t countIndicesForType(std::shared_ptr<arrow::DataType> type)
{
    if (type->id() == arrow::Type::LIST)
        return countIndicesForType(static_cast<arrow::ListType *>(type.get())->value_type()) + 1;

    if (type->id() == arrow::Type::STRUCT)
    {
        int indices = 1;
        auto * struct_type = static_cast<arrow::StructType *>(type.get());
        for (int i = 0; i != struct_type->num_fields(); ++i)
            indices += countIndicesForType(struct_type->field(i)->type());
        return indices;
    }

    if (type->id() == arrow::Type::MAP)
    {
        auto * map_type = static_cast<arrow::MapType *>(type.get());
        return countIndicesForType(map_type->key_type()) + countIndicesForType(map_type->item_type());
    }

    return 1;
}

std::vector<int> ORCBlockInputFormat::getColumnIndices(
    const std::shared_ptr<arrow::Schema> & schema, const Block & header, const bool & ignore_case, const bool & import_nested)
{
    std::vector<int> include_indices;
    std::unordered_set<String> nested_table_names;
    if (import_nested)
        nested_table_names = Nested::getAllTableNames(header, ignore_case);

    for (int i = 0; i < schema->num_fields(); ++i)
    {
        const auto & name = schema->field(i)->name();
        if (header.has(name, ignore_case) || nested_table_names.contains(ignore_case ? boost::to_lower_copy(name) : name))
            include_indices.push_back(i);
    }
    return include_indices;
}

void ORCBlockInputFormat::prepareReader()
{
    std::atomic_int is_stopped = false;
    auto arrow_file = asArrowFile(in, format_settings, is_stopped, "ORC", ORC_MAGIC_BYTES);
    THROW_ARROW_NOT_OK(arrow::adapters::orc::ORCFileReader::Open(arrow_file, ArrowMemoryPool::instance()).Value(&file_reader));
    stripe_total = file_reader->NumberOfStripes();
    stripe_current = 0;

    auto schema_status = file_reader->ReadSchema();
    if (!schema_status.ok())
    {
        throw Exception(schema_status.status().ToString(), ErrorCodes::BAD_ARGUMENTS);
    }

    if (!schema_status.ok())
    {
        throw Exception(schema_status.status().ToString(), ErrorCodes::BAD_ARGUMENTS);
    }

    auto & schema = schema_status.ValueOrDie();

    arrow_column_to_ch_column = std::make_unique<ArrowColumnToCHColumn>(
        getPort().getHeader(),
        "ORC",
        format_settings.orc.import_nested,
        format_settings.orc.allow_missing_columns,
        format_settings.null_as_default,
        format_settings.date_time_overflow_behavior,
        format_settings.orc.case_insensitive_column_matching);

    include_indices = getColumnIndices(schema, getPort().getHeader(), format_settings.orc.case_insensitive_column_matching, format_settings.orc.import_nested);
}

IStorage::ColumnSizeByName ORCBlockInputFormat::getColumnSizes()
{
    std::atomic_int is_stopped = false;
    THROW_ARROW_NOT_OK(arrow::adapters::orc::ORCFileReader::Open(
                           asArrowFile(in, format_settings, is_stopped, "ORC", ORC_MAGIC_BYTES), ArrowMemoryPool::instance())
                           .Value(&file_reader));
    auto * orc_reader = file_reader->GetRawORCReader();
    if (!orc_reader)
    {
        LOG_INFO(&Poco::Logger::get("ORCBlockInputFormat"), "cannot get columns size, raw reader ptr is nullptr.");
        return {};
    }
    return getOrcColumnsSize(*orc_reader);
}

void ORCBlockInputFormat::setQueryInfo([[maybe_unused]] const SelectQueryInfo & query_info, [[maybe_unused]] ContextPtr local_context)
{
    // if(auto select_query = dynamic_cast<ASTSelectQuery*>(query_info.query.get()); select_query)
    // {
    //     if(select_query->prewhere())
    //     {
    //         throw Exception(ErrorCodes::BAD_ARGUMENTS, "NativeORCBlockInputFormat does not support prewhere");
    //     }
    // }
}


void registerInputFormatProcessorORC(FormatFactory &factory)
{
    factory.registerRandomAccessInputFormat(
        "ORC",
        [](ReadBuffer & buf,
           const Block & sample,
           const FormatSettings & settings,
           const ReadSettings & /*read_settings*/,
           bool /*is_remote_fs*/,
           size_t max_download_threads,
           size_t max_parsing_threads,
           SharedParsingThreadPoolPtr parsing_thread_pool) -> InputFormatPtr {
            if (settings.orc.use_fast_decoder >= 1)
                return std::make_shared<LMNativeORCBlockInputFormat>(
                    buf, sample, settings, max_download_threads, max_parsing_threads, parsing_thread_pool);
            else
                return std::make_shared<ORCBlockInputFormat>(buf, sample, settings);
        });
    factory.markFormatAsColumnOriented("ORC");
}
}
#else

namespace DB
{
class FormatFactory;
void registerInputFormatProcessorORC(FormatFactory &)
{
}
}

#endif
