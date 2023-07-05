#include "ORCBlockInputFormat.h"
#if USE_ORC

#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <arrow/adapters/orc/adapter.h>
#include <arrow/io/memory.h>
#include "ArrowBufferedStreams.h"
#include "ArrowColumnToCHColumn.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_READ_ALL_DATA;
}

    // #define THROW_ARROW_NOT_OK(status)                                     \
    // do                                                                 \
    // {                                                                  \
    //     if (::arrow::Status _s = (status); !_s.ok())                   \
    //         throw Exception(_s.ToString(), ErrorCodes::BAD_ARGUMENTS); \
    // } while (false)

ORCBlockInputFormat::ORCBlockInputFormat(
    ReadBuffer & in_,
    Block header_,
    const FormatSettings & format_settings_,
    const std::map<String, String> & partition_kv_)
    : IInputFormat(std::move(header_), in_)
    , format_settings(format_settings_)
    , partition_kv{partition_kv_}
    , read_stripe(format_settings_.orc.read_stripe)
 {
 }

Chunk ORCBlockInputFormat::generate()
{
    Chunk res;

    if (!file_reader)
        prepareReader();

    if (stripe_current >= stripe_total)
        return res;

    // std::shared_ptr<arrow::RecordBatch> batch_result;
    std::shared_ptr<arrow::Table> table;    
    
    
    /// We should extract the number of rows directly from the stripe, because in case when
    /// record batch contains 0 columns (for example if we requested only columns that
    /// are not presented in data) the number of rows in record batch will be 0.
    if(!include_indices.empty())
    {
    auto batch_status = file_reader->ReadStripe(stripe_current, include_indices );
    if (!batch_status.ok())
        throw ParsingException(ErrorCodes::CANNOT_READ_ALL_DATA,
                               "Error while reading batch of ORC data: {}", batch_status.status().ToString());


    auto status = arrow::Table::FromRecordBatches({batch_status.ValueOrDie()}).Value(&table);
    
    if (!status.ok())
        throw ParsingException(ErrorCodes::CANNOT_READ_ALL_DATA,
                               "Error while reading batch of ORC data: {}", status.ToString());
    } else {
        size_t num_rows = file_reader->GetRawORCReader()->getStripe(stripe_current)->getNumberOfRows();
        std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;
        arrow::FieldVector fields; 
        table = arrow::Table::Make(std::make_shared<arrow::Schema>(fields),columns, num_rows);
    }
    
    ++stripe_current;

    arrow_column_to_ch_column->arrowTableToCHChunk(res, table);
    return res;
}

void ORCBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();

    file_reader.reset();
    include_indices.clear();
    stripe_current = 0;
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

void ORCBlockInputFormat::prepareReader()
{
    auto arrow_file = asArrowFile(in);
    auto reader_status = arrow::adapters::orc::ORCFileReader::Open(arrow_file,arrow::default_memory_pool()).Value(&file_reader);
    if(!reader_status.ok())
    {
        throw Exception(reader_status.ToString(), ErrorCodes::BAD_ARGUMENTS); 
    }
    stripe_total = file_reader->NumberOfStripes();
    if(read_stripe)
        stripe_current = format_settings.orc.current_stripe;
    stripe_current = 0;

    auto schema_status = file_reader->ReadSchema();
    if(!schema_status.ok())
    {
        throw Exception(schema_status.status().ToString(), ErrorCodes::BAD_ARGUMENTS); 
    }

    auto & schema = schema_status.ValueOrDie();

    arrow_column_to_ch_column = std::make_unique<ArrowColumnToCHColumn>(
        getPort().getHeader(),
        schema,
        "ORC",
        format_settings.orc.allow_missing_columns,
        format_settings.null_as_default,
        partition_kv);

    int index = 0;
    for (int i = 0; i < schema->num_fields(); ++i)
    {
        /// LIST type require 2 indices, STRUCT - the number of elements + 1,
        /// so we should recursively count the number of indices we need for this type.
        int indexes_count = countIndicesForType(schema->field(i)->type());
        if (getPort().getHeader().has(schema->field(i)->name()))
        {
            for (int j = 0; j != indexes_count; ++j)
                include_indices.push_back(index + j);
        }
        index += indexes_count;
    }
}

void registerInputFormatProcessorORC(FormatFactory &factory)
{
    factory.registerInputFormatProcessor(
            "ORC",
            [](ReadBuffer &buf,
                const Block &sample,
                const RowInputFormatParams &,
                const FormatSettings & settings)
            {
                return std::make_shared<ORCBlockInputFormat>(buf, sample, settings, settings.orc.partition_kv);
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
