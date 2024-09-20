#include "JNIArrowSource.h"
#if USE_JAVA_EXTENSIONS

#include <jni/JNIArrowReader.h>
#include <jni/JNIArrowStream.h>
#include "Formats/FormatSettings.h"
#include "Processors/Formats/Impl/ArrowColumnToCHColumn.h"

#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>

namespace DB
{

#define THROW_RESULT_NOT_OK(result) \
    do \
    { \
        if (const arrow::Status & _s = (result).status(); !_s.ok()) \
            throw Exception(_s.ToString(), ErrorCodes::BAD_ARGUMENTS); \
    } while (false)

JNIArrowSource::JNIArrowSource(Block header_, std::unique_ptr<JNIArrowReader> reader_)
    : ISource(std::move(header_)), reader(std::move(reader_))
{
}

JNIArrowSource::~JNIArrowSource() = default;

Chunk JNIArrowSource::generate()
{
    Chunk res;
    if (!std::exchange(initialized, true))
    {
        prepareReader();
    }

    ArrowArray chunk;
    if (reader->next(chunk))
    {
        auto record_batch = arrow::ImportRecordBatch(&chunk, schema);
        THROW_RESULT_NOT_OK(record_batch);
        auto table = arrow::Table::FromRecordBatches(schema, {record_batch.ValueOrDie()});
        THROW_RESULT_NOT_OK(table);
        arrow_column_to_ch_column->arrowTableToCHChunk(res, table.ValueOrDie(), (*table)->num_rows());
    }
    return res;
}

void JNIArrowSource::prepareReader()
{
    reader->initStream();
    ArrowSchema * s = &reader->getSchema();

    auto res = arrow::ImportSchema(s);
    THROW_RESULT_NOT_OK(res);
    schema = res.ValueOrDie();

    /// check schema matches header;
    arrow_column_to_ch_column = std::make_unique<ArrowColumnToCHColumn>(
        getPort().getHeader(),
        "JNI",
        false, /*import_nested*/
        false, /*allow_missing_columns*/
        true /*null_as_default*/,
        FormatSettings::DateTimeOverflowBehavior::Saturate);
}

}
#endif
