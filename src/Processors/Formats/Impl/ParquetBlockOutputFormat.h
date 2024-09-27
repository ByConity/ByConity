#pragma once

#include "config_formats.h"
#if USE_PARQUET
#    include <Processors/Formats/IOutputFormat.h>
#    include <Formats/FormatSettings.h>

namespace arrow
{
class Array;
class DataType;
}

namespace parquet
{
namespace arrow
{
    class FileWriter;
}
}

namespace DB
{

class CHColumnToArrowColumn;

class ParquetBlockOutputFormat : public IOutputFormat
{
public:
    ParquetBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_);

    String getName() const override { return "ParquetBlockOutputFormat"; }
    void consume(Chunk) override;
    void finalize() override;
    void customReleaseBuffer() override;

    String getContentType() const override { return "application/octet-stream"; }

private:
    void writeRowGroup(std::vector<Chunk> chunks);

    const FormatSettings format_settings;

    std::unique_ptr<parquet::arrow::FileWriter> file_writer;
    std::unique_ptr<CHColumnToArrowColumn> ch_column_to_arrow_column;

    /// Chunks to squash together to form a row group.
    std::vector<Chunk> staging_chunks;
    size_t staging_rows = 0;
    size_t staging_bytes = 0;
    size_t written_chunks = 0;
};

}

#endif
