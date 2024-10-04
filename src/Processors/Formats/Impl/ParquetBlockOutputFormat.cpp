#include "ParquetBlockOutputFormat.h"

#if USE_PARQUET

// TODO: clean includes
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Common/assert_cast.h>
#include <Core/callOnTypeIndex.h>
#include <DataStreams/SquashingBlockOutputStream.h>
#include "DataStreams/materializeBlock.h"
#include <Formats/FormatFactory.h>
#include <IO/WriteHelpers.h>
#include <arrow/api.h>
#include <arrow/util/memory.h>
#include <parquet/arrow/writer.h>
#include "ArrowBufferedStreams.h"
#include "CHColumnToArrowColumn.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
}

namespace
{

    parquet::ParquetVersion::type getParquetVersion(const FormatSettings & settings)
    {
        switch (settings.parquet.output_version)
        {
            case FormatSettings::ParquetVersion::V1_0:
                return parquet::ParquetVersion::PARQUET_1_0;
            case FormatSettings::ParquetVersion::V2_4:
                return parquet::ParquetVersion::PARQUET_2_4;
            case FormatSettings::ParquetVersion::V2_6:
                return parquet::ParquetVersion::PARQUET_2_6;
            case FormatSettings::ParquetVersion::V2_LATEST:
                return parquet::ParquetVersion::PARQUET_2_LATEST;
        }
    }

    parquet::Compression::type getParquetCompression(FormatSettings::ParquetCompression method)
    {
        if (method == FormatSettings::ParquetCompression::NONE)
            return parquet::Compression::type::UNCOMPRESSED;

#if USE_SNAPPY
        if (method == FormatSettings::ParquetCompression::SNAPPY)
            return parquet::Compression::type::SNAPPY;
#endif

#if USE_BROTLI
        if (method == FormatSettings::ParquetCompression::BROTLI)
            return parquet::Compression::type::BROTLI;
#endif

        if (method == FormatSettings::ParquetCompression::ZSTD)
            return parquet::Compression::type::ZSTD;

        if (method == FormatSettings::ParquetCompression::LZ4)
            return parquet::Compression::type::LZ4;

        if (method == FormatSettings::ParquetCompression::GZIP)
            return parquet::Compression::type::GZIP;

        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported parquet compression method");
    }
}

ParquetBlockOutputFormat::ParquetBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_)
    : IOutputFormat(header_, out_), format_settings{format_settings_}
{
}

void ParquetBlockOutputFormat::consume(Chunk chunk)
{
    /// Do something like SquashingTransform to produce big enough row groups.
    /// Because the real SquashingTransform is only used for INSERT, not for SELECT ... INTO OUTFILE.
    /// The latter doesn't even have a pipeline where a transform could be inserted, so it's more
    /// convenient to do the squashing here. It's also parallelized here.

    if (chunk.getNumRows() != 0)
    {
        staging_rows += chunk.getNumRows();
        staging_bytes += chunk.bytes();
        staging_chunks.push_back(std::move(chunk));
    }

    const size_t target_rows = std::max(static_cast<UInt64>(1), format_settings.parquet.row_group_rows);

    if (staging_rows < target_rows &&
        staging_bytes < format_settings.parquet.row_group_bytes)
        return;

    /// In the rare case that more than `row_group_rows` rows arrived in one chunk, split the
    /// staging chunk into multiple row groups.
    if (staging_rows >= target_rows * 2)
    {
        /// Increase row group size slightly (by < 2x) to avoid a small row group at the end.
        size_t num_row_groups = std::max(static_cast<size_t>(1), staging_rows / target_rows);
        size_t row_group_size = (staging_rows - 1) / num_row_groups + 1; // round up

        Chunk concatenated = std::move(staging_chunks[0]);
        for (size_t i = 1; i < staging_chunks.size(); ++i)
            concatenated.append(staging_chunks[i]);
        staging_chunks.clear();

        for (size_t offset = 0; offset < staging_rows; offset += row_group_size)
        {
            size_t count = std::min(row_group_size, staging_rows - offset);
            MutableColumns columns = concatenated.cloneEmptyColumns();
            for (size_t i = 0; i < columns.size(); ++i)
                columns[i]->insertRangeFrom(*concatenated.getColumns()[i], offset, count);

            Chunks piece;
            piece.emplace_back(std::move(columns), count, concatenated.getChunkInfo());
            writeRowGroup(std::move(piece));
        }
    }
    else
    {
        writeRowGroup(std::move(staging_chunks));
    }

    staging_chunks.clear();
    staging_rows = 0;
    staging_bytes = 0;
}

void ParquetBlockOutputFormat::finalize()
{
    if (!staging_chunks.empty())
        writeRowGroup(std::move(staging_chunks));

    if (!file_writer)
    {
        Block header = materializeBlock(getPort(PortKind::Main).getHeader());
        std::vector<Chunk> chunks;
        chunks.push_back(Chunk(header.getColumns(), 0));
        writeRowGroup(std::move(chunks));
    }

    if (file_writer)
    {
        auto status = file_writer->Close();
        if (!status.ok())
            throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Error while closing a table: {}", status.ToString());
    }
}

void ParquetBlockOutputFormat::writeRowGroup(std::vector<Chunk> chunks)
{
    const size_t columns_num = chunks.at(0).getNumColumns();
    std::shared_ptr<arrow::Table> arrow_table;

    if (!ch_column_to_arrow_column)
    {
        const Block & header = getPort(PortKind::Main).getHeader();
        ch_column_to_arrow_column = std::make_unique<CHColumnToArrowColumn>(
            header,
            "Parquet",
            false,
            format_settings.parquet.output_string_as_string,
            format_settings.parquet.output_fixed_string_as_fixed_byte_array);
    }

    ch_column_to_arrow_column->chChunkToArrowTable(arrow_table, chunks, columns_num);

    if (!file_writer)
    {
        auto sink = std::make_shared<ArrowBufferedOutputStream>(out);

        parquet::WriterProperties::Builder builder;
        builder.version(getParquetVersion(format_settings));
        builder.compression(getParquetCompression(format_settings.parquet.output_compression_method));

        parquet::ArrowWriterProperties::Builder writer_props_builder;
        if (format_settings.parquet.output_compliant_nested_types)
            writer_props_builder.enable_compliant_nested_types();
        else
            writer_props_builder.disable_compliant_nested_types();

        auto result = parquet::arrow::FileWriter::Open(
            *arrow_table->schema(),
            ArrowMemoryPool::instance(),
            sink,
            builder.build(),
            writer_props_builder.build());
        if (!result.ok())
            throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Error while opening a table: {}", result.status().ToString());
        file_writer = std::move(result.ValueOrDie());
    }

    auto status = file_writer->WriteTable(*arrow_table, INT64_MAX);

    if (!status.ok())
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Error while writing a table: {}", status.ToString());
}

void ParquetBlockOutputFormat::customReleaseBuffer()
{
    if (file_writer)
    {
        auto status = file_writer->Close();
        if (!status.ok())
            throw Exception{"Error while closing a table: " + status.ToString(), ErrorCodes::UNKNOWN_EXCEPTION};

        file_writer.reset();
    }
}

void registerOutputFormatProcessorParquet(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor(
        "Parquet", [](WriteBuffer & buf, const Block & sample, const RowOutputFormatParams &, const FormatSettings & format_settings) {
            auto impl = std::make_shared<ParquetBlockOutputFormat>(buf, sample, format_settings);
            /// TODO
            // auto res = std::make_shared<SquashingBlockOutputStream>(impl, impl->getHeader(), format_settings.parquet.row_group_size, 0);
            // res->disableFlush();
            return impl;
        });
}

}

#else

namespace DB
{
class FormatFactory;
void registerOutputFormatProcessorParquet(FormatFactory &)
{
}
}

#endif
