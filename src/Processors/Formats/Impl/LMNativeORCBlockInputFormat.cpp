#include "Processors/Formats/Impl/LMNativeORCBlockInputFormat.h"
#include <optional>
#include "AggregateFunctions/AggregateFunnelCommon.h"
#include "Processors/Formats/Impl/OrcCommon.h"
#include "Processors/Formats/Impl/ParallelDecodingBlockInputFormat.h"
#if USE_ORC

#    include <mutex>
#    include <Columns/ColumnDecimal.h>
#    include <Columns/ColumnFixedString.h>
#    include <Columns/ColumnMap.h>
#    include <Columns/ColumnNullable.h>
#    include <Columns/ColumnString.h>
#    include <Columns/ColumnsDateTime.h>
#    include <Columns/ColumnsNumber.h>
#    include <DataTypes/DataTypeArray.h>
#    include <DataTypes/DataTypeDate32.h>
#    include <DataTypes/DataTypeDateTime64.h>
#    include <DataTypes/DataTypeFactory.h>
#    include <DataTypes/DataTypeFixedString.h>
#    include <DataTypes/DataTypeMap.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <DataTypes/DataTypeString.h>
#    include <DataTypes/DataTypeTuple.h>
#    include <DataTypes/DataTypesDecimal.h>
#    include <DataTypes/DataTypesNumber.h>
#    include <DataTypes/NestedUtils.h>
#    include <Formats/FormatFactory.h>
#    include <Formats/insertNullAsDefaultIfNeeded.h>
#    include <IO/ReadBufferFromMemory.h>
#    include <IO/WriteHelpers.h>
#    include <IO/copyData.h>
#    include <Interpreters/castColumn.h>
#    include <arrow/io/caching.h>
#    include <boost/algorithm/string/case_conv.hpp>
#    include <orc/MemoryPool.hh>
#    include <parquet/arrow/reader.h>
#    include <parquet/properties.h>
#    include "Common/Exception.h"
#    include "Common/Stopwatch.h"
#    include "ArrowBufferedStreams.h"
#    include "ArrowColumnCache.h"
#    include "IO/WithFileSize.h"
#    include "Storages/MergeTree/KeyCondition.h"


namespace ProfileEvents
{
extern const Event OrcSkippedRows;
extern const Event OrcTotalRows;
}
namespace DB
{
namespace ErrorCodes
{
    extern const int SYSTEM_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TYPE;
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
    extern const int THERE_IS_NO_COLUMN;
    extern const int INCORRECT_DATA;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int CANNOT_READ_ALL_DATA;
}

LMNativeORCBlockInputFormat::LMNativeORCBlockInputFormat(
    ReadBuffer & in_,
    const Block & header_,
    const FormatSettings & format_settings_,
    size_t max_download_threads_,
    size_t max_parsing_threads_,
    SharedParsingThreadPoolPtr parsing_thread_pool)
    : ParallelDecodingBlockInputFormat(
        in_,
        header_,
        format_settings_,
        max_download_threads_,
        max_parsing_threads_,
        format_settings_.parquet.preserve_order,
        format_settings_.orc.skip_stripes,
        std::move(parsing_thread_pool))
{
    scan_params.header = header_;
    scan_params.in = &in;
    scan_params.format_settings = format_settings_;
    scan_params.chunk_size = format_settings_.orc.row_batch_size;
}

LMNativeORCBlockInputFormat::~LMNativeORCBlockInputFormat()
{
    close();
}

void LMNativeORCBlockInputFormat::resetParser()
{
    ParallelDecodingBlockInputFormat::resetParser();
    scanners.clear();
    orc_file_reader.reset();
}

void LMNativeORCBlockInputFormat::setQueryInfo(const SelectQueryInfo & query_info, ContextPtr local_context)
{
    scan_params.select_query_info = query_info;
    scan_params.local_context = local_context;
}


IStorage::ColumnSizeByName LMNativeORCBlockInputFormat::getColumnSizes()
{
    initializeFileReader();
    return getOrcColumnsSize(*orc_file_reader);
}

void LMNativeORCBlockInputFormat::initializeFileReader()
{
    // std::atomic<int> is_stopped;
    auto arrow_file
        = asArrowFile(*scan_params.in, scan_params.format_settings, is_stopped, "ORC", ORC_MAGIC_BYTES, /* avoid_buffering */ true);
    orc::ReaderOptions options;
    if (scan_params.orc_tail)
    {
        options.setSerializedFileTail(scan_params.orc_tail.value());
    }
    // options.setMemoryPool(scanner_mem_pool);
    orc_file_reader = orc::createReader(std::make_unique<CachedORCArrowInputStream>(arrow_file), options);
    if (!orc_file_reader)
    {
        // TODO(renming): add file name here.
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "cannot create orc file reader for file");
    }
    auto num_strips = orc_file_reader->getNumberOfStripes();
    scanners.resize(num_strips);
    init_scanners_once.resize(num_strips);
    for (size_t i = 0; i < num_strips; i++)
    {
        init_scanners_once[i] = std::make_unique<std::once_flag>();
    }
}

void LMNativeORCBlockInputFormat::initializeRowGroupReaderIfNeeded(size_t row_group_idx)
{
    std::call_once(*init_scanners_once[row_group_idx], [&]() {
        auto stripe_info = orc_file_reader->getStripe(row_group_idx);
        ScanParams current = scan_params;
        // auto number_of_stripe = orc_file_reader->getNumberOfStripes();
        // auto number_of_rows = stripe_info->getNumberOfRows();
        // LOG_INFO(log, "strips {} rows {}", number_of_stripe, number_of_rows);
        current.range_start = stripe_info->getOffset();
        current.range_length = stripe_info->getLength();
        current.orc_tail = orc_file_reader->getSerializedFileTail();
        auto scanner = std::make_unique<OrcScanner>(current);
        auto status = scanner->init();
        if (!status.ok())
        {
            throw DB::Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "cannot initialize OrcScanner, error: {}", status.ToString());
        }
        scanners[row_group_idx] = std::move(scanner);
    });
}

std::optional<ParallelDecodingBlockInputFormat::PendingChunk> LMNativeORCBlockInputFormat::readBatch(size_t row_group_idx)
{
    auto & row_group_status = row_groups[row_group_idx];
    Block block;
    auto status = scanners[row_group_idx]->readNext(block);
    if (status.IsEndOfFile())
    {
        return std::nullopt;
    }
    if (!status.ok())
    {
        throw DB::Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "cannot read orc file, err: {}", status.ToString());
    }
    ParallelDecodingBlockInputFormat::PendingChunk chunk{
        .chunk = Chunk(block.getColumns(), block.rows()),
        .block_missing_values = {},
        .chunk_idx = row_group_status.next_chunk_idx,
        .row_group_idx = row_group_idx,
        .approx_original_chunk_size = 0,
    };
    return chunk;
}

size_t LMNativeORCBlockInputFormat::getNumberOfRowGroups()
{
    return orc_file_reader->getNumberOfStripes();
}

void LMNativeORCBlockInputFormat::resetRowGroupReader(size_t row_group_idx)
{
    scanners[row_group_idx].reset();
}
}
#endif
