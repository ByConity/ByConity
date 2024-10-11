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

#pragma once
#include <Common/Logger.h>
#include "config_formats.h"
#if USE_PARQUET

#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/Impl/ParallelDecodingBlockInputFormat.h>
#include <Formats/FormatSettings.h>
#include <Common/ThreadPool.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Storages/MergeTree/KeyCondition.h>

#include <future>

namespace parquet { class FileMetaData; }
namespace parquet::arrow { class FileReader; }
namespace arrow { class Buffer; class RecordBatchReader; class Schema; }
namespace arrow::io { class RandomAccessFile; }

namespace DB
{
class ArrowFieldIndexUtil;
class ArrowColumnToCHColumn;
class ParquetRecordReader;

// Parquet files contain a metadata block with the following information:
//  * list of columns,
//  * list of "row groups",
//  * for each column in each row group:
//     - byte range for the data,
//     - min, max, count,
//     - (note that we *don't* have a reliable estimate of the decompressed+decoded size; the
//       metadata has decompressed size, but decoded size is sometimes much bigger because of
//       dictionary encoding)
//
// This information could be used for:
//  (1) Precise reads - only reading the byte ranges we need, instead of filling the whole
//      arbitrarily-sized buffer inside ReadBuffer. We know in advance exactly what ranges we'll
//      need to read.
//  (2) Skipping row groups based on WHERE conditions.
//  (3) Skipping decoding of individual pages based on PREWHERE.
//  (4) Projections. I.e. for queries that only request min/max/count, we can report the
//      min/max/count from metadata. This can be done per row group. I.e. for row groups that
//      fully pass the WHERE conditions we'll use min/max/count from metadata, for row groups that
//      only partially overlap with the WHERE conditions we'll read data.
//  (4a) Before projections are implemented, we should at least be able to do `SELECT count(*)`
//       without reading data.
//
// For (1), we need the IInputFormat to be in control of reading, with its own implementation of
// parallel reading+decoding, instead of using ParallelReadBuffer and ParallelParsingInputFormat.
// That's what RandomAccessInputCreator in FormatFactory is about.

class ParquetBlockInputFormat : public ParallelDecodingBlockInputFormat
{
public:
    ParquetBlockInputFormat(
        ReadBuffer & buf,
        const Block & header,
        const FormatSettings & format_settings,
        const ReadSettings & read_settings,
        bool is_remote_fs,
        size_t max_download_threads,
        size_t max_parsing_threads,
        SharedParsingThreadPoolPtr parsing_thread_pool);

    ~ParquetBlockInputFormat() override;

    void setQueryInfo(const SelectQueryInfo & query_info, ContextPtr context) override;

    void resetParser() override;

    String getName() const override { return "ParquetBlockInputFormat"; }

    IStorage::ColumnSizeByName getColumnSizes() override;

    bool supportsPrewhere() const override { return format_settings.parquet.use_native_reader; }

private:
    void initializeFileReader() override;
    void initializeRowGroupReaderIfNeeded(size_t row_group_idx) override;
    void resetRowGroupReader(size_t row_group_idx) override { row_group_readers[row_group_idx].reset(); }
    size_t getNumberOfRowGroups() override;

    std::optional<PendingChunk> readBatch(size_t row_group_idx) override;

    size_t getRowCount() override;

    void prefetchRowGroup(size_t row_group_idx) override;

    // Data layout in the file:
    //
    // row group 0
    //   column 0
    //     page 0, page 1, ...
    //   column 1
    //     page 0, page 1, ...
    //   ...
    // row group 1
    //   column 0
    //     ...
    //   ...
    // ...
    //
    // All columns in one row group have the same number of rows.
    // (Not necessarily the same number of *values* if there are arrays or nulls.)
    // Pages have arbitrary sizes and numbers of rows, independent from each other, even if in the
    // same column or row group.
    //
    // We can think of this as having lots of data streams, one for each column x row group.
    // The main job of this class is to schedule read operations for these streams across threads.
    // Also: reassembling the results into chunks, creating/destroying these streams, prefetching.
    //
    // Some considerations:
    //  * Row group size is typically hundreds of MB (compressed). Apache recommends 0.5 - 1 GB.
    //  * Compression ratio can be pretty extreme, especially with dictionary compression.
    //    We can afford to keep a compressed row group in memory, but not uncompressed.
    //  * For each pair <row group idx, column idx>, the data lives in one contiguous range in the
    //    file. We know all these ranges in advance, from metadata.
    //  * The byte range for a column in a row group is often very short, like a few KB.
    //    So we need to:
    //     - Avoid unnecessary readahead, e.g. don't read 1 MB when we only need 1 KB.
    //     - Coalesce nearby ranges into longer reads when needed. E.g. if we need to read 5 ranges,
    //       1 KB each, with 1 KB gaps between them, it's better to do 1 x 9 KB read instead of
    //       5 x 1 KB reads.
    //     - Have lots of parallelism for reading (not necessarily for parsing). E.g. if we're
    //       reading one small column, it may translate to hundreds of tiny reads with long gaps
    //       between them. If the data comes from an HTTP server, that's hundreds of tiny HTTP GET
    //       requests. To get good performance, we have to do tens or hundreds of them in parallel.
    //       So we should probably have separate parallelism control for IO vs parsing (since we
    //       don't want hundreds of worker threads oversubscribing the CPU cores).
    //
    // (Some motivating example access patterns:
    //   - 'SELECT small_column'. Bottlenecked on number of seeks. Need to do lots of file/network
    //     reads in parallel, for lots of row groups.
    //   - 'SELECT *' when row group size is big and there are many columns. Read the whole file.
    //     Need some moderate parallelism for IO and for parsing. Ideally read+parse columns of
    //     one row group in parallel to avoid having multiple row groups in memory at once.
    //   - 'SELECT big_column'. Have to read+parse multiple row groups in parallel.
    //   - 'SELECT big_column, many small columns'. This is a mix of the previous two scenarios.
    //     We have many columns, but still need to read+parse multiple row groups in parallel.)

    // With all that in mind, here's what we do.
    //
    // We treat each row group as a sequential single-threaded stream of blocks.
    //
    // We have a sliding window of active row groups. When a row group becomes active, we start
    // reading its data (using RAM). Row group becomes inactive when we finish reading and
    // delivering all its blocks and free the RAM. Size of the window is max_decoding_threads.
    //
    // Decoded blocks are placed in `pending_chunks` queue, then picked up by generate().
    // If row group decoding runs too far ahead of delivery (by `max_pending_chunks_per_row_group`
    // chunks), we pause the stream for the row group, to avoid using too much memory when decoded
    // chunks are much bigger than the compressed data.
    //
    // Also:
    //  * If preserve_order = true, we deliver chunks strictly in order of increasing row group.
    //    Decoding may still proceed in later row groups.
    //  * If max_decoding_threads <= 1, we run all tasks inline in generate(), without thread pool.

    // Potential improvements:
    //  * Plan all read ranges ahead of time, for the whole file, and do prefetching for them
    //    in background. Using max_download_threads, which can be made much greater than
    //    max_decoding_threads by default.
    //  * Can parse different columns within the same row group in parallel. This would let us have
    //    fewer row groups in memory at once, reducing memory usage when selecting many columns.
    //    Should probably do more than one column per task because columns are often very small.
    //    Maybe split each row group into, say, max_decoding_threads * 2 equal-sized column bunches?
    //  * Sliding window could take into account the (predicted) memory usage of row groups.
    //    If row groups are big and many columns are selected, we may use lots of memory when
    //    reading max_decoding_threads row groups at once. Can adjust the sliding window size based
    //    on row groups' data sizes from metadata.
    //  * The max_pending_chunks_per_row_group limit could be based on actual memory usage too.
    //    Useful for preserve_order.

    struct RowGroupReader
    {
        size_t row_group_bytes_uncompressed = 0;
        size_t row_group_rows = 0;

        // These are only used by the decoding thread, so don't require locking the mutex.
        bool initialized = false;

        // If use_native_reader, only native_record_reader is used;
        // otherwise, only native_record_reader is not used.
        std::shared_ptr<ParquetRecordReader> native_record_reader;
        std::unique_ptr<parquet::arrow::FileReader> file_reader;
        std::shared_ptr<arrow::RecordBatchReader> record_batch_reader;

        std::vector<int> read_column_indices;
        std::unique_ptr<ArrowColumnToCHColumn> arrow_column_to_ch_column;

        void reset()
        {
            initialized = false;
            native_record_reader.reset();
            file_reader.reset();
            record_batch_reader.reset();
            read_column_indices.clear();
            arrow_column_to_ch_column.reset();
        }
    };
    std::vector<RowGroupReader> row_group_readers;

    const ReadSettings read_settings;
    bool is_remote_fs = false;

    // RandomAccessFile is thread safe, so we share it among threads.
    // FileReader is not, so each thread creates its own.
    std::shared_ptr<arrow::io::RandomAccessFile> arrow_file;
    std::shared_ptr<parquet::FileMetaData> metadata;
    std::shared_ptr<arrow::Schema> schema;
    // indices of columns to read from Parquet file
    std::vector<int> column_indices;

    /// Pushed-down filter that we'll use to skip row groups.
    PrewhereInfoPtr prewhere_info;
    std::shared_ptr<ArrowFieldIndexUtil> field_util;

    LoggerPtr log {getLogger("ParquetBlockInputFormat")};
};

class ParquetSchemaReader : public ISchemaReader
{
public:
    ParquetSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_);

    NamesAndTypesList readSchema() override;

private:
    const FormatSettings format_settings;
};


}

#endif
