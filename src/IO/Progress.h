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

#include <atomic>
#include <cstddef>
#include <common/types.h>

#include <Core/ProtocolDefines.h>
#include <Protos/plan_segment_manager.pb.h>
#include <Common/Stopwatch.h>


namespace DB
{

class ReadBuffer;
class WriteBuffer;

/// See Progress.
struct ProgressValues
{
    size_t read_rows;
    size_t read_bytes;
    size_t read_raw_bytes;

    size_t total_rows_to_read;
    size_t total_raw_bytes_to_read;

    size_t disk_cache_read_bytes;

    size_t written_rows;
    size_t written_bytes;

    void read(ReadBuffer & in, UInt64 server_revision);
    void write(WriteBuffer & out, UInt64 client_revision) const;
    void writeJSON(WriteBuffer & out) const;
    Protos::Progress toProto() const;
    void fromProto(const Protos::Progress & progress);
    bool empty() const;
    ProgressValues operator-(const ProgressValues & other) const;
    ProgressValues operator+(const ProgressValues & other) const;
    bool operator==(const ProgressValues & other) const;
    String toString() const;
};

struct ReadProgress
{
    size_t read_rows;
    size_t read_bytes;
    size_t total_rows_to_read;
    size_t disk_cache_read_bytes;

    ReadProgress(size_t read_rows_, size_t read_bytes_, size_t total_rows_to_read_ = 0, size_t disk_cache_read_bytes_ = 0)
        : read_rows(read_rows_)
        , read_bytes(read_bytes_)
        , total_rows_to_read(total_rows_to_read_)
        , disk_cache_read_bytes(disk_cache_read_bytes_) {}
};

struct WriteProgress
{
    size_t written_rows;
    size_t written_bytes;

    WriteProgress(size_t written_rows_, size_t written_bytes_)
        : written_rows(written_rows_), written_bytes(written_bytes_) {}
};

struct FileProgress
{
    /// Here read_bytes (raw bytes) - do not equal ReadProgress::read_bytes, which are calculated according to column types.
    size_t read_bytes;
    size_t total_bytes_to_read;

    FileProgress(size_t read_bytes_, size_t total_bytes_to_read_ = 0) : read_bytes(read_bytes_), total_bytes_to_read(total_bytes_to_read_) {}
};


/** Progress of query execution.
  * Values, transferred over network are deltas - how much was done after previously sent value.
  * The same struct is also used for summarized values.
  */
struct Progress
{
    std::atomic<size_t> read_rows {0};        /// Rows (source) processed.
    std::atomic<size_t> read_bytes {0};       /// Bytes (uncompressed, source) processed.
    std::atomic<size_t> read_raw_bytes {0};   /// Raw bytes processed.

    /** How much rows/bytes must be processed, in total, approximately. Non-zero value is sent when there is information about
      * some new part of job. Received values must be summed to get estimate of total rows to process.
      * `total_raw_bytes_to_process` is used for file table engine or when reading from file descriptor.
      * Used for rendering progress bar on client.
      */
    std::atomic<size_t> total_rows_to_read {0};
    std::atomic<size_t> total_raw_bytes_to_read {0};

    std::atomic<size_t> written_rows {0};
    std::atomic<size_t> written_bytes {0};

    std::atomic<size_t> written_elapsed_milliseconds {0};

    std::atomic<size_t> disk_cache_read_bytes {0};

    Progress() = default;

    Progress(size_t read_rows_, size_t read_bytes_, size_t total_rows_to_read_ = 0, size_t written_elapsed_milliseconds_ = 0, size_t disk_cache_read_bytes_ = 0)
        : read_rows(read_rows_)
        , read_bytes(read_bytes_)
        , total_rows_to_read(total_rows_to_read_)
        , written_elapsed_milliseconds(written_elapsed_milliseconds_)
        , disk_cache_read_bytes(disk_cache_read_bytes_) {}

    explicit Progress(const ProgressValues & v)
        : read_rows(v.read_rows)
        , read_bytes(v.read_bytes)
        , read_raw_bytes(v.read_raw_bytes)
        , total_rows_to_read(v.total_rows_to_read)
        , total_raw_bytes_to_read(v.total_raw_bytes_to_read)
        , written_rows(v.written_rows)
        , written_bytes(v.written_bytes)
        , disk_cache_read_bytes(v.disk_cache_read_bytes)
    {
    }

    explicit Progress(ReadProgress read_progress)
        : read_rows(read_progress.read_rows)
        , read_bytes(read_progress.read_bytes)
        , total_rows_to_read(read_progress.total_rows_to_read)
        , disk_cache_read_bytes(read_progress.disk_cache_read_bytes) {}

    explicit Progress(WriteProgress write_progress)
        : written_rows(write_progress.written_rows), written_bytes(write_progress.written_bytes)  {}

    explicit Progress(FileProgress file_progress)
        : read_raw_bytes(file_progress.read_bytes), total_raw_bytes_to_read(file_progress.total_bytes_to_read) {}

    void read(ReadBuffer & in, UInt64 server_revision);

    void write(WriteBuffer & out, UInt64 client_revision) const;

    /// Progress in JSON format (single line, without whitespaces) is used in HTTP headers.
    void writeJSON(WriteBuffer & out) const;

    Protos::Progress toProto() const;
    void fromProto(const Protos::Progress & progress);
    bool empty() const;

    /// Each value separately is changed atomically (but not whole object).
    bool incrementPiecewiseAtomically(const Progress & rhs);

    void reset();

    ProgressValues getValues() const;

    ProgressValues fetchAndResetPiecewiseAtomically();

    Progress & operator=(Progress && other);

    Progress(Progress && other)
    {
        *this = std::move(other);
    }
};

/** Callback to track the progress of the query.
  * Used in QueryPipeline and Context.
  * The function takes the number of rows in the last block, the number of bytes in the last block.
  * Note that the callback can be called from different threads.
  */
using ProgressCallback = std::function<void(const Progress & progress)>;
}
