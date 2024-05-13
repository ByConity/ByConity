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

#include "Progress.h"
#include <atomic>

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Protos/plan_segment_manager.pb.h>


namespace DB
{
void ProgressValues::read(ReadBuffer & in, UInt64 server_revision)
{
    size_t new_read_rows = 0;
    size_t new_read_bytes = 0;
    size_t new_total_rows_to_read = 0;
    size_t new_disk_cache_read_bytes = 0;
    size_t new_written_rows = 0;
    size_t new_written_bytes = 0;

    readVarUInt(new_read_rows, in);
    readVarUInt(new_read_bytes, in);
    readVarUInt(new_total_rows_to_read, in);
    if (server_revision >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO)
    {
        readVarUInt(new_written_rows, in);
        readVarUInt(new_written_bytes, in);
    }

    this->read_rows = new_read_rows;
    this->read_bytes = new_read_bytes;
    this->total_rows_to_read = new_total_rows_to_read;
    this->disk_cache_read_bytes = new_disk_cache_read_bytes;
    this->written_rows = new_written_rows;
    this->written_bytes = new_written_bytes;
}


void ProgressValues::write(WriteBuffer & out, UInt64 client_revision) const
{
    writeVarUInt(this->read_rows, out);
    writeVarUInt(this->read_bytes, out);
    writeVarUInt(this->total_rows_to_read, out);
    if (client_revision >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO)
    {
        writeVarUInt(this->written_rows, out);
        writeVarUInt(this->written_bytes, out);
    }
}

void ProgressValues::writeJSON(WriteBuffer & out) const
{
    /// Numbers are written in double quotes (as strings) to avoid loss of precision
    ///  of 64-bit integers after interpretation by JavaScript.

    writeCString("{\"read_rows\":\"", out);
    writeText(this->read_rows, out);
    writeCString("\",\"read_bytes\":\"", out);
    writeText(this->read_bytes, out);
    writeCString("\",\"written_rows\":\"", out);
    writeText(this->written_rows, out);
    writeCString("\",\"written_bytes\":\"", out);
    writeText(this->written_bytes, out);
    writeCString("\",\"total_rows_to_read\":\"", out);
    writeText(this->total_rows_to_read, out);
    writeCString("\"}", out);
}

Protos::Progress ProgressValues::toProto() const
{
    Protos::Progress proto;
    proto.set_read_rows(this->read_rows);
    proto.set_read_bytes(this->read_bytes);
    proto.set_read_raw_bytes(this->read_raw_bytes);
    proto.set_written_rows(this->written_rows);
    proto.set_written_bytes(this->written_bytes);
    proto.set_total_rows_to_read(this->total_rows_to_read);
    proto.set_total_raw_bytes_to_read(this->total_raw_bytes_to_read);
    proto.set_disk_cache_read_bytes(this->disk_cache_read_bytes);
    return proto;
}

void ProgressValues::fromProto(const Protos::Progress & progress)
{
    this->read_rows = progress.read_rows();
    this->read_bytes = progress.read_bytes();
    this->read_raw_bytes = progress.read_raw_bytes();
    this->written_rows = progress.written_rows();
    this->written_bytes = progress.written_bytes();
    this->total_rows_to_read = progress.total_rows_to_read();
    this->total_raw_bytes_to_read = progress.total_raw_bytes_to_read();
    this->disk_cache_read_bytes = progress.disk_cache_read_bytes();
}

bool ProgressValues::empty() const
{
    return read_rows == 0 && read_bytes == 0 && read_raw_bytes == 0 && written_rows == 0 && written_bytes == 0 && total_rows_to_read == 0
        && disk_cache_read_bytes == 0 && total_raw_bytes_to_read == 0;
}

ProgressValues ProgressValues::operator-(const ProgressValues & other) const
{
    ProgressValues v;
    v.read_rows = read_rows >= other.read_rows ? read_rows - other.read_rows : 0;
    v.read_bytes = read_bytes >= other.read_bytes ? read_bytes - other.read_bytes : 0;
    v.read_raw_bytes = read_raw_bytes >= other.read_raw_bytes ? read_raw_bytes - other.read_raw_bytes : 0;

    v.written_rows = written_rows >= other.written_rows ? written_rows - other.written_rows : 0;
    v.written_bytes = written_bytes >= other.written_bytes ? written_bytes - other.written_bytes : 0;

    v.disk_cache_read_bytes
        = disk_cache_read_bytes >= other.disk_cache_read_bytes ? disk_cache_read_bytes - other.disk_cache_read_bytes : 0;

    v.total_rows_to_read = total_rows_to_read >= other.total_rows_to_read ? total_rows_to_read - other.total_rows_to_read : 0;
    v.total_raw_bytes_to_read
        = total_raw_bytes_to_read >= other.total_raw_bytes_to_read ? total_raw_bytes_to_read - other.total_raw_bytes_to_read : 0;

    return v;
}

ProgressValues ProgressValues::operator+(const ProgressValues & other) const
{
    ProgressValues v;
    v.read_rows = read_rows + other.read_rows;
    v.read_bytes = read_bytes + other.read_bytes;
    v.read_raw_bytes = read_raw_bytes + other.read_raw_bytes;

    v.written_rows = written_rows + other.written_rows;
    v.written_bytes = written_bytes + other.written_bytes;

    v.disk_cache_read_bytes = disk_cache_read_bytes + other.disk_cache_read_bytes;

    v.total_rows_to_read = total_rows_to_read + other.total_rows_to_read;
    v.total_raw_bytes_to_read = total_raw_bytes_to_read + other.total_raw_bytes_to_read;

    return v;
}

bool ProgressValues::operator==(const ProgressValues & other) const
{
    return read_rows == other.read_rows && read_bytes == other.read_bytes && read_raw_bytes == other.read_raw_bytes
        && written_rows == other.written_rows && written_bytes == other.written_bytes && total_rows_to_read == other.total_rows_to_read
        && disk_cache_read_bytes == other.disk_cache_read_bytes && total_raw_bytes_to_read == other.total_raw_bytes_to_read;
}

String ProgressValues::toString() const
{
    return fmt::format(
        "progress[read_rows={}, read_bytes={}, read_raw_bytes={}, written_rows={}, written_bytes={}, disk_cache_read_bytes={}, "
        "total_rows_to_read={}, "
        "total_raw_bytes_to_read={}]",
        read_rows,
        read_bytes,
        read_raw_bytes,
        written_rows,
        written_bytes,
        disk_cache_read_bytes,
        total_rows_to_read,
        total_raw_bytes_to_read);
}

bool Progress::incrementPiecewiseAtomically(const Progress & rhs)
{
    read_rows += rhs.read_rows;
    read_bytes += rhs.read_bytes;
    read_raw_bytes += rhs.read_raw_bytes;

    total_rows_to_read += rhs.total_rows_to_read;
    total_raw_bytes_to_read += rhs.total_raw_bytes_to_read;

    disk_cache_read_bytes += rhs.disk_cache_read_bytes;

    written_rows += rhs.written_rows;
    written_bytes += rhs.written_bytes;

    written_elapsed_milliseconds += rhs.written_elapsed_milliseconds;

    return rhs.read_rows || rhs.written_rows;
}

void Progress::reset()
{
    read_rows = 0;
    read_bytes = 0;
    read_raw_bytes = 0;

    total_rows_to_read = 0;
    total_raw_bytes_to_read = 0;

    disk_cache_read_bytes = 0;

    written_rows = 0;
    written_bytes = 0;

    written_elapsed_milliseconds = 0;
}

ProgressValues Progress::getValues() const
{
    ProgressValues res;

    res.read_rows = read_rows.load(std::memory_order_relaxed);
    res.read_bytes = read_bytes.load(std::memory_order_relaxed);
    res.read_raw_bytes = read_raw_bytes.load(std::memory_order_relaxed);

    res.total_rows_to_read = total_rows_to_read.load(std::memory_order_relaxed);
    res.total_raw_bytes_to_read = total_raw_bytes_to_read.load(std::memory_order_relaxed);

    res.disk_cache_read_bytes = disk_cache_read_bytes.load(std::memory_order_relaxed);

    res.written_rows = written_rows.load(std::memory_order_relaxed);
    res.written_bytes = written_bytes.load(std::memory_order_relaxed);

    return res;
}

ProgressValues Progress::fetchAndResetPiecewiseAtomically()
{
    ProgressValues res;

    res.read_rows = read_rows.fetch_and(0);
    res.read_bytes = read_bytes.fetch_and(0);
    res.read_raw_bytes = read_raw_bytes.fetch_and(0);

    res.total_rows_to_read = total_rows_to_read.fetch_and(0);
    res.total_raw_bytes_to_read = total_raw_bytes_to_read.fetch_and(0);

    res.disk_cache_read_bytes = disk_cache_read_bytes.fetch_and(0);

    res.written_rows = written_rows.fetch_and(0);
    res.written_bytes = written_bytes.fetch_and(0);

    return res;
}

Progress & Progress::operator=(Progress && other)
{
    read_rows = other.read_rows.load(std::memory_order_relaxed);
    read_bytes = other.read_bytes.load(std::memory_order_relaxed);
    read_raw_bytes = other.read_raw_bytes.load(std::memory_order_relaxed);

    total_rows_to_read = other.total_rows_to_read.load(std::memory_order_relaxed);
    total_raw_bytes_to_read = other.total_raw_bytes_to_read.load(std::memory_order_relaxed);

    disk_cache_read_bytes = other.disk_cache_read_bytes.load(std::memory_order_relaxed);

    written_rows = other.written_rows.load(std::memory_order_relaxed);
    written_bytes = other.written_bytes.load(std::memory_order_relaxed);

    written_elapsed_milliseconds = other.written_elapsed_milliseconds.load(std::memory_order_relaxed);

    return *this;
}

void Progress::read(ReadBuffer & in, UInt64 server_revision)
{
    ProgressValues values;
    values.read(in, server_revision);

    read_rows.store(values.read_rows, std::memory_order_relaxed);
    read_bytes.store(values.read_bytes, std::memory_order_relaxed);
    total_rows_to_read.store(values.total_rows_to_read, std::memory_order_relaxed);
    disk_cache_read_bytes.store(values.disk_cache_read_bytes, std::memory_order_relaxed);
    written_rows.store(values.written_rows, std::memory_order_relaxed);
    written_bytes.store(values.written_bytes, std::memory_order_relaxed);
}

void Progress::write(WriteBuffer & out, UInt64 client_revision) const
{
    getValues().write(out, client_revision);
}

void Progress::writeJSON(WriteBuffer & out) const
{
    getValues().writeJSON(out);
}

Protos::Progress Progress::toProto() const
{
    Protos::Progress proto;
    proto.set_read_rows(read_rows.load(std::memory_order_relaxed));
    proto.set_read_bytes(read_bytes.load(std::memory_order_relaxed));
    proto.set_read_raw_bytes(read_raw_bytes.load(std::memory_order_relaxed));
    proto.set_total_rows_to_read(total_rows_to_read.load(std::memory_order_relaxed));
    proto.set_total_raw_bytes_to_read(total_raw_bytes_to_read.load(std::memory_order_relaxed));
    proto.set_disk_cache_read_bytes(disk_cache_read_bytes.load(std::memory_order_relaxed));
    proto.set_written_rows(written_rows.load(std::memory_order_relaxed));
    proto.set_written_bytes(written_bytes.load(std::memory_order_relaxed));
    proto.set_written_elapsed_milliseconds(written_elapsed_milliseconds.load(std::memory_order_relaxed));
    return proto;
}

void Progress::fromProto(const Protos::Progress & progress)
{
    read_rows.store(progress.read_rows(), std::memory_order_relaxed);
    read_bytes.store(progress.read_bytes(), std::memory_order_relaxed);
    read_raw_bytes.store(progress.read_raw_bytes(), std::memory_order_relaxed);

    total_rows_to_read.store(progress.total_rows_to_read(), std::memory_order_relaxed);
    total_raw_bytes_to_read.store(progress.total_raw_bytes_to_read(), std::memory_order_relaxed);
    disk_cache_read_bytes.store(progress.disk_cache_read_bytes(), std::memory_order_relaxed);

    written_rows.store(progress.written_rows(), std::memory_order_relaxed);
    written_bytes.store(progress.written_bytes(), std::memory_order_relaxed);

    written_elapsed_milliseconds.store(progress.written_elapsed_milliseconds(), std::memory_order_relaxed);
}

bool Progress::empty() const
{
    return read_rows.load(std::memory_order_relaxed) == 0 && read_bytes.load(std::memory_order_relaxed) == 0
        && read_raw_bytes.load(std::memory_order_relaxed) == 0 && total_rows_to_read.load(std::memory_order_relaxed)
        && total_raw_bytes_to_read.load(std::memory_order_relaxed) == 0 && disk_cache_read_bytes.load(std::memory_order_relaxed)
        && written_rows.load(std::memory_order_relaxed) && written_bytes.load(std::memory_order_relaxed) == 0
        && written_elapsed_milliseconds.load(std::memory_order_relaxed);
}
}
