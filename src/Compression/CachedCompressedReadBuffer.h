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

#include <memory>
#include <time.h>
#include <IO/ReadBufferFromFileBase.h>
#include "CompressedReadBufferBase.h"
#include <IO/UncompressedCache.h>


namespace DB
{


/** A buffer for reading from a compressed file using the cache of decompressed blocks.
  * The external cache is passed as an argument to the constructor.
  * Allows you to increase performance in cases where the same blocks are often read.
  * Disadvantages:
  * - in case you need to read a lot of data in a row, but of them only a part is cached, you have to do seek-and.
  */
class CachedCompressedReadBuffer : public CompressedReadBufferBase, public ReadBuffer
{
private:
    std::function<std::unique_ptr<ReadBufferFromFileBase>()> file_in_creator;
    UncompressedCache * cache;
    std::unique_ptr<ReadBufferFromFileBase> file_in;

    const std::string path;

    /// Current position in file_in
    size_t file_pos;
    /// It represents the end of file in local storage. but in remote storage(e.g. hdfs),
    /// We merge some small file to a big data file, so it represents the end pos of small file in one big data file.
    const off_t limit_offset_in_file;

    /// The parameter marks whether to read range in the data file.
    /// In compact map data, all implicit columns are stored in the same file. So when reading one implicit column data, it will be a range, which is [offset, offset + implicit col file size]. In this case, this parameter is true.
    bool is_limit = false;

    /// A piece of data from the cache, or a piece of read data that we put into the cache.
    UncompressedCache::MappedPtr owned_cell;

    void initInput();

    bool nextImpl() override;

    void prefetch(Priority priority) override;

    /// Passed into file_in.
    ReadBufferFromFileBase::ProfileCallback profile_callback;
    clockid_t clock_type {};

    /// Check comment in CompressedReadBuffer
    /* size_t nextimpl_working_buffer_offset; */

public:
    CachedCompressedReadBuffer(
        const std::string & path,
        std::function<std::unique_ptr<ReadBufferFromFileBase>()> file_in_creator,
        UncompressedCache * cache_,
        bool allow_different_codecs_ = false,
        off_t file_offset_ = 0,
        size_t file_size_ = 0,
        bool is_limit_ = false);

    /// Seek is lazy. It doesn't move the position anywhere, just remember them and perform actual
    /// seek inside nextImpl.
    void seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block);

    template<typename T>
    size_t readZeroCopy(ZeroCopyBuffer<T> &data_refs, size_t n, bool &mod_not_zero) {
        size_t bytes_copied = 0;
        mod_not_zero = false;

        while (bytes_copied < n && !eof())
        {
            size_t bytes_to_copy = std::min(static_cast<size_t>(working_buffer.end() - pos), n - bytes_copied);
            if (bytes_to_copy % sizeof(T)) 
            {
                mod_not_zero = true;
                return bytes_copied;
            }
            data_refs.add(reinterpret_cast<const T *>(pos), bytes_to_copy / sizeof(T), owned_cell);
            pos += bytes_to_copy;
            bytes_copied += bytes_to_copy;
        }

        return bytes_copied;
    }

    void setProfileCallback(const ReadBufferFromFileBase::ProfileCallback & profile_callback_, clockid_t clock_type_ = CLOCK_MONOTONIC_COARSE)
    {
        profile_callback = profile_callback_;
        clock_type = clock_type_;
    }

    // We have to initInput() here since read_until_position will be set before prefetch() or nextImpl().
    // Otherwise, read_until_position can not be set, and after reading the necessary data, 
    // the next prefetch task with wrong offset will be submitted incorrectly.
    void setReadUntilPosition(size_t position) override
    {
        initInput();
        file_in->setReadUntilPosition(position);
    }

    void setReadUntilEnd() override
    {
        initInput();
        file_in->setReadUntilEnd();
    }

    String getPath() const
    {
        return path;
    }

    UncompressedCache::MappedPtr getOwnedCell() const { return owned_cell; }

    size_t getSizeCompressed() const { return owned_cell == nullptr ? 0 : owned_cell->compressed_size; }

    size_t compressedOffset() const { return file_pos; }

    /// Return compressed offset and uncompressed offset of current read buffer
    std::pair<size_t, size_t> position() const;

    /// Return uncompressed size of current compress block, return 0 if there isn't any
    /// yet
    size_t currentBlockUncompressedSize() const { return owned_cell == nullptr ? 0 : working_buffer.size(); }
};

}
