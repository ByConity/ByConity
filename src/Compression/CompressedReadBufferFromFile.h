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

#include "CompressedReadBufferBase.h"
#include <IO/ReadBufferFromFileBase.h>
#include <time.h>
#include <memory>


namespace DB
{

class MMappedFileCache;


/// Unlike CompressedReadBuffer, it can do seek.
class CompressedReadBufferFromFile : public CompressedReadBufferBase, public BufferWithOwnMemory<ReadBuffer>
{
private:
      /** At any time, one of two things is true:
      * a) size_compressed = 0
      * b)
      *  - `working_buffer` contains the entire block.
      *  - `file_in` points to the end of this block.
      *  - `size_compressed` contains the compressed size of this block.
      */
    std::unique_ptr<ReadBufferFromFileBase> p_file_in;
    ReadBufferFromFileBase & file_in;
    size_t size_compressed = 0;

    const off_t limit_offset_in_file;
    bool is_limit = false;

    /// This field inherited from ReadBuffer. It's used to perform "lazy" seek, so in seek() call we:
    /// 1) actually seek only underlying compressed file_in to offset_in_compressed_file;
    /// 2) reset current working_buffer;
    /// 3) remember the position in decompressed block in nextimpl_working_buffer_offset.
    /// After following ReadBuffer::next() -> nextImpl call we will read new data into working_buffer and
    /// ReadBuffer::next() will move our position in the fresh working_buffer to nextimpl_working_buffer_offset and
    /// reset it to zero.
    ///
    /// NOTE: We have independent readBig implementation, so we have to take
    /// nextimpl_working_buffer_offset into account there as well.
    ///
    /* size_t nextimpl_working_buffer_offset; */

    bool nextImpl() override;

    void prefetch(Priority priority) override;

public:
    explicit CompressedReadBufferFromFile(
        std::unique_ptr<ReadBufferFromFileBase> buf,
        bool allow_different_codecs_ = false,
        off_t file_offset_ = 0,
        size_t file_size_ = 0,
        bool is_limit_ = false);

    CompressedReadBufferFromFile(
        const std::string & path,
        size_t estimated_size,
        size_t aio_threshold,
        size_t mmap_threshold,
        MMappedFileCache * mmap_cache,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        bool allow_different_codecs_ = false,
        off_t file_offset_ = 0,
        size_t file_size_ = 0,
        bool is_limit_ = false);

    /// Seek is lazy in some sense. We move position in compressed file_in to offset_in_compressed_file, but don't
    /// read data into working_buffer and don't shift our position to offset_in_decompressed_block. Instead
    /// we store this offset inside nextimpl_working_buffer_offset.
    void seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block);

    size_t readBig(char * to, size_t n) override;

    void setProfileCallback(const ReadBufferFromFileBase::ProfileCallback & profile_callback_, clockid_t clock_type_ = CLOCK_MONOTONIC_COARSE)
    {
        file_in.setProfileCallback(profile_callback_, clock_type_);
    }

    void setReadUntilPosition(size_t position) override { file_in.setReadUntilPosition(position); }

    void setReadUntilEnd() override { file_in.setReadUntilEnd(); }

    String getPath() const
    {
        return file_in.getFileName();
    }

    size_t getSizeCompressed() const { return size_compressed; }

    size_t compressedOffset() const { return file_in.getPosition(); }

    /// Return compressed offset and uncompressed offset of current read buffer
    std::pair<size_t, size_t> position() const;

    /// Return uncompressed size of current compress block, return 0 if there isn't any
    /// yet
    size_t currentBlockUncompressedSize() const { return size_compressed == 0 ? 0 : working_buffer.size(); }
};

}
