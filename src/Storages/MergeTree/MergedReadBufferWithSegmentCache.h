/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <Common/Logger.h>
#include <cstddef>
#include <ctime>
#include <memory>
#include <Compression/CachedCompressedReadBuffer.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Interpreters/StorageID.h>
#include <Storages/MarkCache.h>
#include <Storages/DiskCache/IDiskCache.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeMarksLoader.h>
#include <IO/ReadBufferFromRpcStreamFile.h>
#include "Common/typeid_cast.h"
#include "Storages/MergeTree/MergeTreeSuffix.h"

namespace DB
{

using ProgressCallback = std::function<void(const Progress & progress)>;

class MergedReadBufferWithSegmentCache: public ReadBuffer
{
public:
    MergedReadBufferWithSegmentCache(const StorageID& storage_id_,
        const String& part_name_, const String& stream_name_, const DiskPtr& source_disk_,
        const String& source_file_path_, size_t source_data_offset_,
        size_t source_data_size_, size_t cache_segment_size_, const PartHostInfo & part_host_,
        IDiskCache* segment_cache_, const MergeTreeReaderSettings& settings_,
        size_t total_segment_count_, MergeTreeMarksLoader& marks_loader_,
        UncompressedCache* uncompressed_cache_ = nullptr,
        const ReadBufferFromFileBase::ProfileCallback& profile_callback_ = {},
        const ProgressCallback & internal_progress_cb_ = {},
        clockid_t clock_type_ = CLOCK_MONOTONIC_COARSE,
        String stream_extension_ = DATA_FILE_EXTENSION);

    virtual size_t readBig(char* to, size_t n) override;
    virtual bool nextImpl() override;

    void prefetch(Priority priority) override;

    void seekToStart();
    void seekToMark(size_t mark);

    void setReadUntilPosition(size_t position) override;
    void setReadUntilEnd() override;

    template<typename T>
    size_t readZeroCopy(ZeroCopyBuffer<T> &data_refs, size_t n, bool &incomplete_read) 
    {
        size_t bytes_copied = 0;
        incomplete_read = false;

        while (bytes_copied < n && !eof())
        {
            // since there are multiple segment as well as the remote read, so the active buffer would change when execute nextImpl(). We need to judge the current buffer type for each copy
            if (auto * cached_bf = typeid_cast<CachedCompressedReadBuffer *>(&activeBuffer()))
            {
                size_t bytes_to_copy = std::min(static_cast<size_t>(working_buffer.end() - pos), n - bytes_copied);
                data_refs.add(reinterpret_cast<const T *>(pos), bytes_to_copy / sizeof(T), cached_bf->getOwnedCell());
                pos += bytes_to_copy;
                bytes_copied += bytes_to_copy;
            }
            else 
            {
                incomplete_read = true;
                return bytes_copied;
            }
        }

        if (bytes_copied % sizeof(T)) 
        {
            incomplete_read = true;
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Read incompleted data while readZeroCopy, bytes_copied is {}, sizeof(T) is {}", bytes_copied, sizeof(T));
        }
        
        return bytes_copied;
    }

    bool isInternalCachedCompressedReadBuffer() { return typeid_cast<CachedCompressedReadBuffer *>(&activeBuffer()) != nullptr; }

private:
    class DualCompressedReadBuffer
    {
    public:
        DualCompressedReadBuffer(): cached_buffer(nullptr), non_cached_buffer(nullptr) {}

        inline bool initialized() const;
        inline void initialize(std::unique_ptr<CachedCompressedReadBuffer> cb,
            std::unique_ptr<CompressedReadBufferFromFile> ncb);
        inline void reset();
        inline String path() const;
        inline void setProfileCallback(const ReadBufferFromFileBase::ProfileCallback& callback,
            clockid_t clock_typ);

        inline void seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block);
        inline size_t compressedOffset();
        inline void disableChecksumming();

        inline ReadBuffer& activeBuffer();

    private:
        inline void assertInitialized() const;

        std::unique_ptr<CachedCompressedReadBuffer> cached_buffer;
        std::unique_ptr<CompressedReadBufferFromFile> non_cached_buffer;
    };

    void seekToPosition(size_t segment_idx, const MarkInCompressedFile& mark_pos);
    bool seekToMarkInSegmentCache(size_t segment_idx, const MarkInCompressedFile& mark_pos);
    void initialize();
    bool seekToMarkInRemoteSegmentCache(size_t segment_idx, const MarkInCompressedFile& mark_pos, const String & segment_key);
    void initCacheBufferIfNeeded(const DiskPtr & disk, const String & path, std::unique_ptr<ReadBufferFromRpcStreamFile> remote_cache = nullptr);
    void initSourceBufferIfNeeded();

    inline size_t toSourceDataOffset(size_t logical_offset) const;
    inline size_t fromSourceDataOffset(size_t physical_offset) const;

    ReadBuffer& activeBuffer();

    // Reader stream info
    StorageID storage_id;
    String part_name;
    String stream_name;

    // Source file info
    DiskPtr source_disk;
    const String source_file_path;
    size_t source_data_offset;
    size_t source_data_size;

    // Segment cache info
    const size_t cache_segment_size;
    IDiskCache* segment_cache;

    MergeTreeReaderSettings settings;
    UncompressedCache* uncompressed_cache;
    ReadBufferFromFileBase::ProfileCallback profile_callback;
    ProgressCallback progress_callback;
    clockid_t clock_type;

    size_t total_segment_count;

    MergeTreeMarksLoader& marks_loader;

    // current segment index is guarantee to be consistent with cache_buffer
    size_t current_segment_idx;
    // Current compressed offset of underlying data, if this object has_value,
    // then there must encounter end of a segment
    std::optional<size_t> current_compressed_offset;
    DualCompressedReadBuffer cache_buffer;
    DualCompressedReadBuffer source_buffer;

    PartHostInfo part_host;

    String stream_extension;

    LoggerPtr logger;

    off_t read_until_position = 0;
};

}
