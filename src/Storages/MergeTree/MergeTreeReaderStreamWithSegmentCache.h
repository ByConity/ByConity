#pragma once

#include <memory>
#include <Storages/MarkCache.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeMarksLoader.h>
#include <Storages/DiskCache/IDiskCache.h>
#include <bits/types/clockid_t.h>
#include "Compression/CachedCompressedReadBuffer.h"
#include "Compression/CompressedReadBufferFromFile.h"
#include "DataStreams/MarkInCompressedFile.h"
#include "IO/ReadBuffer.h"
#include "IO/ReadBufferFromFileBase.h"
#include "IO/UncompressedCache.h"
#include "Interpreters/StorageID.h"
#include "Storages/HDFS/HDFSCommon.h"
#include "Storages/MergeTree/IMergeTreeDataPart.h"
#include "Storages/MergeTree/MergeTreeIndexGranularity.h"

namespace DB
{

class MergeTreeReaderStreamReadBuffer
{
public:
    MergeTreeReaderStreamReadBuffer(): non_cached_buffer(nullptr), cached_buffer(nullptr) {}

    void seek(size_t offset_in_compressed_data, size_t offset_in_decompressed_block);
    bool initialized() const;
    void initialize(std::unique_ptr<CompressedReadBufferFromFile> ncb,
        std::unique_ptr<CachedCompressedReadBuffer> cb);
    void reset();
    ReadBuffer* readBuffer();

    void setProfileCallback(ReadBufferFromFileBase::ProfileCallback callback,
        clockid_t clock_type);
    void disableChecksumming();
    String path() const;

private:
    std::unique_ptr<CompressedReadBufferFromFile> non_cached_buffer;
    std::unique_ptr<CachedCompressedReadBuffer> cached_buffer;
};

class MergeTreeReaderStreamWithSegmentCache
{
public:
    MergeTreeReaderStreamWithSegmentCache(
        const StorageID& storage_id_, const String& part_name_,
        const String& stream_name_, DiskPtr disk_, size_t marks_count_,
        const String& data_path_, off_t data_offset_, size_t data_size_,
        const String& mark_path_, off_t mark_offset_, size_t mark_size_,
        const MarkRanges& all_mark_ranges_, const MergeTreeReaderSettings& settings_,
        MarkCache* mark_cache_, UncompressedCache* uncompressed_cache_,
        IDiskCache* segment_cache_, size_t cache_segment_size_,
        const MergeTreeIndexGranularityInfo* index_granularity_info_,
        const ReadBufferFromFileBase::ProfileCallback& profile_callback_,
        clockid_t clock_type_);

    void seekToMark(size_t mark);

    void seekToStart();

    ReadBuffer* data_buffer;

private:
    static void markRangeStatistics(const MarkRanges& all_mark_ranges,
        MergeTreeMarksLoader& marks_loader, size_t file_size,
        size_t* max_mark_range_bytes, size_t* sum_mark_range_bytes);

    void seekToPosition(size_t mark, const MarkInCompressedFile& mark_pos);
    void initSourceBufferIfNeeded();
    void initCacheBufferIfNeeded(const DiskPtr& cache_disk, const String& cache_path);

    bool seekToMarkInSegmentCache(size_t mark, const MarkInCompressedFile& mark_pos);

    const StorageID storage_id;
    const String part_name;
    const String stream_name;

    DiskPtr disk;

    String data_file_path;
    off_t data_file_offset;
    size_t data_file_size;

    UncompressedCache* uncompressed_cache;

    IDiskCache* segment_cache;
    size_t cache_segment_size;

    // Read buffer configurations
    ReadBufferFromFileBase::ProfileCallback profile_callback;
    clockid_t clock_type;
    size_t buffer_size;
    size_t estimate_range_bytes;

    MergeTreeReaderSettings settings;

    MergeTreeMarksLoader marks_loader;

    MergeTreeReaderStreamReadBuffer cache_buffer;
    MergeTreeReaderStreamReadBuffer source_buffer;
};

}
