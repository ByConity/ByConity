#pragma once
#include <Storages/MarkCache.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <Compression/CachedCompressedReadBuffer.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeMarksLoader.h>


namespace DB
{

/// Class for reading a single column (or index).
class MergeTreeReaderStream
{
public:
    MergeTreeReaderStream(
        DiskPtr disk_,
        const String & path_prefix_, const String & stream_name_, const String & data_file_extension_, 
        size_t marks_count_,
        const MarkRanges & all_mark_ranges,
        const MergeTreeReaderSettings & settings_,
        MarkCache * mark_cache, UncompressedCache * uncompressed_cache,
        const MergeTreeIndexGranularityInfo * index_granularity_info_,
        const ReadBufferFromFileBase::ProfileCallback & profile_callback, clockid_t clock_type,
        off_t data_file_offset_, size_t data_file_size_,
        off_t mark_file_offset_, size_t mark_file_size_);

    void seekToMark(size_t index);

    void seekToStart();

    size_t getCurrentBlockCompressedSize() const;

private:
    DiskPtr disk;
    std::string path_prefix;
    std::string stream_name;
    std::string data_file_extension;

    size_t marks_count;

    MarkCache * mark_cache;
    bool save_marks_in_cache;

    off_t data_file_offset;

    const MergeTreeIndexGranularityInfo * index_granularity_info;

    std::unique_ptr<CachedCompressedReadBuffer> cached_buffer;
    std::unique_ptr<CompressedReadBufferFromFile> non_cached_buffer;

public:
    ReadBuffer * data_buffer;

    MergeTreeMarksLoader marks_loader;
};
}
