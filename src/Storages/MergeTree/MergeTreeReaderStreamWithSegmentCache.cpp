#include <memory>
#include <optional>
#include <Storages/MergeTree/MergeTreeReaderStreamWithSegmentCache.h>
#include <Storages/DiskCache/DiskCacheSegment.h>
#include <DataStreams/MarkInCompressedFile.h>
#include <fmt/core.h>
#include "Common/Exception.h"
#include "Compression/CachedCompressedReadBuffer.h"
#include "Compression/CompressedReadBuffer.h"
#include "Compression/CompressedReadBufferFromFile.h"
#include "Disks/IDisk.h"
#include "IO/ReadBufferFromFileBase.h"
#include "IO/createReadBufferFromFileBase.h"
#include "Storages/DiskCache/DiskCacheSegment.h"
#include "Storages/MergeTree/MergeTreeReaderStream.h"
#include "Storages/MergeTree/MergeTreeSuffix.h"

namespace DB
{

void MergeTreeReaderStreamReadBuffer::seek(size_t offset_in_compressed_data,
    size_t offset_in_decompressed_block)
{
    if (non_cached_buffer != nullptr)
        non_cached_buffer->seek(offset_in_compressed_data, offset_in_decompressed_block);
    else
        cached_buffer->seek(offset_in_compressed_data, offset_in_decompressed_block);

}

bool MergeTreeReaderStreamReadBuffer::initialized() const
{
    return non_cached_buffer != nullptr || cached_buffer != nullptr;
}

void MergeTreeReaderStreamReadBuffer::initialize(std::unique_ptr<CompressedReadBufferFromFile> ncb,
    std::unique_ptr<CachedCompressedReadBuffer> cb)
{
    if (initialized())
    {
        throw Exception("Trying to initialize a MergeTreeReaderStreamReadBuffer which already initialized",
            ErrorCodes::LOGICAL_ERROR);
    }

    if (!((ncb == nullptr) ^ (cb == nullptr)))
    {
        throw Exception("Only cached or non cached buffer should be specific when initialize",
            ErrorCodes::LOGICAL_ERROR);
    }

    non_cached_buffer = std::move(ncb);
    cached_buffer = std::move(cb);
}

void MergeTreeReaderStreamReadBuffer::reset()
{
    non_cached_buffer.reset();
    cached_buffer.reset();
}

ReadBuffer* MergeTreeReaderStreamReadBuffer::readBuffer()
{
    if (non_cached_buffer != nullptr)
        return non_cached_buffer.get();
    else
        return cached_buffer.get();
}

void MergeTreeReaderStreamReadBuffer::setProfileCallback(ReadBufferFromFileBase::ProfileCallback callback,
    clockid_t clock_type)
{
    if (non_cached_buffer != nullptr)
        non_cached_buffer->setProfileCallback(callback, clock_type);
    else
        cached_buffer->setProfileCallback(callback, clock_type);
}

void MergeTreeReaderStreamReadBuffer::disableChecksumming()
{
    if (non_cached_buffer != nullptr)
        non_cached_buffer->disableChecksumming();
    else
        cached_buffer->disableChecksumming();
}

String MergeTreeReaderStreamReadBuffer::path() const
{
    if (non_cached_buffer != nullptr)
        return non_cached_buffer->getPath();
    else
        return cached_buffer->getPath();
}

MergeTreeReaderStreamWithSegmentCache::MergeTreeReaderStreamWithSegmentCache(
    const StorageID& storage_id_, const String& part_name_,
    const String& stream_name_, DiskPtr disk_, size_t marks_count_,
    const String& data_path_, off_t data_offset_, size_t data_size_,
    const String& mark_path_, off_t mark_offset_, size_t mark_size_,
    const MarkRanges& all_mark_ranges_, const MergeTreeReaderSettings& settings_,
    MarkCache* mark_cache_, UncompressedCache* uncompressed_cache_,
    IDiskCache* segment_cache_, size_t cache_segment_size_,
    const MergeTreeIndexGranularityInfo* index_granularity_info_,
    const ReadBufferFromFileBase::ProfileCallback& profile_callback_,
    clockid_t clock_type_): 
        data_buffer(nullptr), storage_id(storage_id_), part_name(part_name_),
        stream_name(stream_name_), disk(disk_), data_file_path(data_path_),
        data_file_offset(data_offset_), data_file_size(data_size_),
        uncompressed_cache(uncompressed_cache_), segment_cache(segment_cache_),
        cache_segment_size(cache_segment_size_), profile_callback(profile_callback_),
        clock_type(clock_type_), estimate_range_bytes(0), settings(settings_),
        marks_loader(disk_, mark_cache_, mark_path_, stream_name_, marks_count_,
            *index_granularity_info_, settings_.save_marks_in_cache,
            mark_offset_, mark_size_)
{
    size_t max_mark_range_bytes = 0;

    markRangeStatistics(all_mark_ranges_, marks_loader, data_size_,
        &max_mark_range_bytes, &estimate_range_bytes);

    /// Avoid empty buffer. May happen while reading dictionary for DataTypeLowCardinality.
    /// For example: part has single dictionary and all marks point to the same position.
    if (max_mark_range_bytes == 0)
        max_mark_range_bytes = settings.max_read_buffer_size;

    buffer_size = std::min(settings_.max_read_buffer_size, max_mark_range_bytes);
}

void MergeTreeReaderStreamWithSegmentCache::seekToStart()
{
    return seekToPosition(0, {0, 0});
}

void MergeTreeReaderStreamWithSegmentCache::seekToMark(size_t mark)
{
    MarkInCompressedFile mark_pos = marks_loader.getMark(mark);
    return seekToPosition(mark, mark_pos);
}

void MergeTreeReaderStreamWithSegmentCache::seekToPosition(size_t mark,
    const MarkInCompressedFile& mark_pos)
{
    // Seek to data in segment cache, reading from there
    if (seekToMarkInSegmentCache(mark, mark_pos))
    {
        return;
    }

    try
    {
        initSourceBufferIfNeeded();
        // Mark file will record raw offset, since data maybe merged into larger
        // file, we need to add data file's offset here
        source_buffer.seek(data_file_offset + mark_pos.offset_in_compressed_file,
            mark_pos.offset_in_decompressed_block);
    }
    catch (Exception& e)
    {
        /// Better diagnostics.
        if (e.code() == ErrorCodes::ARGUMENT_OUT_OF_BOUND)
            e.addMessage("(while seeking to mark " + toString(mark)
                         + " of column " + data_file_path + "; offsets are: "
                         + toString(mark_pos.offset_in_compressed_file + data_file_offset) + " "
                         + toString(mark_pos.offset_in_decompressed_block) + ")");

        throw;
    }
}

void MergeTreeReaderStreamWithSegmentCache::initSourceBufferIfNeeded()
{
    if (source_buffer.initialized())
    {
        return;
    }

    if (uncompressed_cache)
    {
        auto cached_buffer = std::make_unique<CachedCompressedReadBuffer>(
            fullPath(disk, data_file_path),
            [this]() {
                return disk->readFile(data_file_path, {
                    .buffer_size = buffer_size,
                    .estimated_size = estimate_range_bytes,
                    .aio_threshold = settings.min_bytes_to_use_direct_io,
                    .mmap_threshold = settings.min_bytes_to_use_mmap_io,
                    .mmap_cache = settings.mmap_cache.get()
                });
            },
            uncompressed_cache, data_file_offset, data_file_size, true
        );

        source_buffer.initialize(nullptr, std::move(cached_buffer));
    }
    else
    {
        auto non_cached_buffer = std::make_unique<CompressedReadBufferFromFile>(
            disk->readFile(data_file_path, {
                .buffer_size = buffer_size,
                .estimated_size = estimate_range_bytes,
                .aio_threshold = settings.min_bytes_to_use_direct_io,
                .mmap_threshold = settings.min_bytes_to_use_mmap_io,
                .mmap_cache = settings.mmap_cache.get()
            }),
            false, data_file_offset, data_file_size, true
        );

        source_buffer.initialize(std::move(non_cached_buffer), nullptr);
    }

    if (profile_callback)
        source_buffer.setProfileCallback(profile_callback, clock_type);

    if (!settings.checksum_on_read)
        source_buffer.disableChecksumming();

    data_buffer = source_buffer.readBuffer();
}

bool MergeTreeReaderStreamWithSegmentCache::seekToMarkInSegmentCache(size_t mark,
    const MarkInCompressedFile& mark_pos)
{
    if (segment_cache == nullptr)
    {
        return false;
    }

    size_t segment_index = mark / cache_segment_size;
    String segment_key = DiskCacheSegment::getSegmentKey(storage_id, part_name,
        stream_name, segment_index, DATA_FILE_EXTENSION);
    std::optional<String> cache_path = segment_cache->get(segment_key);
    if (!cache_path.has_value())
    {
        return false;
    }

    try
    {
        size_t segment_start_mark = segment_index * cache_segment_size;
        MarkInCompressedFile segment_start_mark_pos = marks_loader.getMark(segment_start_mark);
        size_t offset_in_segment_buffer =
            mark_pos.offset_in_compressed_file - segment_start_mark_pos.offset_in_compressed_file;

        // No need to set limit since each file only contains one stream
        initCacheBufferIfNeeded(cache_path.value());

        cache_buffer.seek(offset_in_segment_buffer, mark_pos.offset_in_decompressed_block);
    }
    catch(...)
    {
        String extra_msg = fmt::format("Failed to seek to cache buffer, mark: {}, segment_index: {}, segment_key: {}, cache_path: {}",
            mark, segment_index, segment_key, cache_path.value());
        tryLogCurrentException("MergeTreeReaderStreamWithSegmentCache",
            extra_msg);
        return false;
    }

    return true;
}

void MergeTreeReaderStreamWithSegmentCache::initCacheBufferIfNeeded(
    const String& cache_path)
{
    if (cache_buffer.initialized() && cache_buffer.path() == cache_path)
    {
        // We already have cache read buffer for this segment, reuse it
        return;
    }

    cache_buffer.reset();

    if (uncompressed_cache)
    {
        // NOTE(wsy): Maybe replace cache path here to allow source buffer
        // and cache buffer using same cache namespace?
        // NOTE(wsy): Let segment cache return corresponding disk?
        auto cached_buffer = std::make_unique<CachedCompressedReadBuffer>(
            cache_path,
            [this, &cache_path]() {
                return createReadBufferFromFileBase(cache_path, {
                    .buffer_size = buffer_size,
                    .estimated_size = estimate_range_bytes,
                    .aio_threshold = settings.min_bytes_to_use_direct_io,
                    .mmap_threshold = settings.min_bytes_to_use_mmap_io,
                    .mmap_cache = settings.mmap_cache.get()
                });
            },
            uncompressed_cache
        );

        cache_buffer.initialize(nullptr, std::move(cached_buffer));
    }
    else
    {
        auto non_cached_buffer = std::make_unique<CompressedReadBufferFromFile>(
            cache_path, estimate_range_bytes, settings.min_bytes_to_use_direct_io,
            settings.min_bytes_to_use_mmap_io, settings.mmap_cache.get(),
            buffer_size
        );

        cache_buffer.initialize(std::move(non_cached_buffer), nullptr);
    }

    if (profile_callback)
        cache_buffer.setProfileCallback(profile_callback, clock_type);

    if (!settings.checksum_on_read)
        cache_buffer.disableChecksumming();

    data_buffer = cache_buffer.readBuffer();
}

void MergeTreeReaderStreamWithSegmentCache::markRangeStatistics(const MarkRanges& all_mark_ranges,
    MergeTreeMarksLoader& marks_loader, size_t file_size, size_t* max_mark_range_bytes,
    size_t* sum_mark_range_bytes)
{
    size_t marks_count = marks_loader.marksCount();
    for (const auto & mark_range : all_mark_ranges)
    {
        size_t left_mark = mark_range.begin;
        size_t right_mark = mark_range.end;

        /// NOTE: if we are reading the whole file, then right_mark == marks_count
        /// and we will use max_read_buffer_size for buffer size, thus avoiding the need to load marks.

        /// If the end of range is inside the block, we will need to read it too.
        if (right_mark < marks_count && marks_loader.getMark(right_mark).offset_in_decompressed_block > 0)
        {
            auto indices = collections::range(right_mark, marks_count);
            auto it = std::upper_bound(indices.begin(), indices.end(), right_mark, [&marks_loader](size_t i, size_t j)
            {
                return marks_loader.getMark(i).offset_in_compressed_file < marks_loader.getMark(j).offset_in_compressed_file;
            });

            right_mark = (it == indices.end() ? marks_count : *it);
        }

        size_t mark_range_bytes;

        /// If there are no marks after the end of range, just use file size
        if (right_mark >= marks_count
            || (right_mark + 1 == marks_count
                && marks_loader.getMark(right_mark).offset_in_compressed_file == marks_loader.getMark(mark_range.end).offset_in_compressed_file))
        {
            mark_range_bytes = file_size - (left_mark < marks_count ? marks_loader.getMark(left_mark).offset_in_compressed_file : 0);
        }
        else
        {
            mark_range_bytes = marks_loader.getMark(right_mark).offset_in_compressed_file - marks_loader.getMark(left_mark).offset_in_compressed_file;
        }

        *max_mark_range_bytes = std::max(*max_mark_range_bytes, mark_range_bytes);
        *sum_mark_range_bytes += mark_range_bytes;
    }
}

}
