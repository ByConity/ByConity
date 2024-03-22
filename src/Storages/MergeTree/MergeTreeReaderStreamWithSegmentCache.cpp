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

#include <cstddef>
#include <memory>
#include <optional>
#include <Compression/CachedCompressedReadBuffer.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <DataStreams/MarkInCompressedFile.h>
#include <Disks/IDisk.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/createReadBufferFromFileBase.h>
#include <Storages/DiskCache/PartFileDiskCacheSegment.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/MergeTreeReaderStreamWithSegmentCache.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>
#include <fmt/core.h>
#include <Common/Exception.h>
#include <Storages/MergeTree/IMergeTreeReaderStream.h>

namespace DB
{
MergeTreeReaderStreamWithSegmentCache::MergeTreeReaderStreamWithSegmentCache(
    const StorageID& storage_id_, const String& part_name_,
    const String& stream_name_, DiskPtr disk_, size_t marks_count_,
    const String& data_path_, off_t data_offset_, size_t data_size_,
    const String& mark_path_, off_t mark_offset_, size_t mark_size_,
    const MarkRanges& all_mark_ranges_, const MergeTreeReaderSettings& settings_,
    MarkCache* mark_cache_, UncompressedCache* uncompressed_cache_,
    IDiskCache* segment_cache_, size_t cache_segment_size_, const PartHostInfo & part_host_,
    const MergeTreeIndexGranularityInfo* index_granularity_info_,
    const ReadBufferFromFileBase::ProfileCallback& profile_callback_,
    const ProgressCallback & internal_progress_cb_,
    clockid_t clock_type_, bool is_low_cardinality_dictionary_, String stream_extension_):
        IMergeTreeReaderStream(disk_, mark_cache_, mark_path_,
            stream_name_, marks_count_, *index_granularity_info_,
            settings_.save_marks_in_cache, mark_offset_, mark_size_,
            settings_, data_size_, is_low_cardinality_dictionary_, part_host_,
            1, segment_cache_ ? segment_cache_->getMetaCache().get() : nullptr, storage_id_.uuid, part_name_),
         stream_extension(stream_extension_)
{
    size_t max_mark_range_bytes = 0;
    size_t sum_mark_range_bytes = 0;

    for (const auto & mark_range : all_mark_ranges_)
    {
        size_t left_mark = mark_range.begin;
        size_t right_mark = mark_range.end;
        size_t left_offset = left_mark < marks_count ? marks_loader.getMark(left_mark).offset_in_compressed_file : 0;
        auto mark_range_bytes = getRightOffset(right_mark) - left_offset;

        max_mark_range_bytes = std::max(max_mark_range_bytes, mark_range_bytes);
        sum_mark_range_bytes += mark_range_bytes;
    }

    MergeTreeReaderSettings reader_settings = settings_;
    if (max_mark_range_bytes != 0)
        reader_settings.read_settings.adjustBufferSize(max_mark_range_bytes);
    reader_settings.read_settings.estimated_size = sum_mark_range_bytes;

    size_t total_segment_count = (marks_count_ + cache_segment_size_ - 1) / cache_segment_size_;
    read_buffer_holder = std::make_unique<MergedReadBufferWithSegmentCache>(
        storage_id_, part_name_, stream_name_, disk_, data_path_, data_offset_,
        data_size_, cache_segment_size_, part_host_, segment_cache_ ? segment_cache_->getDataCache().get() : nullptr, reader_settings,
        total_segment_count, marks_loader, uncompressed_cache_, profile_callback_, internal_progress_cb_,
        clock_type_, stream_extension_
    );
    data_buffer = read_buffer_holder.get();
}

void MergeTreeReaderStreamWithSegmentCache::seekToStart()
{
    read_buffer_holder->seekToStart();
}

void MergeTreeReaderStreamWithSegmentCache::seekToMark(size_t mark)
{
    read_buffer_holder->seekToMark(mark);
}
}
