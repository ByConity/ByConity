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

#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Compression/CachedCompressedReadBuffer.h>
#include <Storages/MergeTree/IMergeTreeReaderStream.h>

#include <utility>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

MergeTreeReaderStream::MergeTreeReaderStream(
    const StreamFileMeta& bin_file_,
    const StreamFileMeta& mrk_file_,
    const String& stream_name_,
    size_t marks_count_,
    const MarkRanges & all_mark_ranges,
    const MergeTreeReaderSettings & settings_,
    MarkCache * mark_cache_, UncompressedCache * uncompressed_cache_,
    const MergeTreeIndexGranularityInfo * index_granularity_info_,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback_,
    clockid_t clock_type_, bool is_low_cardinality_dictionary_):
        IMergeTreeReaderStream(mrk_file_.disk, mark_cache_, mrk_file_.rel_path, stream_name_,
            marks_count_, *index_granularity_info_, settings_.save_marks_in_cache, mrk_file_.offset,
            mrk_file_.size, settings_, bin_file_.size, is_low_cardinality_dictionary_),
        data_disk(bin_file_.disk), data_rel_path(bin_file_.rel_path),
        stream_name(stream_name_),
        read_settings(settings_.read_settings), data_file_offset(bin_file_.offset)
{
    /// Compute the size of the buffer.
    size_t max_mark_range_bytes = 0;
    size_t sum_mark_range_bytes = 0;

    for (const auto & mark_range : all_mark_ranges)
    {
        size_t left_mark = mark_range.begin;
        size_t right_mark = mark_range.end;
        size_t left_offset = left_mark < marks_count ? marks_loader.getMark(left_mark).offset_in_compressed_file : 0;
        auto mark_range_bytes = getRightOffset(right_mark) - left_offset;

        max_mark_range_bytes = std::max(max_mark_range_bytes, mark_range_bytes);
        sum_mark_range_bytes += mark_range_bytes;
    }

    /// Avoid empty buffer. May happen while reading dictionary for DataTypeLowCardinality.
    /// For example: part has single dictionary and all marks point to the same position.
    if (max_mark_range_bytes != 0)
        read_settings.adjustBufferSize(max_mark_range_bytes);
    read_settings.estimated_size = sum_mark_range_bytes;

    /// Initialize the objects that shall be used to perform read operations.
    if (uncompressed_cache_)
    {
        auto buffer = std::make_unique<CachedCompressedReadBuffer>(
            fullPath(data_disk, data_rel_path),
            [this]()
            {
                return data_disk->readFile(data_rel_path, read_settings);
            },
            uncompressed_cache_,
            /* allow_different_codecs = */false,
            bin_file_.offset,
            bin_file_.size,
            /* is_limit = */true);

        if (profile_callback_)
            buffer->setProfileCallback(profile_callback_, clock_type_);

        if (!settings_.checksum_on_read)
            buffer->disableChecksumming();

        cached_buffer = std::move(buffer);
        data_buffer = cached_buffer.get();
    }
    else
    {
        auto buffer = std::make_unique<CompressedReadBufferFromFile>(
            data_disk->readFile(data_rel_path, read_settings),
            /* allow_different_codecs = */false,
            bin_file_.offset,
            bin_file_.size,
            /* is_limit = */true);

        if (profile_callback_)
            buffer->setProfileCallback(profile_callback_, clock_type_);

        if (!settings_.checksum_on_read)
            buffer->disableChecksumming();

        non_cached_buffer = std::move(buffer);
        data_buffer = non_cached_buffer.get();
    }
}

void MergeTreeReaderStream::seekToMark(size_t index)
{
    MarkInCompressedFile mark = marks_loader.getMark(index);

    try
    {
        if (cached_buffer)
            cached_buffer->seek(mark.offset_in_compressed_file + data_file_offset, mark.offset_in_decompressed_block);
        if (non_cached_buffer)
            non_cached_buffer->seek(mark.offset_in_compressed_file + data_file_offset, mark.offset_in_decompressed_block);
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        if (e.code() == ErrorCodes::ARGUMENT_OUT_OF_BOUND)
            e.addMessage("(while seeking to mark " + toString(index)
                         + " of column " + stream_name + "; offsets are: "
                         + toString(mark.offset_in_compressed_file + data_file_offset) + " "
                         + toString(mark.offset_in_decompressed_block) + ")");

        throw;
    }
}


void MergeTreeReaderStream::seekToStart()
{
    try
    {
        if (cached_buffer)
            cached_buffer->seek(data_file_offset, 0);
        if (non_cached_buffer)
            non_cached_buffer->seek(data_file_offset, 0);
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        if (e.code() == ErrorCodes::ARGUMENT_OUT_OF_BOUND)
            e.addMessage("(while seeking to start of column " + stream_name + ")");

        throw;
    }
}

}
