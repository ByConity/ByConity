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
#include <Storages/MarkCache.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <Storages/MergeTree/IMergeTreeReaderStream.h>
#include <Compression/CachedCompressedReadBuffer.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeMarksLoader.h>
#include "Storages/MergeTree/IMergeTreeReaderStream.h"


namespace DB
{

/// Class for reading a single column (or index).
class MergeTreeReaderStream: public IMergeTreeReaderStream
{
public:
    MergeTreeReaderStream(
        const StreamFileMeta& bin_file_,
        const StreamFileMeta& mrk_file_,
        const String& stream_name_,
        size_t marks_count_,
        const MarkRanges & all_mark_ranges_,
        const MergeTreeReaderSettings & settings_,
        MarkCache * mark_cache_, UncompressedCache * uncompressed_cache_,
        const MergeTreeIndexGranularityInfo * index_granularity_info_,
        const ReadBufferFromFileBase::ProfileCallback & profile_callback_,
        clockid_t clock_type_);

    virtual void seekToStart() override;
    virtual void seekToMark(size_t index) override;

private:
    DiskPtr data_disk;
    std::string data_rel_path;
    std::string stream_name;

    size_t marks_count;

    MarkCache * mark_cache;
    bool save_marks_in_cache;

    off_t data_file_offset;

    const MergeTreeIndexGranularityInfo * index_granularity_info;

    std::unique_ptr<CachedCompressedReadBuffer> cached_buffer;
    std::unique_ptr<CompressedReadBufferFromFile> non_cached_buffer;

    MergeTreeMarksLoader marks_loader;
};
}
