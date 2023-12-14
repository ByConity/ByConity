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

#include <Core/Types.h>
#include <IO/ReadBuffer.h>
#include <Disks/IDisk.h>
#include <Storages/MergeTree/MergeTreeMarksLoader.h>

namespace DB
{

class IMergeTreeReaderStream
{
public:
    struct StreamFileMeta
    {
        DiskPtr disk;
        String rel_path;
        off_t offset;
        size_t size;
    };

    IMergeTreeReaderStream(
            DiskPtr disk_,
            MarkCache * mark_cache_,
            const String & mrk_path,
            const String & stream_name_,
            size_t marks_count_,
            const MergeTreeIndexGranularityInfo & index_granularity_info_,
            bool save_marks_in_cache_,
            off_t mark_file_offset_,
            size_t mark_file_size_,
            const MergeTreeReaderSettings & settings_,
            size_t file_size_,
            bool is_low_cardinality_dictionary_,
            const PartHostInfo & part_host_ = {},
            size_t columns_in_mark_ = 1,
            IDiskCache * disk_cache_ = nullptr,
            UUID storage_uuid_ = {},
            const String & part_name_ = {}):
        is_low_cardinality_dictionary(is_low_cardinality_dictionary_),
        file_size(file_size_),
        marks_count(marks_count_),
        marks_loader(disk_, mark_cache_, mrk_path, stream_name_, marks_count_, index_granularity_info_,
            save_marks_in_cache_, mark_file_offset_, mark_file_size_, settings_, columns_in_mark_, disk_cache_,
            part_host_, storage_uuid_, part_name_) {}

    virtual ~IMergeTreeReaderStream() = default;

    virtual void seekToMark(size_t index) = 0;
    virtual void seekToStart() = 0;

    /**
     * Does buffer need to know something about mark ranges bounds it is going to read?
     * (In case of MergeTree* tables). Mostly needed for reading from remote fs.
     */
    virtual void adjustRightMark(size_t right_mark);

    virtual size_t getRightOffset(size_t right_mark);

    virtual ReadBuffer * getDataBuffer();

    bool is_low_cardinality_dictionary = false;

    ReadBuffer * data_buffer;

    std::optional<size_t> last_right_offset;

    size_t file_size;
    size_t marks_count;
    MergeTreeMarksLoader marks_loader;
};

}
