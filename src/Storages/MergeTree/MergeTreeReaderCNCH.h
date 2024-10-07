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
#include <unordered_map>
#include <Core/NamesAndTypes.h>
#include <Storages/DiskCache/IDiskCache.h>
#include <Storages/DiskCache/IDiskCacheStrategy.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <bits/types/clockid_t.h>
#include "IO/ReadBufferFromFileBase.h"
#include "Storages/MergeTree/IMergeTreeReaderStream.h"

namespace DB
{

class MergeTreeDataPartCNCH;
using DataPartCNCHPtr = std::shared_ptr<const MergeTreeDataPartCNCH>;

/// Reader for Wide parts.
class MergeTreeReaderCNCH : public IMergeTreeReader
{
public:
    MergeTreeReaderCNCH(
        const DataPartCNCHPtr & data_part_,
        const NamesAndTypesList & columns_,
        const StorageMetadataPtr & metadata_snapshot_,
        UncompressedCache * uncompressed_cache_,
        MarkCache * mark_cache_,
        const MarkRanges & mark_ranges_,
        const MergeTreeReaderSettings & settings_,
        MergeTreeIndexExecutor* index_executor_,
        const ValueSizeMap & avg_value_size_hints_ = {},
        const ReadBufferFromFileBase::ProfileCallback & profile_callback_ = {},
        const ProgressCallback & internal_progress_cb_ = {},
        clockid_t clock_type_ = CLOCK_MONOTONIC_COARSE);

    size_t readRows(size_t from_mark, size_t from_row, size_t max_rows_to_read,
        size_t current_task_last_mark, const UInt8* filter, Columns& res_columns) override;

    bool canReadIncompleteGranules() const override { return true; }

private:
    using MergeTreeReaderStreamUniquePtr = std::unique_ptr<IMergeTreeReaderStream>;
    using FileStreams = std::map<std::string, MergeTreeReaderStreamUniquePtr>;
    using FileStreamBuilders = std::map<std::string, std::function<MergeTreeReaderStreamUniquePtr()>>;

    void initializeStreams(const ReadBufferFromFileBase::ProfileCallback& profile_callback,
        const ProgressCallback & internal_progress_cb,
        clockid_t clock_type);
    void initializeStreamForColumnIfNoBurden(const NameAndTypePair& column,
        const ReadBufferFromFileBase::ProfileCallback& profile_callback,
        const ProgressCallback & internal_progress_cb,
        clockid_t clock_type, FileStreamBuilders* stream_builders);
    void executeFileStreamBuilders(FileStreamBuilders& stream_builders);
    void addStreamsIfNoBurden(const NameAndTypePair& name_and_type,
        const ReadBufferFromFileBase::ProfileCallback& profile_callback,
        const ProgressCallback & internal_progress_cb,
        clockid_t clock_type, FileStreamBuilders* stream_builders);

    size_t readBatch(const NamesAndTypesList& sort_columns, size_t num_columns,
        size_t from_mark, bool continue_reading, size_t rows_to_read,
        size_t current_task_last_mark, std::unordered_map<String, size_t>& res_col_to_idx,
        const UInt8* filter, Columns& res_columns);

    size_t readIndexColumns(size_t from_mark, bool continue_reading, size_t max_rows,
        Columns& res_bitmap_columns);

    IDiskCacheStrategyPtr segment_cache_strategy;
    IDiskCachePtr segment_cache;

    LoggerPtr log;
    String reader_id;
};

}
