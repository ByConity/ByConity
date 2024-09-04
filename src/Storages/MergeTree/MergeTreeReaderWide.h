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

#include <unordered_map>
#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/IMergeTreeReader.h>


namespace DB
{

/// Reader for Wide parts.
class MergeTreeReaderWide : public IMergeTreeReader
{
public:
    MergeTreeReaderWide(
        MergeTreeMetaBase::DataPartPtr data_part_,
        NamesAndTypesList columns_,
        const StorageMetadataPtr & metadata_snapshot_,
        UncompressedCache * uncompressed_cache_,
        MarkCache * mark_cache_,
        MarkRanges mark_ranges_,
        MergeTreeReaderSettings settings_,
        MergeTreeIndexExecutor * index_executor_,
        ValueSizeMap avg_value_size_hints_ = {},
        const ReadBufferFromFileBase::ProfileCallback & profile_callback_ = {},
        clockid_t clock_type_ = CLOCK_MONOTONIC_COARSE,
        bool create_streams_ = true);

    size_t readRows(size_t from_mark, size_t from_row, size_t max_rows_to_read,
        size_t current_task_last_mark, const UInt8* filter, Columns & res_columns) override;

    bool canReadIncompleteGranules() const override { return true; }

private:

    void addStreams(const NameAndTypePair & name_and_type,
        const ReadBufferFromFileBase::ProfileCallback & profile_callback, clockid_t clock_type);

    size_t readBatch(const NamesAndTypesList& sort_columns, size_t num_columns,
        size_t from_mark, bool continue_reading, size_t rows_to_read,
        size_t current_task_last_mark, std::unordered_map<String, size_t>& res_col_to_idx,
        const UInt8* filter, Columns& res_columns);
};

}
