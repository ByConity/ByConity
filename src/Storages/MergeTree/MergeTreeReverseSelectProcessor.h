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
#include <DataStreams/IBlockInputStream.h>
#include <Storages/MergeTree/MergeTreeThreadSelectBlockInputProcessor.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{


/// Used to read data from single part with select query
/// Cares about PREWHERE, virtual columns, indexes etc.
/// To read data from multiple parts, Storage (MergeTree) creates multiple such objects.
class MergeTreeReverseSelectProcessor : public MergeTreeBaseSelectProcessor
{
public:
    MergeTreeReverseSelectProcessor(
        const MergeTreeMetaBase & storage,
        const StorageSnapshotPtr storage_snapshot_,
        const MergeTreeMetaBase::DataPartPtr & owned_data_part,
        ImmutableDeleteBitmapPtr delete_bitmap,
        UInt64 max_block_size_rows,
        size_t preferred_block_size_bytes,
        size_t preferred_max_column_in_block_size_bytes,
        Names required_columns_,
        MarkRanges mark_ranges,
        bool use_uncompressed_cache,
        const SelectQueryInfo & query_info_,
        ExpressionActionsSettings actions_settings,
        bool check_columns,
        const MergeTreeReaderSettings & reader_settings,
        const Names & virt_column_names = {},
        size_t part_index_in_query = 0,
        bool quiet = false);

    ~MergeTreeReverseSelectProcessor() override;

    String getName() const override { return "MergeTreeReverse"; }

    /// Closes readers and unlock part locks
    void finish();

protected:

    bool getNewTask() override;
    Chunk readFromPart() override;

private:
    Block header;

    /// Used by Task
    Names required_columns;
    /// Names from header. Used in order to order columns in read blocks.
    Names ordered_names;
    NameSet column_name_set;

    MergeTreeReadTaskColumns task_columns;

    /// Data part will not be removed if the pointer owns it
    MergeTreeMetaBase::DataPartPtr data_part;
    ImmutableDeleteBitmapPtr delete_bitmap;

    /// Mark ranges we should read (in ascending order)
    MarkRanges all_mark_ranges;
    /// Total number of marks we should read
    size_t total_marks_count = 0;
    /// Value of _part_index virtual column (used only in SelectExecutor)
    size_t part_index_in_query = 0;

    String path;

    Chunks chunks;

    bool check_columns;

    Poco::Logger * log = &Poco::Logger::get("MergeTreeReverseSelectProcessor");
};

}
