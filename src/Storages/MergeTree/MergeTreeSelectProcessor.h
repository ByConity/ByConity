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
#include <Common/Logger.h>
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
class MergeTreeSelectProcessor : public MergeTreeBaseSelectProcessor
{
public:

    MergeTreeSelectProcessor(
        const MergeTreeMetaBase & storage,
        const StorageSnapshotPtr & storage_snapshot_,
        const RangesInDataPart & part_detail_,
        MergeTreeMetaBase::DeleteBitmapGetter delete_bitmap_getter_,
        Names required_columns_,
        const SelectQueryInfo & query_info_,
        bool check_columns_,
        const MergeTreeStreamSettings & stream_settings_,
        const Names & virt_column_names_ = {},
        const MarkRangesFilterCallback& range_filter_callback_ = {});

    ~MergeTreeSelectProcessor() override;

    String getName() const override { return "MergeTree"; }

    /// Closes readers and unlock part locks
    void finish();

protected:

    bool getNewTask() override;
    /// Do extra mark range filter, row filter generation and delete bitmap
    /// initialize
    void firstTaskInitialization();

    /// Used by Task
    Names required_columns;
    /// Names from header. Used in order to order columns in read blocks.
    Names ordered_names;
    NameSet column_name_set;

    MergeTreeReadTaskColumns task_columns;

    /// Data part will not be removed if the pointer owns it
    RangesInDataPart part_detail;
    MergeTreeMetaBase::DeleteBitmapGetter delete_bitmap_getter;
    /// Lazy init, need to use getDeleteBitmap() interface rather than use delete_bitmap directly
    DeleteBitmapPtr delete_bitmap;

    MarkRangesFilterCallback mark_ranges_filter_callback;

    /// Total number of marks we should read
    size_t total_marks_count = 0;

    bool check_columns;
    bool is_first_task = true;

    LoggerPtr log = getLogger("MergeTreeSelectProcessor");
};

}
