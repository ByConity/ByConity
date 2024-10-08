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

#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeBaseSelectProcessor.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/FilterWithRowUtils.h>
#include <roaring.hh>

namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

MergeTreeSelectProcessor::MergeTreeSelectProcessor(
    const MergeTreeMetaBase & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    const RangesInDataPart & part_detail_,
    MergeTreeMetaBase::DeleteBitmapGetter delete_bitmap_getter_,
    Names required_columns_,
    const SelectQueryInfo & query_info_,
    bool check_columns_,
    const MergeTreeStreamSettings & stream_settings_,
    const Names & virt_column_names_,
    const MarkRangesFilterCallback& range_filter_callback_)
    :
    MergeTreeBaseSelectProcessor{
        storage_snapshot_->getSampleBlockForColumns(required_columns_),
        storage_, storage_snapshot_, query_info_, stream_settings_, virt_column_names_},
    required_columns{std::move(required_columns_)},
    part_detail{part_detail_},
    delete_bitmap_getter(std::move(delete_bitmap_getter_)),
    mark_ranges_filter_callback(range_filter_callback_),
    check_columns(check_columns_)
{
    /// Let's estimate total number of rows for progress bar.
    total_marks_count = part_detail.getMarksCount();

    size_t total_rows = part_detail.getRowsCount();
    LOG_DEBUG(log, "Reading {} ranges from part {}, approx. {} rows starting from {}",
        part_detail.ranges.size(), part_detail.data_part->name, total_rows,
        part_detail.data_part->index_granularity.getMarkStartingRow(part_detail.ranges.front().begin));

    addTotalRowsApprox(total_rows);
    ordered_names = header_without_virtual_columns.getNames();
}


bool MergeTreeSelectProcessor::getNewTask()
try
{
    /// Produce no more than one task
    if (!is_first_task || total_marks_count == 0)
    {
        finish();
        return false;
    }
    if (is_first_task)
    {
        firstTaskInitialization();
    }
    is_first_task = false;

    task_columns = getReadTaskColumns(
        storage, storage_snapshot, part_detail.data_part,
        required_columns, prewhere_info, index_context, {}, check_columns);

    auto size_predictor = (stream_settings.preferred_block_size_bytes == 0)
        ? nullptr
        : std::make_unique<MergeTreeBlockSizePredictor>(part_detail.data_part, ordered_names, storage_snapshot->metadata->getSampleBlock());

    /// will be used to distinguish between PREWHERE and WHERE columns when applying filter
    const auto & column_names = task_columns.columns.getNames();
    column_name_set = NameSet{column_names.begin(), column_names.end()};

    task = std::make_unique<MergeTreeReadTask>(
        part_detail.data_part, delete_bitmap, part_detail.ranges, part_detail.part_index_in_query, ordered_names, column_name_set, task_columns,
        prewhere_info && prewhere_info->remove_prewhere_column, task_columns.should_reorder, std::move(size_predictor), part_detail.ranges);

    if (!reader)
        initializeReaders(part_detail.ranges);

    return true;
}
catch (...)
{
    /// Suspicion of the broken part. A part is added to the queue for verification.
    if (getCurrentExceptionCode() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
        storage.reportBrokenPart(part_detail.data_part->name);
    throw;
}

void MergeTreeSelectProcessor::firstTaskInitialization()
{
    std::unique_ptr<roaring::Roaring> row_filter = nullptr;
    if (mark_ranges_filter_callback)
    {
        if (stream_settings.reader_settings.read_settings.filtered_ratio_to_use_skip_read > 0)
        {
            row_filter = std::make_unique<roaring::Roaring>();
        }
        part_detail.ranges = mark_ranges_filter_callback(part_detail.data_part,
            part_detail.ranges, row_filter.get());
    }
    delete_bitmap = combineFilterBitmap(part_detail, delete_bitmap_getter);
    if (row_filter != nullptr)
    {
        flipFilterWithMarkRanges(part_detail.ranges,
            part_detail.data_part->index_granularity, *row_filter);
        if (delete_bitmap == nullptr)
        {
            delete_bitmap = std::shared_ptr<roaring::Roaring>(row_filter.release());
        }
        else
        {
            *delete_bitmap &= *row_filter;
        }
    }
}

void MergeTreeSelectProcessor::finish()
{
    /** Close the files (before destroying the object).
    * When many sources are created, but simultaneously reading only a few of them,
    * buffers don't waste memory.
    */
    reader.reset();
    pre_reader.reset();
    part_detail.data_part.reset();
    index_executor.reset();
    pre_index_executor.reset();
}


MergeTreeSelectProcessor::~MergeTreeSelectProcessor() = default;


}
