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

#include <Storages/MergeTree/MergeTreeReverseSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeBaseSelectProcessor.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int INVALID_BITMAP_INDEX_READER;
}

bool MergeTreeReverseSelectProcessor::getNewTask()
try
{
    if ((chunks.empty() && part_detail.ranges.empty()) || total_marks_count == 0)
    {
        finish();
        return false;
    }
    if (is_first_task)
    {
        firstTaskInitialization();
    }
    is_first_task = false;

    /// We have some blocks to return in buffer.
    /// Return true to continue reading, but actually don't create a task.
    if (part_detail.ranges.empty())
        return true;

    task_columns = getReadTaskColumns(storage, storage_snapshot, part_detail.data_part, required_columns, prewhere_info, index_context, {}, check_columns);

    /// will be used to distinguish between PREWHERE and WHERE columns when applying filter
    const auto & column_names = task_columns.columns.getNames();
    column_name_set = NameSet{column_names.begin(), column_names.end()};

    /// Read ranges from right to left.
    MarkRanges mark_ranges_for_task = { part_detail.ranges.back() };
    part_detail.ranges.pop_back();

    auto size_predictor = (stream_settings.preferred_block_size_bytes == 0)
        ? nullptr
        : std::make_unique<MergeTreeBlockSizePredictor>(part_detail.data_part, ordered_names, storage_snapshot->metadata->getSampleBlock());

    task = std::make_unique<MergeTreeReadTask>(
        part_detail.data_part, delete_bitmap, mark_ranges_for_task, part_detail.part_index_in_query, ordered_names, column_name_set,
        task_columns, prewhere_info && prewhere_info->remove_prewhere_column,
        task_columns.should_reorder, std::move(size_predictor), part_detail.ranges);

    if (!reader)
        initializeReaders(mark_ranges_for_task);

    return true;
}
catch (...)
{
    /// Suspicion of the broken part. A part is added to the queue for verification.
    if (getCurrentExceptionCode() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
        storage.reportBrokenPart(part_detail.data_part->name);
    throw;
}

Chunk MergeTreeReverseSelectProcessor::readFromPart()
{
    Chunk res;

    if (!chunks.empty())
    {
        res = std::move(chunks.back());
        chunks.pop_back();
        return res;
    }

    if (!task->range_reader.isInitialized())
        initializeRangeReaders(*task);

    while (!task->isFinished())
    {
        Chunk chunk = readFromPartImpl();
        chunks.push_back(std::move(chunk));
    }

    if (chunks.empty())
        return {};

    res = std::move(chunks.back());
    chunks.pop_back();

    return res;
}

void MergeTreeReverseSelectProcessor::finish()
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

MergeTreeReverseSelectProcessor::~MergeTreeReverseSelectProcessor() = default;

}
