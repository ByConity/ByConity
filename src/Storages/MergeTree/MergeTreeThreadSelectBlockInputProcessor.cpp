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

#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <Storages/MergeTree/MergeTreeThreadSelectBlockInputProcessor.h>
#include <Interpreters/Context.h>

namespace ProfileEvents
{
    extern const Event ReusedDataPartReaders;
};

namespace DB
{

MergeTreeThreadSelectBlockInputProcessor::MergeTreeThreadSelectBlockInputProcessor(
    const size_t thread_,
    const MergeTreeReadPoolPtr & pool_,
    const size_t min_marks_to_read_,
    const UInt64 max_block_size_rows_,
    size_t preferred_block_size_bytes_,
    size_t preferred_max_column_in_block_size_bytes_,
    const MergeTreeMetaBase & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    const bool use_uncompressed_cache_,
    const SelectQueryInfo & query_info_,
    ExpressionActionsSettings actions_settings,
    const MergeTreeReaderSettings & reader_settings_,
    const Names & virt_column_names_)
    :
    MergeTreeBaseSelectProcessor{
        pool_->getHeader(), storage_, storage_snapshot_, query_info_, std::move(actions_settings), max_block_size_rows_,
        preferred_block_size_bytes_, preferred_max_column_in_block_size_bytes_,
        reader_settings_, use_uncompressed_cache_, virt_column_names_},
    thread{thread_},
    pool{pool_}
{
    /// round min_marks_to_read up to nearest multiple of block_size expressed in marks
    /// If granularity is adaptive it doesn't make sense
    /// Maybe it will make sense to add settings `max_block_size_bytes`
    if (max_block_size_rows && !storage.canUseAdaptiveGranularity())
    {
        size_t fixed_index_granularity = storage.getSettings()->index_granularity;
        min_marks_to_read = (min_marks_to_read_ * fixed_index_granularity + max_block_size_rows - 1)
            / max_block_size_rows * max_block_size_rows / fixed_index_granularity;
    }
    else
        min_marks_to_read = min_marks_to_read_;

    ordered_names = getPort().getHeader().getNames();
}

/// Requests read task from MergeTreeReadPool and signals whether it got one
bool MergeTreeThreadSelectBlockInputProcessor::getNewTask()
{
    task = pool->getTask(min_marks_to_read, thread, ordered_names);

    if (!task)
    {
        /** Close the files (before destroying the object).
          * When many sources are created, but simultaneously reading only a few of them,
          * buffers don't waste memory.
          */
        reader.reset();
        pre_reader.reset();
        index_executor.reset();
        pre_index_executor.reset();
        return false;
    }

    const std::string part_name = task->data_part->isProjectionPart() ? task->data_part->getParentPart()->name : task->data_part->name;

    /// Allows pool to reduce number of threads in case of too slow reads.
    auto profile_callback = [this](ReadBufferFromFileBase::ProfileInfo info_) { pool->profileFeedback(info_); };

    if (!reader)
    {
        initializeReaders(task->mark_ranges, IMergeTreeReader::ValueSizeMap{}, profile_callback);
    }
    else if (part_name != last_readed_part_name)
    {
        auto avg_size_hint = reader->getAvgValueSizeHints();
        initializeReaders(task->mark_ranges, avg_size_hint, profile_callback);
    }
    /// in other case we can reuse readers, anyway they will be "seeked" to required mark
    else
        ProfileEvents::increment(ProfileEvents::ReusedDataPartReaders);

    last_readed_part_name = part_name;

    return true;
}


MergeTreeThreadSelectBlockInputProcessor::~MergeTreeThreadSelectBlockInputProcessor() = default;

}
