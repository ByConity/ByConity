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

#include <Storages/MergeTree/MergeTreeBaseSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/Index/MergeTreeBitmapIndexReader.h>
#include <QueryPlan/ReadFromMergeTree.h>
#include <Columns/FilterDescription.h>
#include <Common/typeid_cast.h>
#include <Common/escapeForFileName.h>
#include <Storages/SelectQueryInfo.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/MapHelpers.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <fmt/core.h>
#include <common/logger_useful.h>
#include <Processors/IntermediateResult/OwnerInfo.h>

namespace ProfileEvents
{
extern const Event PrewhereSelectedRows;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
    extern const int LOGICAL_ERROR;
    extern const int INVALID_BITMAP_INDEX_READER;
}


MergeTreeBaseSelectProcessor::MergeTreeBaseSelectProcessor(
    Block header,
    const MergeTreeMetaBase & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    const SelectQueryInfo & query_info_,
    const MergeTreeStreamSettings & stream_settings_,
    const Names & virt_column_names_)
    : SourceWithProgress(transformHeader(std::move(header), getPrewhereInfo(query_info_), storage_.getPartitionValueType(), virt_column_names_, getIndexContext(query_info_), query_info_.read_bitmap_index))
    , storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , prewhere_info(getPrewhereInfo(query_info_))
    , index_context(getIndexContext(query_info_))
    , stream_settings(stream_settings_)
    , virt_column_names(virt_column_names_)
    , partition_value_type(storage.getPartitionValueType())
    , support_intermedicate_result_cache(storage.supportIntermedicateResultCache())

{
    header_without_virtual_columns = getPort().getHeader();

    /// Reverse order is to minimize reallocations when removing columns from the block
    for (auto it = virt_column_names.rbegin(); it != virt_column_names.rend(); ++it)
    {
        if (*it == "_part_offset")
        {
            non_const_virtual_column_names.emplace_back(*it);
        }
        else
        {
            /// Remove virtual columns that are going to be filled with const values
            if (header_without_virtual_columns.has(*it))
                header_without_virtual_columns.erase(*it);
        }
    }

    if (prewhere_info)
    {
        prewhere_actions = std::make_unique<PrewhereExprInfo>();
        if (prewhere_info->alias_actions)
            prewhere_actions->alias_actions = std::make_shared<ExpressionActions>(prewhere_info->alias_actions, stream_settings.actions_settings);

        if (prewhere_info->row_level_filter)
            prewhere_actions->row_level_filter = std::make_shared<ExpressionActions>(prewhere_info->row_level_filter, stream_settings.actions_settings);

        prewhere_actions->prewhere_actions = std::make_shared<ExpressionActions>(prewhere_info->prewhere_actions, stream_settings.actions_settings);

        prewhere_actions->row_level_column_name = prewhere_info->row_level_column_name;
        prewhere_actions->prewhere_column_name = prewhere_info->prewhere_column_name;
        prewhere_actions->remove_prewhere_column = prewhere_info->remove_prewhere_column;
        prewhere_actions->need_filter = prewhere_info->need_filter;

        LOG_TRACE(
            getLogger("MergeTreeBaseSelectProcessor"),
            "Prewhere column = {}, actions = {} ",
            prewhere_info->prewhere_column_name,
            prewhere_info->prewhere_actions->dumpDAG());
    }
    if (index_context)
    {
        auto * bitmap_index_info = dynamic_cast<BitmapIndexInfo *>(index_context->get(MergeTreeIndexInfo::Type::BITMAP).get());
        if (bitmap_index_info)
        {
            const auto & name_set = bitmap_index_info->index_column_name_set;
            bitmap_index_columns_superset.insert(name_set.begin(), name_set.end());
        }
    }
}


Chunk MergeTreeBaseSelectProcessor::generate()
{
    while (!isCancelled())
    {
        if ((!task || task->isFinished()) && !getNewTask())
            return {};

        auto res = readFromPart();

        if (res.hasRows())
        {
            injectVirtualColumns(res, task.get(), partition_value_type, virt_column_names);
            if (support_intermedicate_result_cache)
            {
                UInt64 modification_time = task->data_part->getModificationTime();
                /// In some cases, like mutation commands, part will not load delete bitmap for unique table, just skip result cache
                if (modification_time == IMergeTreeDataPart::NOT_INITIALIZED_COMMIT_TIME)
                    return res;
                res.setOwnerInfo({task->data_part->name, static_cast<time_t>(modification_time)});
            }

            return res;
        }
    }

    return {};
}


void MergeTreeBaseSelectProcessor::initializeRangeReaders(MergeTreeReadTask & current_task)
{
    size_t filtered_ratio_to_use_skip_read = stream_settings.reader_settings.read_settings.filtered_ratio_to_use_skip_read;
    if (prewhere_info)
    {
        if (reader->getColumns().empty() && !reader->hasBitmapIndexReader())
        {
            current_task.range_reader = MergeTreeRangeReader(pre_reader.get(), nullptr, prewhere_actions.get(), task->delete_bitmap, true, non_const_virtual_column_names, filtered_ratio_to_use_skip_read);
        }
        else
        {
            MergeTreeRangeReader * pre_reader_ptr = nullptr;
            if (pre_reader != nullptr)
            {
                current_task.pre_range_reader = MergeTreeRangeReader(pre_reader.get(), nullptr, prewhere_actions.get(), task->delete_bitmap, false, non_const_virtual_column_names, filtered_ratio_to_use_skip_read);
                pre_reader_ptr = &current_task.pre_range_reader;
            }

            current_task.range_reader = MergeTreeRangeReader(reader.get(), pre_reader_ptr, nullptr, task->delete_bitmap, true, non_const_virtual_column_names, filtered_ratio_to_use_skip_read);
        }
    }
    else
    {
        current_task.range_reader = MergeTreeRangeReader(reader.get(), nullptr, nullptr, task->delete_bitmap, true, non_const_virtual_column_names, filtered_ratio_to_use_skip_read);
    }
}

void MergeTreeBaseSelectProcessor::initializeReaders(
    const MarkRanges & mark_ranges,
    const IMergeTreeReader::ValueSizeMap & avg_value_size_hints,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback)
{
    if (stream_settings.use_uncompressed_cache)
        owned_uncompressed_cache = storage.getContext()->getUncompressedCache();

    owned_mark_cache = storage.getContext()->getMarkCache();

    if (!task->task_columns.bitmap_index_columns.empty())
    {
        index_executor = index_context ?
            index_context->getIndexExecutor(
                task->data_part,
                task->data_part->index_granularity,
                storage.getSettings()->bitmap_index_segment_granularity,
                storage.getSettings()->bitmap_index_serializing_granularity,
                mark_ranges) : nullptr;
        if (index_executor && index_executor->valid())
            index_executor->initReader(MergeTreeIndexInfo::Type::BITMAP, task->task_columns.bitmap_index_columns);
        else
        {
            throw Exception(
                "Need to read bitmap index columns, but bitmap index reader is invalid. "
                "Maybe memory limit exceeded, try again later",
                ErrorCodes::INVALID_BITMAP_INDEX_READER);
        }
    }
    else
    {
        /**
         * if the current task has no bitmap-index (the current part has no bitmap-index),
         * but still has a bitmap-index reader,
         * it means the current thread read a part with bitmap-index before, thus we need reset bitmap-index.
         *
         */
        if (index_executor)
            index_executor.reset();
    }

    reader = task->data_part->getReader(
        task->task_columns.columns,
        storage_snapshot->metadata,
        mark_ranges,
        owned_uncompressed_cache.get(),
        owned_mark_cache.get(),
        stream_settings.reader_settings,
        index_executor.get(),
        avg_value_size_hints,
        profile_callback,
        [&](const Progress & value) { updateProgress(value); });

    if (prewhere_info)
    {
        if (!task->task_columns.bitmap_index_pre_columns.empty())
        {
            pre_index_executor = prewhere_info->index_context ?
                prewhere_info->index_context->getIndexExecutor(
                    task->data_part,
                    task->data_part->index_granularity,
                    storage.getSettings()->bitmap_index_segment_granularity,
                    storage.getSettings()->bitmap_index_serializing_granularity,
                    mark_ranges) : nullptr;
            if (pre_index_executor && pre_index_executor->valid())
                pre_index_executor->initReader(MergeTreeIndexInfo::Type::BITMAP, task->task_columns.bitmap_index_pre_columns);
            else
            {
                throw Exception(
                    "Need to read prewhere bitmap index columns, but prewhere bitmap index reader is invalid. "
                    "Maybe memory limit exceeded, try again later",
                    ErrorCodes::INVALID_BITMAP_INDEX_READER);
            }
        }
        else
        {
            if (pre_index_executor)
                pre_index_executor.reset();
        }

        pre_reader = task->data_part->getReader(
            task->task_columns.pre_columns,
            storage_snapshot->metadata,
            mark_ranges,
            owned_uncompressed_cache.get(),
            owned_mark_cache.get(),
            stream_settings.reader_settings,
            pre_index_executor.get(),
            avg_value_size_hints,
            profile_callback,
            [&](const Progress & value) { updateProgress(value); });
    }
}

// add mark ranges into segment bitmap grabed from the pool
void MergeTreeBaseSelectProcessor::insertMarkRangesForSegmentBitmapIndexFunctions(
    MergeTreeIndexExecutorPtr & index_executor_,
    const MarkRanges & mark_ranges_inc)
{
    if (!index_executor_)
        return;

    auto bitmap_index_reader = index_executor_->getReader(MergeTreeIndexInfo::Type::BITMAP);
    if (!bitmap_index_reader || !bitmap_index_reader->validIndexReader())
        return;

    bitmap_index_reader->addSegmentIndexesFromMarkRanges(mark_ranges_inc);
}

Chunk MergeTreeBaseSelectProcessor::readFromPartImpl()
{
    if (task->size_predictor)
        task->size_predictor->startBlock();

    const UInt64 current_max_block_size_rows = stream_settings.max_block_size;
    const UInt64 current_min_block_size_rows = stream_settings.min_block_size;
    const UInt64 current_preferred_block_size_bytes = stream_settings.preferred_block_size_bytes;
    const UInt64 current_preferred_max_column_in_block_size_bytes = stream_settings.preferred_max_column_in_block_size_bytes;
    const bool size_predictor_estimate_lc_size_by_fullstate = stream_settings.size_predictor_estimate_lc_size_by_fullstate;
    const MergeTreeIndexGranularity & index_granularity = task->data_part->index_granularity;
    const double min_filtration_ratio = 0.00001;

    auto estimate_num_rows = [current_preferred_block_size_bytes, current_max_block_size_rows,
        &index_granularity, current_preferred_max_column_in_block_size_bytes, min_filtration_ratio](
        MergeTreeReadTask & current_task, MergeTreeRangeReader & current_reader)
    {
        if (!current_task.size_predictor)
            return static_cast<size_t>(current_max_block_size_rows);

        /// Calculates number of rows will be read using preferred_block_size_bytes.
        /// Can't be less than avg_index_granularity.
        size_t rows_to_read = current_task.size_predictor->estimateNumRows(current_preferred_block_size_bytes);
        if (!rows_to_read)
            return rows_to_read;
        auto total_row_in_current_granule = current_reader.numRowsInCurrentGranule();
        rows_to_read = std::max(total_row_in_current_granule, rows_to_read);

        if (current_preferred_max_column_in_block_size_bytes)
        {
            /// Calculates number of rows will be read using preferred_max_column_in_block_size_bytes.
            auto rows_to_read_for_max_size_column
                = current_task.size_predictor->estimateNumRowsForMaxSizeColumn(current_preferred_max_column_in_block_size_bytes);
            double filtration_ratio = std::max(min_filtration_ratio, 1.0 - current_task.size_predictor->filtered_rows_ratio);
            auto rows_to_read_for_max_size_column_with_filtration
                = static_cast<size_t>(rows_to_read_for_max_size_column / filtration_ratio);

            /// If preferred_max_column_in_block_size_bytes is used, number of rows to read can be less than current_index_granularity.
            rows_to_read = std::min(rows_to_read, rows_to_read_for_max_size_column_with_filtration);
        }

        auto unread_rows_in_current_granule = current_reader.numPendingRowsInCurrentGranule();
        if (unread_rows_in_current_granule >= rows_to_read)
            return rows_to_read;

        return index_granularity.countMarksForRows(current_reader.currentMark(), rows_to_read, current_reader.numReadRowsInCurrentGranule());
    };

    UInt64 recommended_rows = estimate_num_rows(*task, task->range_reader);
    UInt64 rows_to_read = std::max(current_min_block_size_rows, std::min(current_max_block_size_rows, recommended_rows));
    rows_to_read = std::max(1UL, rows_to_read);

    auto read_result = task->range_reader.read(rows_to_read, task->mark_ranges_once_read);

    /// All rows were filtered. Repeat.
    if (read_result.num_rows == 0)
        read_result.columns.clear();

    const auto & sample_block = task->range_reader.getSampleBlock();
    if (read_result.num_rows != 0 && sample_block.columns() != read_result.columns.size())
        throw Exception("Inconsistent number of columns got from MergeTreeRangeReader. "
                        "Have " + toString(sample_block.columns()) + " in sample block "
                        "and " + toString(read_result.columns.size()) + " columns in list", ErrorCodes::LOGICAL_ERROR);

    /// TODO: check columns have the same types as in header.

    UInt64 num_filtered_rows = read_result.numReadRows() - read_result.num_rows;

    progress({ read_result.numReadRows(), read_result.numBytesRead() });
    ProfileEvents::increment(ProfileEvents::PrewhereSelectedRows, read_result.num_rows);

    if (task->size_predictor)
    {
        task->size_predictor->updateFilteredRowsRation(read_result.numReadRows(), num_filtered_rows);

        if (!read_result.columns.empty())
            task->size_predictor->update(sample_block, read_result.columns, read_result.num_rows, size_predictor_estimate_lc_size_by_fullstate);
    }

    if (read_result.num_rows == 0)
        return {};

    Columns ordered_columns;
    ordered_columns.reserve(header_without_virtual_columns.columns());

    /// Reorder columns. TODO: maybe skip for default case.
    for (size_t ps = 0; ps < header_without_virtual_columns.columns(); ++ps)
    {
        const auto & name = header_without_virtual_columns.getByPosition(ps).name;
        if (sample_block.has(name))
        {
            auto pos_in_sample_block = sample_block.getPositionByName(name);
            ordered_columns.emplace_back(std::move(read_result.columns[pos_in_sample_block]));
            if (read_result.bitmap_block.has(name))
                read_result.bitmap_block.erase(name);

        }
        /// for bitmap index result column
        else if (read_result.bitmap_block.has(name))
        {
            ordered_columns.emplace_back(std::move(read_result.bitmap_block.getByName(name).column));
            read_result.bitmap_block.erase(name);
        }
        else if (!bitmap_index_columns_superset.contains(name))
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot find columns " + name + " in result block");
        }
        else
        {
            ordered_columns.emplace_back(header_without_virtual_columns.getByPosition(ps).type->createColumnConstWithDefaultValue(read_result.num_rows));
        }
    }

    auto chunk = Chunk(std::move(ordered_columns), read_result.num_rows);
    /// When this function is call, sure task is not null
    for (auto && col : read_result.bitmap_block)
        chunk.addColumnToSideBlock(std::move(col));

    return chunk;
}


Chunk MergeTreeBaseSelectProcessor::readFromPart()
{
    if (!task->range_reader.isInitialized())
        initializeRangeReaders(*task);

    return readFromPartImpl();
}

namespace
{
    /// Simple interfaces to insert virtual columns.
    struct VirtualColumnsInserter
    {
        virtual ~VirtualColumnsInserter() = default;

        virtual void insertArrayOfStringsColumn(const ColumnPtr & column, const String & name) = 0;
        virtual void insertStringColumn(const ColumnPtr & column, const String & name) = 0;
        virtual void insertUInt64Column(const ColumnPtr & column, const String & name) = 0;
        virtual void insertInt64Column(const ColumnPtr & column, const String & name) = 0;
        virtual void insertUInt8Column(const ColumnPtr & column, const String & name) = 0;
        virtual void insertUUIDColumn(const ColumnPtr & column, const String & name) = 0;

        virtual void insertPartitionValueColumn(
            size_t rows,
            const Row & partition_value,
            const DataTypePtr & partition_value_type,
            const String & name) = 0;
    };
}

/// Adds virtual columns that are not const for all rows
static void injectNonConstVirtualColumns(
    size_t rows,
    VirtualColumnsInserter & inserter,
    const Names & virtual_columns)
{
    if (unlikely(rows))
        throw Exception("Cannot insert non-constant virtual column to non-empty chunk.",
                        ErrorCodes::LOGICAL_ERROR);

    for (const auto & virtual_column_name : virtual_columns)
    {
        if (virtual_column_name == "_part_offset")
        {
            inserter.insertUInt64Column(DataTypeUInt64().createColumn(), virtual_column_name);
        }
    }
}

static void injectPartConstVirtualColumns(
    size_t rows,
    VirtualColumnsInserter & inserter,
    MergeTreeReadTask * task,
    const DataTypePtr & partition_value_type,
    const Names & virtual_columns)
{
    /// add virtual columns
    /// Except _sample_factor and _part_row_number, which is added from the outside.
    if (!virtual_columns.empty())
    {
        if (unlikely(rows && !task))
            throw Exception("Cannot insert virtual columns to non-empty chunk without specified task.",
                            ErrorCodes::LOGICAL_ERROR);

        const IMergeTreeDataPart * part = nullptr;
        if (rows)
        {
            part = task->data_part.get();
            if (part->isProjectionPart())
                part = part->getParentPart();
        }
        for (const auto & virtual_column_name : virtual_columns)
        {
            if (virtual_column_name == "_part")
            {
                ColumnPtr column;
                if (rows)
                    column = DataTypeString().createColumnConst(rows, part->name)->convertToFullColumnIfConst();
                else
                    column = DataTypeString().createColumn();

                inserter.insertStringColumn(column, virtual_column_name);
            }
            else if (virtual_column_name == "_part_index")
            {
                ColumnPtr column;
                if (rows)
                    column = DataTypeUInt64().createColumnConst(rows, task->part_index_in_query)->convertToFullColumnIfConst();
                else
                    column = DataTypeUInt64().createColumn();

                inserter.insertUInt64Column(column, virtual_column_name);
            }
            else if (virtual_column_name == "_part_uuid")
            {
                ColumnPtr column;
                if (rows)
                    column = DataTypeUUID().createColumnConst(rows, task->data_part->uuid)->convertToFullColumnIfConst();
                else
                    column = DataTypeUUID().createColumn();

                inserter.insertUUIDColumn(column, virtual_column_name);
            }
            else if (virtual_column_name == "_part_map_files")
            {
                ColumnPtr column;
                auto type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
                if (rows)
                {
                    NameSet key_set;
                    for (auto & [file, _] : task->data_part->getChecksums()->files)
                    {
                        if (startsWith(file, getMapSeparator()) && !isMapBaseFile(file))
                            key_set.insert(unescapeForFileName(file));
                    }

                    column = type->createColumnConst(rows, Array(key_set.begin(), key_set.end()))->convertToFullColumnIfConst();
                }
                else
                    column = type->createColumn();

                inserter.insertArrayOfStringsColumn(column, virtual_column_name);
            }
            else if (virtual_column_name == "_bucket_number")
            {
                ColumnPtr column;
                if (rows)
                    column = DataTypeInt64().createColumnConst(rows, task->data_part->bucket_number)->convertToFullColumnIfConst();
                else
                    column = DataTypeInt64().createColumn();

                inserter.insertInt64Column(column, virtual_column_name);
            }
            else if (virtual_column_name == "_partition_id")
            {
                ColumnPtr column;
                if (rows)
                    column = DataTypeString().createColumnConst(rows, part->info.partition_id)->convertToFullColumnIfConst();
                else
                    column = DataTypeString().createColumn();

                inserter.insertStringColumn(column, virtual_column_name);
            }
            else if (virtual_column_name == "_partition_value")
            {
                if (rows)
                    inserter.insertPartitionValueColumn(rows, task->data_part->partition.value, partition_value_type, virtual_column_name);
                else
                    inserter.insertPartitionValueColumn(rows, {}, partition_value_type, virtual_column_name);
            }
        }
    }
}

namespace
{
    struct VirtualColumnsInserterIntoBlock : public VirtualColumnsInserter
    {
        explicit VirtualColumnsInserterIntoBlock(Block & block_) : block(block_) {}

        void insertArrayOfStringsColumn(const ColumnPtr & column, const String & name) final
        {
            block.insert({column, std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), name});
        }

        void insertStringColumn(const ColumnPtr & column, const String & name) final
        {
            block.insert({column, std::make_shared<DataTypeString>(), name});
        }

        void insertUInt64Column(const ColumnPtr & column, const String & name) final
        {
            block.insert({column, std::make_shared<DataTypeUInt64>(), name});
        }

        void insertInt64Column(const ColumnPtr & column, const String & name) final
        {
            block.insert({column, std::make_shared<DataTypeInt64>(), name});
        }

        void insertUInt8Column(const ColumnPtr & column, const String & name) final
        {
            block.insert({column, std::make_shared<DataTypeUInt8>(), name});
        }

        void insertUUIDColumn(const ColumnPtr & column, const String & name) final
        {
            block.insert({column, std::make_shared<DataTypeUUID>(), name});
        }

        void insertPartitionValueColumn(
            size_t rows, const Row & partition_value, const DataTypePtr & partition_value_type, const String & name) final
        {
            ColumnPtr column;
            if (rows)
            {
                //Tuple is just a vector hence
                //partition_value.begin() and .end() is used here to initialize Tuple
                //coverity[mismatched_iterator]
                column = partition_value_type->createColumnConst(rows, Tuple(partition_value.begin(), partition_value.end()))
                             ->convertToFullColumnIfConst();
            }
            else
            {
                column = partition_value_type->createColumn();
            }

            block.insert({column, partition_value_type, name});
        }

        Block & block;
    };

    struct VirtualColumnsInserterIntoColumns : public VirtualColumnsInserter
    {
        explicit VirtualColumnsInserterIntoColumns(Columns & columns_) : columns(columns_) {}

        void insertArrayOfStringsColumn(const ColumnPtr & column, const String &) final
        {
            columns.push_back(column);
        }

        void insertStringColumn(const ColumnPtr & column, const String &) final
        {
            columns.push_back(column);
        }

        void insertUInt64Column(const ColumnPtr & column, const String &) final
        {
            columns.push_back(column);
        }

        void insertInt64Column(const ColumnPtr & column, const String &) final
        {
            columns.push_back(column);
        }

        void insertUInt8Column(const ColumnPtr & column, const String &) final
        {
            columns.push_back(column);
        }

        void insertUUIDColumn(const ColumnPtr & column, const String &) final
        {
            columns.push_back(column);
        }

        void insertPartitionValueColumn(
            size_t rows, const Row & partition_value, const DataTypePtr & partition_value_type, const String &) final
        {
            ColumnPtr column;
            if (rows)
            {
                //Tuple is just a vector hence
                //.begin() and .end() is used here to initialize Tuple with the contents in the range
                //coverity[mismatched_iterator]
                column = partition_value_type->createColumnConst(rows, Tuple(partition_value.begin(), partition_value.end()))
                             ->convertToFullColumnIfConst();
            }
            else
            {
                column = partition_value_type->createColumn();
            }
            columns.push_back(column);
        }

        Columns & columns;
    };
}

void MergeTreeBaseSelectProcessor::injectVirtualColumns(
    Block & block, MergeTreeReadTask * task, const DataTypePtr & partition_value_type, const Names & virtual_columns)
{
    VirtualColumnsInserterIntoBlock inserter{block};
    injectNonConstVirtualColumns(block.rows(), inserter, virtual_columns);
    injectPartConstVirtualColumns(block.rows(), inserter, task, partition_value_type, virtual_columns);
}

void MergeTreeBaseSelectProcessor::injectVirtualColumns(
    Chunk & chunk, MergeTreeReadTask * task, const DataTypePtr & partition_value_type, const Names & virtual_columns)
{
    UInt64 num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    VirtualColumnsInserterIntoColumns inserter{columns};
    injectPartConstVirtualColumns(num_rows, inserter, task, partition_value_type, virtual_columns);

    chunk.setColumns(columns, num_rows);
}

Block MergeTreeBaseSelectProcessor::transformHeader(
    Block block, const PrewhereInfoPtr & prewhere_info, const DataTypePtr & partition_value_type, const Names & virtual_columns, const MergeTreeIndexContextPtr & index_context_, bool read_bitmap_index)
{
    auto * bitmap_index_info = index_context_ ? dynamic_cast<BitmapIndexInfo *>(index_context_->get(MergeTreeIndexInfo::Type::BITMAP).get()) : nullptr;
    if (bitmap_index_info)
    {
        if (auto projection_index_action = index_context_->getProjectionActions(false); projection_index_action && read_bitmap_index)
        {
            auto projection_index_expression = std::make_shared<ExpressionActions>(projection_index_action);
            projection_index_expression->execute(block);
        }
        block = bitmap_index_info->updateHeader(std::move(block));
    }
    if (prewhere_info)
    {
        if (prewhere_info->alias_actions)
            block = prewhere_info->alias_actions->updateHeader(std::move(block));

        if (prewhere_info->row_level_filter)
        {
            block = prewhere_info->row_level_filter->updateHeader(std::move(block));
            auto & row_level_column = block.getByName(prewhere_info->row_level_column_name);
            if (!row_level_column.type->canBeUsedInBooleanContext())
            {
                throw Exception("Invalid type for filter in PREWHERE: " + row_level_column.type->getName(),
                    ErrorCodes::LOGICAL_ERROR);
            }

            block.erase(prewhere_info->row_level_column_name);
        }

        if (prewhere_info->prewhere_actions)
            block = prewhere_info->prewhere_actions->updateHeader(std::move(block));

        auto & prewhere_column = block.getByName(prewhere_info->prewhere_column_name);
        if (!prewhere_column.type->canBeUsedInBooleanContext())
        {
            throw Exception("Invalid type for filter in PREWHERE: " + prewhere_column.type->getName(),
                ErrorCodes::LOGICAL_ERROR);
        }

        if (prewhere_info->remove_prewhere_column)
            block.erase(prewhere_info->prewhere_column_name);
        else
        {
            WhichDataType which(removeNullable(recursiveRemoveLowCardinality(prewhere_column.type)));
            if (which.isInt() || which.isUInt())
                prewhere_column.column = prewhere_column.type->createColumnConst(block.rows(), 1u)->convertToFullColumnIfConst();
            else if (which.isFloat())
                prewhere_column.column = prewhere_column.type->createColumnConst(block.rows(), 1.0f)->convertToFullColumnIfConst();
            else
                throw Exception("Illegal type " + prewhere_column.type->getName() + " of column for filter.",
                                ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);
        }
    }

    injectVirtualColumns(block, nullptr, partition_value_type, virtual_columns);
    return block;
}


MergeTreeBaseSelectProcessor::~MergeTreeBaseSelectProcessor() = default;

}
