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

#include <algorithm>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreePrefetchedReaderCNCH.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeFactory.h>
#include <Core/Defines.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

MergeTreeSequentialSource::MergeTreeSequentialSource(
    const MergeTreeMetaBase & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    MergeTreeMetaBase::DataPartPtr data_part_,
    Names columns_to_read_,
    bool read_with_direct_io_,
    bool take_column_types_from_storage,
    bool quiet,
    CnchMergePrefetcher::PartFutureFiles* future_files)
    : MergeTreeSequentialSource(
        storage_,
        storage_snapshot_,
        data_part_,
        data_part_->getDeleteBitmap(),
        columns_to_read_,
        read_with_direct_io_,
        take_column_types_from_storage,
        quiet,
        future_files)
{
}

MergeTreeSequentialSource::MergeTreeSequentialSource(
    const MergeTreeMetaBase & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    MergeTreeMetaBase::DataPartPtr data_part_,
    ImmutableDeleteBitmapPtr delete_bitmap_,
    Names columns_to_read_,
    bool read_with_direct_io_,
    bool take_column_types_from_storage,
    bool quiet,
    CnchMergePrefetcher::PartFutureFiles* future_files,
    BitEngineReadType bitengine_read_type)
    : SourceWithProgress(storage_snapshot_->getSampleBlockForColumns(columns_to_read_, {}, bitengine_read_type))
    , storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , data_part(std::move(data_part_))
    , delete_bitmap(std::move(delete_bitmap_))
    , columns_to_read(std::move(columns_to_read_))
    , read_with_direct_io(read_with_direct_io_)
    , mark_cache(storage.getContext()->getMarkCache())
{
    size_t num_deletes = delete_bitmap ? delete_bitmap->cardinality() : 0;

    addTotalRowsApprox(data_part->rows_count - num_deletes);

    /// Add columns because we don't want to read empty blocks
    injectRequiredColumns(storage, storage_snapshot->metadata, data_part, columns_to_read,
        future_files == nullptr ? "" : future_files->getFixedInjectedColumn());
    NamesAndTypesList columns_for_reader;
    if (take_column_types_from_storage)
    {
        auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical).withExtendedObjects();
        columns_for_reader = storage_snapshot->getColumnsByNames(options, columns_to_read);

        if (bitengine_read_type != BitEngineReadType::ONLY_SOURCE)
            columns_for_reader = columns_for_reader.addTypes(columns_for_reader.getNames(), bitengine_read_type);
    }
    else
    {
        /// take columns from data_part
        columns_for_reader = data_part->getColumns().addTypes(columns_to_read, bitengine_read_type);
    }

    if (!quiet)
    {
        /// Print column name but don't pollute logs in case of many columns.
        if (columns_for_reader.size() == 1)
            LOG_TRACE(log, "Reading {} marks from part {}, total {} rows starting from the beginning of the part, column {}",
                      data_part->getMarksCount(), data_part->name, data_part->rows_count, columns_for_reader.getNames().front());
        else
            LOG_TRACE(log, "Reading {} marks from part {}, total {} rows starting from the beginning of the part",
                      data_part->getMarksCount(), data_part->name, data_part->rows_count);
    }

    MergeTreeReaderSettings reader_settings =
    {
        /// bytes to use AIO (this is hack)
        .read_settings = ReadSettings {
            .aio_threshold = read_with_direct_io ? 1UL : std::numeric_limits<size_t>::max(),
            .buffer_size = DBMS_DEFAULT_BUFFER_SIZE,
        },
        .save_marks_in_cache = false,
        .read_source_bitmap = true,
    };

    if (future_files)
    {
        /// need convert xx.xx to a subcolumn of nested column xx, this setting will be set at data_part->getReader()
        reader_settings.convert_nested_to_subcolumns = true;

        reader = std::make_unique<MergeTreePrefetchedReaderCNCH>(
            data_part, columns_for_reader, storage_snapshot->metadata, nullptr,
            MarkRanges{MarkRange(0, data_part->getMarksCount())}, reader_settings,
            future_files
        );
    }
    else
    {
        reader = data_part->getReader(columns_for_reader, storage_snapshot->metadata,
            MarkRanges{MarkRange(0, data_part->getMarksCount())},
            /* uncompressed_cache = */ nullptr, mark_cache.get(), reader_settings, nullptr, {}, {}, internal_progress_callback);
    }
}

Chunk MergeTreeSequentialSource::generate()
try
{
    if (delete_bitmap)
    {
        /// skip deleted mark
        size_t marks_count = data_part->index_granularity.getMarksCount();
        while (current_mark < marks_count && delete_bitmap->containsRange(currentMarkStart(), currentMarkEnd()))
        {
            current_row += data_part->index_granularity.getMarkRows(current_mark);
            current_mark++;
        }
        if (current_mark >= marks_count)
        {
            finish();
            return Chunk();
        }
    }

    const auto & header = getPort().getHeader();

    if (!isCancelled() && current_row < data_part->rows_count)
    {
        size_t rows_to_read = data_part->index_granularity.getMarkRows(current_mark);

        const auto & sample = reader->getColumns();
        Columns columns(sample.size());
        size_t rows_read = reader->readRows(current_mark, data_part->getMarksCount(),
            current_row - data_part->index_granularity.getMarkStartingRow(current_mark),
            rows_to_read, columns);

        if (rows_read)
        {
            size_t num_deleted = 0;
            if (delete_bitmap)
            {
                /// construct delete filter for current granule
                ColumnUInt8::MutablePtr delete_column = ColumnUInt8::create(rows_read, 1);
                UInt8 * filter_data = delete_column->getData().data();
                size_t start_row = currentMarkStart();
                size_t end_row = currentMarkEnd();

                auto iter = delete_bitmap->begin();
                iter.equalorlarger(start_row);
                for (auto end = delete_bitmap->end(); iter != end && *iter < end_row; iter++)
                {
                    filter_data[*iter - start_row] = 0;
                    num_deleted++;
                }
                for (auto & column : columns)
                {
                    /// The column is nullptr when it doesn't exist in the data_part, this case will happen in the following cases:
                    /// 1. When the table has applied operations of adding columns.
                    /// 2. When query a map implicit column that doesn't exist.
                    /// 3. When query a map column that doesn't have any key.
                    if (column)
                        column = column->filter(delete_column->getData(), rows_read - num_deleted);
                }
            }

            bool should_evaluate_missing_defaults = false;
            reader->fillMissingColumns(columns, should_evaluate_missing_defaults, rows_read - num_deleted);

            if (should_evaluate_missing_defaults)
            {
                reader->evaluateMissingDefaults({}, columns);
            }

            reader->performRequiredConversions(columns);

            Columns res_columns;
            res_columns.reserve(sample.size());

            auto it = sample.begin();
            for (size_t i = 0; i < sample.size(); ++i)
            {
                if (header.has(it->name))
                    res_columns.emplace_back(std::move(columns[i]));
                ++it;
            }

            current_row += rows_read;
            current_mark += (rows_to_read == rows_read);

            LOG_TRACE(
                log,
                "Try to read rows {}, actual read rows {}, delete {} row, remaining rows {} from part {}",
                rows_to_read,
                rows_read,
                num_deleted,
                rows_read - num_deleted,
                data_part->name);
            return Chunk(std::move(res_columns), rows_read - num_deleted);
        }
    }
    else
    {
        finish();
    }

    return {};
}
catch (...)
{
    /// Suspicion of the broken part. A part is added to the queue for verification.
    if (getCurrentExceptionCode() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
        storage.reportBrokenPart(data_part->name);
    throw;
}

void MergeTreeSequentialSource::finish()
{
    /** Close the files (before destroying the object).
     * When many sources are created, but simultaneously reading only a few of them,
     * buffers don't waste memory.
     */
    reader.reset();
    data_part.reset();
    delete_bitmap.reset();
}

MergeTreeSequentialSource::~MergeTreeSequentialSource() = default;

}
