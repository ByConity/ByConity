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
#include "Storages/MergeTree/MergeTreeSuffix.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
}

MergeTreeSequentialSource::RuntimeContext::~RuntimeContext()
{
    size_t total_rows_ = total_rows.load(std::memory_order_relaxed);
    size_t total_bytes_ = total_bytes.load(std::memory_order_relaxed);
    size_t update_count_ = update_count.load(std::memory_order_relaxed);
    size_t avg_read_rows = update_count_ > 0 ? (total_rows_ / update_count_) : 0;
    size_t bytes_per_row = total_rows_ > 0 ? (total_bytes_ / total_rows_) : 0;

    LOG_TRACE(getLogger("MergeTreeSequentialSource::RuntimeContext"),
        "Total rows {}, total bytes {}, read count {}, average read rows {}, bytes per row {}",
        total_rows_, ReadableSize(total_bytes_), update_count_, avg_read_rows, ReadableSize(bytes_per_row));
}

void MergeTreeSequentialSource::RuntimeContext::update(size_t rows_read_, const Columns & columns)
{
    size_t total_bytes_ = 0;
    for (auto & col: columns)
    {
        if (col)
        {
            auto lc_col = typeid_cast<const ColumnLowCardinality *>(col.get());
            /// lc in global state will only calculate indexes bytes in byteSize(), so we only need to handle local state here
            if (lc_col && !lc_col->isFullState() && lc_col->getDictionary().size() > lc_col->size())
            {
                /// If lc_col->getDictionary().size() > lc_col->size(), the average bytes of each rows based on col->byteSize()
                /// is larger than average bytes of each dictionary key, limit to read a smaller rows may still get a big dictionary,
                /// and makes average bytes of each rows larger and larger. So we assuming the column is in fullstate, so the result
                /// of average bytes of each rows will converge to a stable value (max_bytes / avg_bytes_of_each_dict_key).
                size_t approx_total_bytes_in_fullstate = lc_col->size() * lc_col->getDictionary().byteSize() / lc_col->getDictionary().size();
                total_bytes += approx_total_bytes_in_fullstate;
            }
            else
            {
                total_bytes += col->byteSize();
            }
        }
    }

    total_rows.fetch_add(rows_read_, std::memory_order_relaxed);
    total_bytes.fetch_add(total_bytes_, std::memory_order_relaxed);
    update_count.fetch_add(1, std::memory_order_relaxed);
}


MergeTreeSequentialSource::MergeTreeSequentialSource(
    const MergeTreeMetaBase & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    MergeTreeMetaBase::DataPartPtr data_part_,
    Names columns_to_read_,
    bool read_with_direct_io_,
    bool take_column_types_from_storage,
    bool quiet,
    CnchMergePrefetcher::PartFutureFiles * future_files,
    BitEngineReadType bitengine_read_type,
    size_t block_preferred_size_bytes_,
    MergeTreeSequentialSource::RuntimeContextPtr rt_ctx_)
    : MergeTreeSequentialSource(
        storage_,
        storage_snapshot_,
        data_part_,
        data_part_->getDeleteBitmap(),
        columns_to_read_,
        read_with_direct_io_,
        take_column_types_from_storage,
        quiet,
        future_files,
        bitengine_read_type,
        block_preferred_size_bytes_,
        rt_ctx_)
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
    BitEngineReadType bitengine_read_type,
    size_t block_preferred_size_bytes_,
    MergeTreeSequentialSource::RuntimeContextPtr rt_ctx_)
    : SourceWithProgress(storage_snapshot_->getSampleBlockForColumns(columns_to_read_, {}, bitengine_read_type))
    , storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , data_part(std::move(data_part_))
    , delete_bitmap(std::move(delete_bitmap_))
    , columns_to_read(std::move(columns_to_read_))
    , read_with_direct_io(read_with_direct_io_)
    , mark_cache(storage.getContext()->getMarkCache())
    , block_preferred_size_bytes(block_preferred_size_bytes_)
    , rt_ctx(rt_ctx_)
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
        },
        .save_marks_in_cache = false,
        .read_source_bitmap = (bitengine_read_type == BitEngineReadType::ONLY_SOURCE),
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
            /* uncompressed_cache = */ nullptr, mark_cache.get(), reader_settings, nullptr, {}, {}, [&](const Progress& value) { this->progress(value); });
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
        size_t mark_start_row = currentMarkStart();
        size_t mark_end_row = currentMarkEnd();

        if (current_row > mark_end_row || current_row < mark_start_row)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Failed to read from part {} at mark {}, current_row should in range of [mark_start_row, mark_end_row), this is a bug. "
                "current rows {}, mark start rows {}, mark end rows {}.", data_part->getFullPath(), current_mark, current_row,
                mark_start_row, mark_end_row);
        }

        size_t rows_to_read = mark_end_row - current_row;

        /// Runtime context will be shared between all the sequential source reading the same columns from different parts in merge,
        /// so we can store some statistic infos in it and calculate approximate bytes of single rows. This can help to limit to
        /// read fewer rows and make the result block bytes smaller than block_preferred_size_bytes.
        if (rt_ctx && block_preferred_size_bytes > 0)
        {
            /// Using memory_order_relaxed here is safe in multi-thread scenario, a larger or smaller approx_rows_by_byte
            /// can lead to use a little more memory or reading a bit slower, but can converge quickly.
            size_t total_rows = rt_ctx->total_rows.load(std::memory_order_relaxed);
            size_t total_bytes = rt_ctx->total_bytes.load(std::memory_order_relaxed);

            if (total_rows > 0 && total_bytes > 0)
            {
                /// Need a min rows here. For example, if we read a lc column with a huge dictionary and a few rows,
                /// these infos will make approx_rows_by_bytes smaller every time and finally reach the min rows.
                size_t approx_rows_by_bytes = std::max(100UL, block_preferred_size_bytes * total_rows / total_bytes);

                /// If only remains a few rows in current mark, read them together. This may consume up to 50% memory additionally.
                if (approx_rows_by_bytes + (approx_rows_by_bytes / 2) < rows_to_read)
                    rows_to_read = std::min(rows_to_read, approx_rows_by_bytes);
            }
        }

        const auto & sample = reader->getColumns();
        Columns columns(sample.size());
        size_t rows_read = reader->readRows(current_mark, current_row - mark_start_row,
            rows_to_read, data_part->getMarksCount(), nullptr, columns);

        if (rows_read)
        {
            if (rt_ctx)
            {
                rt_ctx->update(rows_read, columns);
            }

            size_t num_deleted = 0;
            if (delete_bitmap)
            {
                /// construct delete filter for current granule
                ColumnUInt8::MutablePtr delete_column = ColumnUInt8::create(rows_read, 1);
                UInt8 * filter_data = delete_column->getData().data();
                size_t end_row = current_row + rows_read;

                auto iter = delete_bitmap->begin();
                iter.equalorlarger(current_row);
                for (auto end = delete_bitmap->end(); iter != end && *iter < end_row; iter++)
                {
                    filter_data[*iter - current_row] = 0;
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
            current_mark += (current_row >= mark_end_row);

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
