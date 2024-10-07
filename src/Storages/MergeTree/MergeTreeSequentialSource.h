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
#include <memory>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MarkRange.h>
#include <WorkerTasks/CnchMergePrefetcher.h>
#include <memory>

namespace DB
{

/// Lightweight (in terms of logic) stream for reading single part from MergeTree
class MergeTreeSequentialSource : public SourceWithProgress
{
public:
    struct RuntimeContext
    {
        std::atomic<size_t> total_rows = 0;
        std::atomic<size_t> total_bytes = 0;
        std::atomic<size_t> update_count = 0;

        virtual ~RuntimeContext();

        void update(size_t rows_read_, const Columns & columns);
    };
    using RuntimeContextPtr = std::shared_ptr<RuntimeContext>;

    /// NOTE: in case you want to read part with row id included, please add extra `_part_row_number` to
    /// the columns you want to read.
    MergeTreeSequentialSource(
        const MergeTreeMetaBase & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        MergeTreeMetaBase::DataPartPtr data_part_,
        Names columns_to_read_,
        bool read_with_direct_io_,
        bool take_column_types_from_storage,
        bool quiet = false,
        CnchMergePrefetcher::PartFutureFiles * future_files = nullptr,
        BitEngineReadType bitengine_read_type = BitEngineReadType::ONLY_SOURCE,
        size_t block_preferred_size_bytes_ = 0, /// 0 means unlimited, will read single granule each time
        RuntimeContextPtr rt_ctx_ = nullptr);

    MergeTreeSequentialSource(
        const MergeTreeMetaBase & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        MergeTreeMetaBase::DataPartPtr data_part_,
        ImmutableDeleteBitmapPtr delete_bitmap_,
        Names columns_to_read_,
        bool read_with_direct_io_,
        bool take_column_types_from_storage,
        bool quiet = false,
        CnchMergePrefetcher::PartFutureFiles* future_files = nullptr,
        BitEngineReadType bitengine_read_type = BitEngineReadType::ONLY_SOURCE,
        size_t block_preferred_size_bytes_ = 0, /// 0 means unlimited, will read single granule each time
        RuntimeContextPtr rt_ctx_ = nullptr);

    ~MergeTreeSequentialSource() override;

    String getName() const override { return "MergeTreeSequentialSource"; }

    size_t getCurrentMark() const { return current_mark; }

    size_t getCurrentRow() const { return current_row; }

protected:
    Chunk generate() override;

private:

    const MergeTreeMetaBase & storage;
    StorageSnapshotPtr storage_snapshot;

    /// Data part will not be removed if the pointer owns it
    MergeTreeMetaBase::DataPartPtr data_part;
    ImmutableDeleteBitmapPtr delete_bitmap;

    /// Columns we have to read (each Block from read will contain them)
    Names columns_to_read;

    /// Should read using direct IO
    bool read_with_direct_io;

    LoggerPtr log = getLogger("MergeTreeSequentialSource");

    std::shared_ptr<MarkCache> mark_cache;
    using MergeTreeReaderPtr = std::unique_ptr<IMergeTreeReader>;
    MergeTreeReaderPtr reader;

    /// current mark at which we stop reading
    size_t current_mark = 0;

    /// current row at which we stop reading
    size_t current_row = 0;

    size_t block_preferred_size_bytes = 0;

    RuntimeContextPtr rt_ctx = nullptr;

private:
    /// Closes readers and unlock part locks
    void finish();
    size_t currentMarkStart() const { return data_part->index_granularity.getMarkStartingRow(current_mark); }
    size_t currentMarkEnd() const { return data_part->index_granularity.getMarkStartingRow(current_mark + 1); }
};

}
