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
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/Index/BitmapIndexHelper.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Storages/SelectQueryInfo.h>

#include <Processors/Sources/SourceWithProgress.h>
#include <Interpreters/ExpressionActionsSettings.h>
#include <Interpreters/ExtractExpressionInfoVisitor.h>
#include <Core/Names.h>
#include <QueryPlan/ReadFromMergeTree.h>

namespace DB
{

class IMergeTreeReader;
class UncompressedCache;
class MarkCache;
struct PrewhereExprInfo;


/// Base class for MergeTreeThreadSelectProcessor and MergeTreeSelectProcessor
class MergeTreeBaseSelectProcessor : public SourceWithProgress
{
public:
    MergeTreeBaseSelectProcessor(
        Block header,
        const MergeTreeMetaBase & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        const SelectQueryInfo & query_info_,
        const MergeTreeStreamSettings & stream_settings_,
        const Names & virt_column_names_ = {});

    ~MergeTreeBaseSelectProcessor() override;

    static Block transformHeader(
        Block block, const PrewhereInfoPtr & prewhere_info, const DataTypePtr & partition_value_type, const Names & virtual_columns,
        const MergeTreeIndexContextPtr & index_context_, bool read_bitmap_index);
    /// Two versions for header and chunk.
    static void
    injectVirtualColumns(Block & block, MergeTreeReadTask * task, const DataTypePtr & partition_value_type, const Names & virtual_columns);
    static void
    injectVirtualColumns(Chunk & chunk, MergeTreeReadTask * task, const DataTypePtr & partition_value_type, const Names & virtual_columns);

protected:
    Chunk generate() final;

    /// Creates new this->task, and initializes readers.
    virtual bool getNewTask() = 0;

    virtual Chunk readFromPart();

    Chunk readFromPartImpl();


    void initializeRangeReaders(MergeTreeReadTask & task);
    void initializeReaders(const MarkRanges & mark_ranges, const IMergeTreeReader::ValueSizeMap & avg_value_size_hints = IMergeTreeReader::ValueSizeMap{}, const ReadBufferFromFileBase::ProfileCallback & profile_callback = ReadBufferFromFileBase::ProfileCallback{});
    void insertMarkRangesForSegmentBitmapIndexFunctions(MergeTreeIndexExecutorPtr & index_executor, const MarkRanges & mark_ranges_inc);

protected:
    const MergeTreeMetaBase & storage;
    StorageSnapshotPtr storage_snapshot;

    PrewhereInfoPtr prewhere_info;
    std::unique_ptr<PrewhereExprInfo> prewhere_actions;
    MergeTreeIndexContextPtr index_context;

    MergeTreeStreamSettings stream_settings;

    Names virt_column_names;

    /// These columns will be filled by the merge tree range reader
    Names non_const_virtual_column_names;

    DataTypePtr partition_value_type;

    /// This header is used for chunks from readFromPart().
    Block header_without_virtual_columns;

    std::unique_ptr<MergeTreeReadTask> task;

    std::shared_ptr<UncompressedCache> owned_uncompressed_cache;
    std::shared_ptr<MarkCache> owned_mark_cache;

    using MergeTreeReaderPtr = std::unique_ptr<IMergeTreeReader>;
    using MergeTreeBitMapIndexReaderPtr = std::unique_ptr<MergeTreeBitmapIndexReader>;

    MergeTreeReaderPtr reader;
    MergeTreeReaderPtr pre_reader;
    MergeTreeIndexExecutorPtr index_executor;
    MergeTreeIndexExecutorPtr pre_index_executor;
    NameSet bitmap_index_columns_superset;
    bool support_intermedicate_result_cache;

};

}
