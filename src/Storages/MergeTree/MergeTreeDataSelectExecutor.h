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
#include <unordered_map>
#include <Core/QueryProcessingStage.h>
#include <Storages/SelectQueryInfo.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/PartitionPruner.h>
#include <QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MultiIndexFilterCondition.h>

namespace DB
{

class KeyCondition;
struct IndexTimeWatcher;

using PartitionIdToMaxBlock = std::unordered_map<String, Int64>;

/** Executes SELECT queries on data from the merge tree.
  */
class MergeTreeDataSelectExecutor
{
public:
    explicit MergeTreeDataSelectExecutor(const MergeTreeMetaBase & data_);

    /** When reading, selects a set of parts that covers the desired range of the index.
      * max_blocks_number_to_read - if not nullptr, do not read all the parts whose right border is greater than max_block in partition.
      */

    QueryPlanPtr read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        const SelectQueryInfo & query_info,
        ContextPtr context,
        UInt64 max_block_size,
        unsigned num_streams,
        QueryProcessingStage::Enum processed_stage,
        std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read = nullptr) const;

    /// The same as read, but with specified set of parts.
    QueryPlanPtr readFromParts(
        MergeTreeMetaBase::DataPartsVector parts,
        MergeTreeMetaBase::DeleteBitmapGetter delete_bitmap_getter,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        const SelectQueryInfo & query_info,
        ContextPtr context,
        UInt64 max_block_size,
        unsigned num_streams,
        std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read = nullptr,
        MergeTreeDataSelectAnalysisResultPtr merge_tree_select_result_ptr = nullptr) const;

    /// Get an estimation for the number of marks we are going to read.
    /// Reads nothing. Secondary indexes are not used.
    /// This method is used to select best projection for table.
    MergeTreeDataSelectAnalysisResultPtr estimateNumMarksToRead(
        MergeTreeMetaBase::DataPartsVector parts,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot_base,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        ContextPtr context,
        unsigned num_streams,
        std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read = nullptr) const;

private:
    const MergeTreeMetaBase & data;
    LoggerPtr log;

    /// Get the approximate value (bottom estimate - only by full marks) of the number of rows falling under the index.
    static size_t getApproximateTotalRowsToRead(
        const MergeTreeMetaBase::DataPartsVector & parts,
        const StorageMetadataPtr & metadata_snapshot,
        const KeyCondition & key_condition,
        const Settings & settings,
        LoggerPtr log);

    static MarkRanges markRangesFromPKRange(
        const MergeTreeMetaBase::DataPartPtr & part,
        const StorageMetadataPtr & metadata_snapshot,
        const KeyCondition & key_condition,
        const Settings & settings,
        LoggerPtr log);

    /// If filter_bitmap is nullptr, then we won't trying to generate read filter
    static MarkRanges filterMarksUsingIndex(
        MergeTreeIndexPtr index_helper,
        MergeTreeIndexConditionPtr condition,
        MergeTreeMetaBase::DataPartPtr part,
        const MarkRanges & ranges,
        ContextPtr context,
        const MergeTreeReaderSettings & reader_settings,
        size_t & total_granules,
        size_t & granules_dropped,
        roaring::Roaring * filter_bitmap,
        LoggerPtr log,
        IndexTimeWatcher & index_time_watcher);

    struct PartFilterCounters
    {
        size_t num_initial_selected_parts = 0;
        size_t num_initial_selected_granules = 0;
        size_t num_parts_after_minmax = 0;
        size_t num_granules_after_minmax = 0;
        size_t num_parts_after_partition_pruner = 0;
        size_t num_granules_after_partition_pruner = 0;
    };

    /// Select the parts in which there can be data that satisfy `minmax_idx_condition` and that match the condition on `_part`,
    ///  as well as `max_block_number_to_read`.
    static void selectPartsToRead(
        MergeTreeMetaBase::DataPartsVector & parts,
        const std::optional<std::unordered_set<String>> & part_values,
        const std::optional<KeyCondition> & minmax_idx_condition,
        const DataTypes & minmax_columns_types,
        std::optional<PartitionPruner> & partition_pruner,
        const PartitionIdToMaxBlock * max_block_numbers_to_read,
        PartFilterCounters & counters);

    /// Same as previous but also skip parts uuids if any to the query context, or skip parts which uuids marked as excluded.
    static void selectPartsToReadWithUUIDFilter(
        MergeTreeMetaBase::DataPartsVector & parts,
        const std::optional<std::unordered_set<String>> & part_values,
        MergeTreeMetaBase::PinnedPartUUIDsPtr pinned_part_uuids,
        const std::optional<KeyCondition> & minmax_idx_condition,
        const DataTypes & minmax_columns_types,
        std::optional<PartitionPruner> & partition_pruner,
        const PartitionIdToMaxBlock * max_block_numbers_to_read,
        ContextPtr query_context,
        PartFilterCounters & counters,
        LoggerPtr log);

public:
    /// For given number rows and bytes, get the number of marks to read.
    /// It is a minimal number of marks which contain so many rows and bytes.
    static size_t roundRowsOrBytesToMarks(
        size_t rows_setting,
        size_t bytes_setting,
        size_t rows_granularity,
        size_t bytes_granularity);

    /// The same as roundRowsOrBytesToMarks, but return no more than max_marks.
    static size_t minMarksForConcurrentRead(
        size_t rows_setting,
        size_t bytes_setting,
        size_t rows_granularity,
        size_t bytes_granularity,
        size_t max_marks);

    /// If possible, filter using expression on virtual columns.
    /// Example: SELECT count() FROM table WHERE _part = 'part_name'
    /// If expression found, return a set with allowed part names (std::nullopt otherwise).
    static std::optional<std::unordered_set<String>> filterPartsByVirtualColumns(
        const MergeTreeMetaBase & data,
        const MergeTreeMetaBase::DataPartsVector & parts,
        const SelectQueryInfo & query_info,
        ContextPtr context);

    static std::optional<bool> isValidPartitionFilter(
        const StoragePtr & storage,
        ASTPtr filter,
        ContextPtr context);

    /// Filter parts using minmax index and partition key.
    static void filterPartsByPartition(
        MergeTreeMetaBase::DataPartsVector & parts,
        const std::optional<std::unordered_set<String>> & part_values,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeMetaBase & data,
        const SelectQueryInfo & query_info,
        const ContextPtr & context,
        const PartitionIdToMaxBlock * max_block_numbers_to_read,
        LoggerPtr log,
        ReadFromMergeTree::IndexStats & index_stats);

    static DataTypes get_set_element_types(const NamesAndTypesList & source_columns, const String & column_name);

    static bool extractBitmapIndexImpl(
        const SelectQueryInfo&    queryInfo,
        const ASTPtr&             expr,
        const MergeTreeMetaBase&      data,
        std::map<String, SetPtr>& bitmap_index_map);

    static bool extractBitmapIndex(
        const SelectQueryInfo &    queryInfo,
        const MergeTreeMetaBase &      data,
        std::map<String, SetPtr> & bitmap_index_map);

    /// Filter parts using primary key and secondary indexes.
    /// For every part, select mark ranges to read.
    static RangesInDataParts filterPartsByPrimaryKeyAndSkipIndexes(
        MergeTreeMetaBase::DataPartsVector && parts,
        StorageMetadataPtr metadata_snapshot,
        const SelectQueryInfo & query_info,
        const ContextPtr & context,
        const KeyCondition & key_condition,
        const MergeTreeReaderSettings & reader_settings,
        LoggerPtr log,
        size_t num_streams,
        ReadFromMergeTree::IndexStats & index_stats,
        SkipIndexFilterInfo & delayed_indices_,
        bool use_skip_indexes,
        const MergeTreeMetaBase & data_,
        bool use_sampling,
        RelativeSize relative_sample_size);

    static RangesInDataParts filterPartsByIntermediateResultCache(
        const StorageID & storage_id,
        const SelectQueryInfo & query_info,
        const ContextPtr & context,
        LoggerPtr log,
        RangesInDataParts & parts_with_ranges,
        CacheHolderPtr & part_cache_holder);

    /// Create expression for sampling.
    /// Also, calculate _sample_factor if needed.
    /// Also, update key condition with selected sampling range.
    static MergeTreeDataSelectSamplingData getSampling(
        const ASTSelectQuery & select,
        NamesAndTypesList available_real_columns,
        const MergeTreeMetaBase::DataPartsVector & parts,
        KeyCondition & key_condition,
        const MergeTreeMetaBase & data,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        bool sample_factor_column_queried,
        LoggerPtr log);

    static MarkRanges sampleByRange(
        const MergeTreeMetaBase::DataPartPtr & part,
        const MarkRanges & ranges,
        const RelativeSize & relative_sample_size,
        bool deterministic,
        bool uniform,
        bool ensure_one_mark_in_part_when_sample_by_range);

    static MarkRanges sliceRange(const MarkRange & range, const UInt64 & sample_size);

    /// Check query limits: max_partitions_to_read, max_concurrent_queries.
    /// Also, return QueryIdHolder. If not null, we should keep it until query finishes.
    static std::shared_ptr<QueryIdHolder> checkLimits(
        const MergeTreeMetaBase & data,
        const RangesInDataParts & parts_with_ranges,
        const ContextPtr & context);

    static bool shouldFilterMarkRangesAtPipelineExec(const Settings& settings,
        const InputOrderInfoPtr& input_order);

    static MarkRanges filterMarkRangesForPartByInvertedIndex(
        const MergeTreeData::DataPartPtr& part_, const MarkRanges& mark_ranges_,
        const std::shared_ptr<SkipIndexFilterInfo>& delayed_index_, const ContextPtr& context_,
        const MergeTreeReaderSettings& reader_settings_, roaring::Roaring* row_filter_,
        size_t& total_granules_, size_t& dropped_granules_);

    static MarkRanges filterMarkRangesForPartByMultiInvertedIndex(
        const MergeTreeData::DataPartPtr& part_, const MarkRanges& mark_ranges_,
        MultiIndexFilterCondition& multi_idx_cond_, roaring::Roaring* row_filter_,
        IndexTimeWatcher& idx_timer_, size_t& total_granules_, size_t& dropped_granules_);
};

struct IndexTimeWatcher
{
    enum class Type
    {
        SEEK,
        READ,
        CLAC,
    };

    Stopwatch index_seek_watcher;
    Stopwatch index_read_watcher;
    Stopwatch index_calc_watcher;

    size_t index_granule_seek_time;
    size_t index_granule_read_time;
    size_t index_condition_calc_time;

    explicit IndexTimeWatcher(): index_granule_seek_time(0), index_granule_read_time(0), index_condition_calc_time(0) {}

    void begin(Type type)
    {
        switch (type)
        {
            case Type::SEEK: index_seek_watcher.restart(); return;
            case Type::READ: index_read_watcher.restart(); return;
            case Type::CLAC: index_calc_watcher.restart(); return;
        }
    }

    void elapsed(Type type)
    {
        switch (type)
        {
            case Type::SEEK: index_granule_seek_time += index_seek_watcher.elapsedMicroseconds(); return;
            case Type::READ: index_granule_read_time += index_read_watcher.elapsedMicroseconds(); return;
            case Type::CLAC: index_condition_calc_time += index_calc_watcher.elapsedMicroseconds(); return;
        }
    }

    void watch(IndexTimeWatcher::Type type, std::function<void()> func)
    {
        begin(type);
        func();
        elapsed(type);
    }
};

}
