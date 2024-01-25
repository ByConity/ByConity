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

#include <limits>
#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <sparsehash/dense_hash_map>

namespace DB
{

class IDataType;

/// Reads the data between pairs of marks in the same part. When reading consecutive ranges, avoids unnecessary seeks.
/// When ranges are almost consecutive, seeks are fast because they are performed inside the buffer.
/// Avoids loading the marks file if it is not needed (e.g. when reading the whole part).
class IMergeTreeReader : private boost::noncopyable
{
public:
    using ValueSizeMap = std::map<std::string, double>;
	using MapColumnKeys = std::multimap<std::string, std::string>;
    using DeserializeBinaryBulkStateMap = std::map<std::string, ISerialization::DeserializeBinaryBulkStatePtr>;

    IMergeTreeReader(
        const MergeTreeMetaBase::DataPartPtr & data_part_,
        const NamesAndTypesList & columns_,
        const StorageMetadataPtr & metadata_snapshot_,
        UncompressedCache * uncompressed_cache_,
        MarkCache * mark_cache_,
        const MarkRanges & all_mark_ranges_,
        const MergeTreeReaderSettings & settings_,
        const ValueSizeMap & avg_value_size_hints_ = ValueSizeMap{},
        MergeTreeIndexExecutor * index_executor_ = nullptr);

    /// Return the number of rows has been read or zero if there is no columns to read.
    /// If continue_reading is true, continue reading from last state, otherwise seek to from_mark
    virtual size_t readRows(size_t from_mark, size_t current_task_last_mark, size_t from_row, size_t max_rows_to_read, Columns & res_columns) = 0;

    virtual bool canReadIncompleteGranules() const = 0;

    virtual ~IMergeTreeReader();

    const ValueSizeMap & getAvgValueSizeHints() const;

    /// Add columns from ordered_names that are not present in the block.
    /// Missing columns are added in the order specified by ordered_names.
    /// num_rows is needed in case if all res_columns are nullptr.
    void fillMissingColumns(
        Columns & res_columns, bool & should_evaluate_missing_defaults, size_t num_rows, bool check_column_size = true) const;
    /// Evaluate defaulted columns if necessary.
    void evaluateMissingDefaults(Block additional_columns, Columns & res_columns) const;

    /// If part metadata is not equal to storage metadata, than
    /// try to perform conversions of columns.
    void performRequiredConversions(Columns & res_columns, bool check_column_size = true);

    const NamesAndTypesList & getColumns() const { return columns; }
    size_t numColumnsInResult() const { return hasBitmapIndexReader() ? columns.size() + getBitmapOutputColumns().size() : columns.size(); }

    size_t getFirstMarkToRead() const
    {
        return all_mark_ranges.front().begin;
    }

    bool hasBitmapIndexReader() const;

    const NameOrderedSet & getBitmapOutputColumns() const;

    const NamesAndTypesList & getBitmapColumns() const;

    MergeTreeData::DataPartPtr data_part;

    using FileStreams = std::map<std::string, std::unique_ptr<IMergeTreeReaderStream>>;
    using Serializations = std::map<std::string, SerializationPtr>;
protected:
    void prefetchForAllColumns(Priority priority, NamesAndTypesList & prefetch_columns, size_t from_mark, size_t current_task_last_mark, bool continue_reading);

    void prefetchForMapColumn(
        Priority priority,
        const NameAndTypePair & name_and_type,
        size_t from_mark,
        bool continue_reading,
        size_t current_task_last_mark);
    /// Make next readData more simple by calling 'prefetch' of all related ReadBuffers (column streams).
    void prefetchForColumn(
        Priority priority,
        const NameAndTypePair & name_and_type,
        const SerializationPtr & serialization,
        size_t from_mark,
        bool continue_reading,
        size_t current_task_last_mark,
        ISerialization::SubstreamsCache & cache);

    bool isLowCardinalityDictionary(const ISerialization::SubstreamPath & substream_path);
    /// Returns actual column type in part, which can differ from table metadata.
    NameAndTypePair getColumnFromPart(const NameAndTypePair & required_column);

    void checkNumberOfColumns(size_t num_columns_to_read) const;

    void addByteMapStreams(const NameAndTypePair & name_and_type, const String & col_name,
        const ReadBufferFromFileBase::ProfileCallback & profile_callback, clockid_t clock_type);

    void readMapDataNotKV(
        const NameAndTypePair & name_and_type, ColumnPtr & column,
        size_t from_mark, bool continue_reading, size_t current_task_last_mark, size_t max_rows_to_read,
        std::unordered_map<String, size_t> & res_col_to_idx, Columns & res_columns);

    size_t skipMapDataNotKV(
        const NameAndTypePair & name_and_type, size_t from_mark,
        bool continue_reading, size_t current_task_last_mark, size_t max_rows_to_skip);

    void readData(
        const NameAndTypePair & name_and_type, ColumnPtr & column,
        size_t from_mark, bool continue_reading, size_t current_task_last_mark,
        size_t max_rows_to_read, ISerialization::SubstreamsCache & cache);

    size_t skipData(
        const NameAndTypePair & name_and_type, size_t from_mark, bool continue_reading,
        size_t current_task_last_mark, size_t max_rows_to_skip, ISerialization::SubstreamsCache & cache);

    void deserializePrefix(
        const SerializationPtr & serialization,
        const NameAndTypePair & name_and_type,
        size_t current_task_last_mark,
        ISerialization::SubstreamsCache & cache);

    /// avg_value_size_hints are used to reduce the number of reallocations when creating columns of variable size.
    ValueSizeMap avg_value_size_hints;
    /// Stores states for IDataType::deserializeBinaryBulk
    DeserializeBinaryBulkStateMap deserialize_binary_bulk_state_map;

    /// Columns that are read.
    NamesAndTypesList columns;
    // Columns after sort
    NamesAndTypesList sort_columns;
    NamesAndTypesList part_columns;

    /// Map ColumnMap to its keys sub columns
    MapColumnKeys map_column_keys;
    std::set<String> dup_implicit_keys;
    Names names; // only initialized if duplicate implicit key exit

    /// Actual column names and types of columns in part,
    /// which may differ from table metadata.
    NamesAndTypes columns_to_read;
    
    UncompressedCache * uncompressed_cache;
    MarkCache * mark_cache;

    MergeTreeReaderSettings settings;

    const MergeTreeMetaBase & storage;
    StorageMetadataPtr metadata_snapshot;
    MarkRanges all_mark_ranges;

    using ColumnPosition = std::optional<size_t>;
    virtual ColumnPosition findColumnForOffsets(const String & column_name) const;

    FileStreams streams;
    Serializations serializations;

    friend class MergeTreeRangeReader::DelayedStream;

    MergeTreeIndexExecutor * index_executor = nullptr;

    std::unordered_map<String, ISerialization::SubstreamsCache> caches;
    std::unordered_set<std::string> prefetched_streams;
    /// Row number, update on seek or read, set to size_t max to force a seek
    /// when reader start reading
    size_t next_row_number_to_read = std::numeric_limits<size_t>::max();

private:
    NameAndTypePair columnTypeFromPart(const NameAndTypePair & required_column);

    /// Alter conversions, which must be applied on fly if required
    MergeTreeMetaBase::AlterConversions alter_conversions;

    /// Actual data type of columns in part
    google::dense_hash_map<StringRef, const DataTypePtr *, StringRefHash> columns_from_part;

    std::unordered_map<String, NameAndTypePair> columns_type_cache;
};

}
