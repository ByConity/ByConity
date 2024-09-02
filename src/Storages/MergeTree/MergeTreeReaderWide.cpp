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

#include <Storages/MergeTree/MergeTreeReaderWide.h>

#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/MapHelpers.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeDataPartWide.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <Common/ProfileEventsTimer.h>
#include <unordered_map>
#include <utility>

namespace ProfileEvents
{
    extern const Event SkipRowsTimeMicro;
    extern const Event ReadRowsTimeMicro;
}

namespace DB
{

namespace
{
    using OffsetColumns = std::map<std::string, ColumnPtr>;
}

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

MergeTreeReaderWide::MergeTreeReaderWide(
    MergeTreeMetaBase::DataPartPtr data_part_,
    NamesAndTypesList columns_,
    const StorageMetadataPtr & metadata_snapshot_,
    UncompressedCache * uncompressed_cache_,
    MarkCache * mark_cache_,
    MarkRanges mark_ranges_,
    MergeTreeReaderSettings settings_,
    MergeTreeIndexExecutor * index_executor_,
    IMergeTreeDataPart::ValueSizeMap avg_value_size_hints_,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback_,
    clockid_t clock_type_,
    bool create_streams_)
    : IMergeTreeReader(
        std::move(data_part_),
        std::move(columns_),
        metadata_snapshot_,
        uncompressed_cache_,
        std::move(mark_cache_),
        std::move(mark_ranges_),
        std::move(settings_),
        std::move(avg_value_size_hints_),
        index_executor_)
{
    if (!create_streams_)
    {
        return;
    }

    try
    {
        for (const NameAndTypePair & column : columns)
        {
            auto column_from_part = getColumnFromPart(column);
            if (column_from_part.type->isByteMap())
            {
                // Scan the directory to get all implicit columns(stream) for the map type
                const DataTypeMap & type_map = typeid_cast<const DataTypeMap &>(*column_from_part.type);

                String key_name;
                String impl_key_name;
                {
                    for (auto & file : data_part->getChecksums()->files)
                    {
                        // Try to get keys, and form the stream, its bin file name looks like "NAME__xxxxx.bin"
                        const String & file_name = file.first;
                        if (isMapImplicitDataFileNameNotBaseOfSpecialMapName(file_name, column.name))
                        {
                            key_name = parseKeyNameFromImplicitFileName(file_name, column.name);
                            impl_key_name = getImplicitColNameForMapKey(column.name, key_name);
                            // Special handing if implicit key is referenced too
                            if (columns.contains(impl_key_name))
                            {
                                dup_implicit_keys.insert(impl_key_name);
                            }

                            addByteMapStreams(
                                {impl_key_name, type_map.getValueTypeForImplicitColumn()}, column.name, profile_callback_, clock_type_);
                            map_column_keys.insert({column.name, key_name});
                        }
                    }
                }
            }
            else if (isMapImplicitKey(column.name)) // check if it's an implicit key and not KV
            {
                addByteMapStreams({column.name, column.type}, parseMapNameFromImplicitColName(column.name), profile_callback_, clock_type_);
            }
            else if (column.name != "_part_row_number")
            {
                addStreams(column_from_part, profile_callback_, clock_type_);
            }
        }

        if (!dup_implicit_keys.empty())
            names = columns.getNames();
    }
    catch (...)
    {
        storage.reportBrokenPart(data_part->name);
        throw;
    }
}

size_t MergeTreeReaderWide::readRows(size_t from_mark, size_t current_task_last_mark, size_t from_row,
    size_t max_rows_to_read, Columns& res_columns)
{
    try
    {
        size_t num_columns = columns.size();
        checkNumberOfColumns(num_columns);

        std::unordered_map<String, size_t> res_col_to_idx;
        auto column_it = columns.begin();
        for (size_t i = 0; i < num_columns; ++i, ++column_it)
        {
            const auto & [name, type] = getColumnFromPart(*column_it);
            res_col_to_idx[name] = i;
        }

        sort_columns = columns;
        if (!dup_implicit_keys.empty())
            sort_columns.sort([](const auto & lhs, const auto & rhs) { return (!lhs.type->isMap()) && rhs.type->isMap(); });

        size_t from_mark_start_row = data_part->index_granularity.getMarkStartingRow(
            from_mark);
        size_t starting_row = from_mark_start_row + from_row;
        size_t init_row_number = next_row_number_to_read;
        bool adjacent_reading = next_row_number_to_read >= from_mark_start_row
            && starting_row >= next_row_number_to_read;
        size_t rows_to_skip = adjacent_reading ? starting_row - next_row_number_to_read
            : from_row;

        if (!adjacent_reading)
        {
            next_row_number_to_read = from_mark_start_row;
        }

        size_t skipped_rows = skipUnnecessaryRows(num_columns, from_mark, adjacent_reading,
            current_task_last_mark, rows_to_skip);
        next_row_number_to_read += skipped_rows;

        size_t read_rows = 0;
        if (skipped_rows >= rows_to_skip)
        {
            adjacent_reading = rows_to_skip > 0 || init_row_number == starting_row;
            read_rows = readNecessaryRows(num_columns, from_mark, adjacent_reading,
                current_task_last_mark, max_rows_to_read, res_col_to_idx, res_columns);
            next_row_number_to_read += read_rows;
        }

        return read_rows;
    }
    catch (Exception & e)
    {
        if (e.code() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
            storage.reportBrokenPart(data_part->name);

        /// Better diagnostics.
        e.addMessage("(while reading from part " + data_part->getFullPath() + " "
                     "from mark " + toString(from_mark) + " "
                     "with max_rows_to_read = " + toString(max_rows_to_read) + ")");
        throw;
    }
    catch (...)
    {
        storage.reportBrokenPart(data_part->name);

        throw;
    }
}

size_t MergeTreeReaderWide::skipUnnecessaryRows(size_t num_columns, size_t from_mark, bool continue_reading,
    size_t current_task_last_mark, size_t rows_to_skip)
{
    if (rows_to_skip <= 0)
        return 0;

    ProfileEventsTimer timer(ProfileEvents::SkipRowsTimeMicro);

    size_t skipped_rows = 0;
    auto name_and_type = sort_columns.begin();
    for (size_t i = 0; i < num_columns; ++i, ++name_and_type)
    {
        auto column_from_part = getColumnFromPart(*name_and_type);
        const auto& [name, type] = column_from_part;
        
        if (name == "_part_row_number")
        {
            skipped_rows = std::max(skipped_rows, std::min(rows_to_skip, data_part->rows_count - next_row_number_to_read));
            continue;
        }

        try
        {
            auto& cache = caches[column_from_part.getNameInStorage()];
            if (type->isByteMap())
                skipped_rows = std::max(skipped_rows,
                    skipMapDataNotKV(column_from_part, from_mark, continue_reading,
                        current_task_last_mark, rows_to_skip));
            else
                skipped_rows = std::max(skipped_rows,
                    skipData(column_from_part, from_mark, continue_reading,
                        current_task_last_mark, rows_to_skip, cache));
        }
        catch (Exception& e)
        {
            /// Better diagnostics.
            e.addMessage("(while reading column " + name + ")");
            throw;
        }
    }
    caches.clear();

    return skipped_rows;
}

size_t MergeTreeReaderWide::readNecessaryRows(size_t num_columns, size_t from_mark, bool continue_reading,
    size_t current_task_last_mark, size_t rows_to_read,
    std::unordered_map<String, size_t>& res_col_to_idx, Columns& res_columns)
{
    if (rows_to_read <= 0)
        return 0;

    ProfileEventsTimer timer(ProfileEvents::ReadRowsTimeMicro);

    size_t read_rows = 0;
    int row_number_column_pos = -1;
    auto name_and_type = sort_columns.begin();
    for (size_t i = 0; i < num_columns; ++i, ++name_and_type)
    {
        auto column_from_part = getColumnFromPart(*name_and_type);
        const auto& [name, type] = column_from_part;
        size_t pos = res_col_to_idx[name];

        if (!res_columns[pos]) {
            type->enable_zero_cpy_read = this->settings.read_settings.zero_copy_read_from_cache;
            res_columns[pos] = type->createColumn();
        }

        /// row number column will be populated at last after `read_rows` is set
        if (name == "_part_row_number")
        {
            row_number_column_pos = pos;
            continue;
        }

        auto& column = res_columns[pos];
        try
        {
            size_t column_size_before_reading = column->size();
            auto& cache = caches[column_from_part.getNameInStorage()];
            if (type->isByteMap())
                readMapDataNotKV(column_from_part, column, from_mark, continue_reading,
                    current_task_last_mark, rows_to_read, res_col_to_idx, res_columns);
            else
                readData(column_from_part, column, from_mark, continue_reading,
                   current_task_last_mark, rows_to_read, cache);

            /// For elements of Nested, column_size_before_reading may be greater than column size
            ///  if offsets are not empty and were already read, but elements are empty.
            if (!column->empty())
                read_rows = std::max(read_rows, column->size() - column_size_before_reading);
        }
        catch (Exception& e)
        {
            /// Better diagnostics.
            e.addMessage("(while reading column " + name + ")");
            throw;
        }

        if (column->empty())
            res_columns[pos] = nullptr;
    }
    caches.clear();

    /// Populate _part_row_number column if requested
    if (row_number_column_pos >= 0)
    {
        /// update `read_rows` if no physical columns are read (only _part_row_number is requested)
        if (columns.size() == 1)
        {
            read_rows = std::min(rows_to_read, data_part->rows_count - next_row_number_to_read);
        }

        if (read_rows)
        {
            auto mutable_column = res_columns[row_number_column_pos]->assumeMutable();
            ColumnUInt64 & column = assert_cast<ColumnUInt64 &>(*mutable_column);
            for (size_t i = 0, row_number = next_row_number_to_read; i < read_rows; ++i)
                column.insertValue(row_number++);
            res_columns[row_number_column_pos] = std::move(mutable_column);
        }
        else
        {
            res_columns[row_number_column_pos] = nullptr;
        }
    }

    return read_rows;
}

void MergeTreeReaderWide::addStreams(const NameAndTypePair & name_and_type,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback, clockid_t clock_type)
{
    ISerialization::StreamCallback callback = [&](const ISerialization::SubstreamPath & substream_path) {
        String stream_name = ISerialization::getFileNameForStream(name_and_type, substream_path);

        if (streams.count(stream_name))
            return;

        auto check_validity
            = [&](String & stream_name_) -> bool { return data_part->getChecksums()->files.count(stream_name_ + DATA_FILE_EXTENSION); };

        /** If data file is missing then we will not try to open it.
          * It is necessary since it allows to add new column to structure of the table without creating new files for old parts.
          */
        if ((!name_and_type.type->isKVMap() && !check_validity(stream_name))
            || (name_and_type.type->isKVMap() && !tryConvertToValidKVStreamName(stream_name, check_validity)))
            return;

        bool is_lc_dict = isLowCardinalityDictionary(substream_path);
        streams.emplace(
            stream_name,
            std::make_unique<MergeTreeReaderStream>(
                IMergeTreeReaderStream::StreamFileMeta {
                    .disk = data_part->volume->getDisk(),
                    .rel_path = data_part->getFullRelativePath() + stream_name + DATA_FILE_EXTENSION,
                    .offset = data_part->getFileOffsetOrZero(stream_name + DATA_FILE_EXTENSION),
                    .size = data_part->getFileSizeOrZero(stream_name + DATA_FILE_EXTENSION),
                },
                IMergeTreeReaderStream::StreamFileMeta {
                    .disk = data_part->volume->getDisk(),
                    .rel_path = data_part->index_granularity_info.getMarksFilePath(data_part->getFullRelativePath() + stream_name),
                    .offset = data_part->getFileOffsetOrZero(data_part->index_granularity_info.getMarksFilePath(stream_name)),
                    .size = data_part->getFileSizeOrZero(data_part->index_granularity_info.getMarksFilePath(stream_name)),
                },
                stream_name,
                data_part->getMarksCount(),
                all_mark_ranges,
                settings,
                mark_cache,
                uncompressed_cache,
                &data_part->index_granularity_info,
                profile_callback,
                clock_type,
                is_lc_dict
            )
        );
    };

    auto serialization = data_part->getSerializationForColumn(name_and_type);
    serialization->enumerateStreams(callback);
    serializations.emplace(name_and_type.name, std::move(serialization));
}

}
