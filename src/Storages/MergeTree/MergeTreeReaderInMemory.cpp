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

#include <cstddef>
#include <Storages/MergeTree/MergeTreeReaderInMemory.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Interpreters/getColumnFromBlock.h>
#include <Common/ProfileEventsTimer.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/MapHelpers.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>

namespace ProfileEvents
{
    extern const Event SkipRowsTimeMicro;
    extern const Event ReadRowsTimeMicro;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int LOGICAL_ERROR;
}


MergeTreeReaderInMemory::MergeTreeReaderInMemory(
    DataPartInMemoryPtr data_part_,
    NamesAndTypesList columns_,
    const StorageMetadataPtr & metadata_snapshot_,
    MarkRanges mark_ranges_,
    MergeTreeReaderSettings settings_)
    : IMergeTreeReader(data_part_, std::move(columns_), metadata_snapshot_,
        nullptr, nullptr, std::move(mark_ranges_),
        std::move(settings_), {})
    , part_in_memory(std::move(data_part_))
{
    for (const auto & name_and_type : columns)
    {
        auto [name, type] = getColumnFromPart(name_and_type);
        if (name == "_part_row_number")
            continue;

        /// If array of Nested column is missing in part,
        /// we have to read its offsets if they exist.
        if (!part_in_memory->block.has(name) && typeid_cast<const DataTypeArray *>(type.get()))
            if (auto offset_position = findColumnForOffsets(name))
                positions_for_offsets[name] = *offset_position;
    }
}

size_t MergeTreeReaderInMemory::readRows(size_t from_mark, size_t from_row,
    size_t max_rows_to_read, size_t, const UInt8* filter, Columns & res_columns)
{
    size_t from_mark_start_row = data_part->index_granularity.getMarkStartingRow(
        from_mark);
    size_t starting_row = from_mark_start_row + from_row;

    size_t rows_to_skip = from_row;
    bool adjacent_reading = next_row_number_to_read >= from_mark_start_row
        && starting_row >= next_row_number_to_read;
    if (adjacent_reading)
    {
        rows_to_skip = starting_row - next_row_number_to_read;
    }

    size_t skipped_rows = 0;
    if (rows_to_skip > 0)
    {
        ProfileEventsTimer timer(ProfileEvents::SkipRowsTimeMicro);

        Columns tmp_columns(columns.size(), nullptr);
        auto column_iter = columns.begin();
        for (size_t i = 0; i < columns.size(); ++i)
        {
            const auto& [name, type] = getColumnFromPart(*column_iter);
            tmp_columns[i] = type->createColumn();
        }

        skipped_rows = resumableReadRows(from_mark, adjacent_reading, rows_to_skip,
            nullptr, tmp_columns);
    }

    next_row_number_to_read += skipped_rows;

    size_t read_rows = 0;
    if (skipped_rows >= rows_to_skip)
    {
        ProfileEventsTimer timer(ProfileEvents::ReadRowsTimeMicro);

        adjacent_reading = rows_to_skip > 0 || starting_row == next_row_number_to_read;

        read_rows = resumableReadRows(from_mark, adjacent_reading, max_rows_to_read,
            filter + skipped_rows, res_columns);
    }
    return read_rows;
}

size_t MergeTreeReaderInMemory::resumableReadRows(size_t from_mark, bool continue_reading,
    size_t max_rows_to_read, const UInt8* filter, Columns & res_columns)
{
    if (filter != nullptr)
    {
        /// NOTE: MergeTreeReaderInMemory is already removed in community's code
        throw Exception("MergeTreeReaderInMemory didn't support read filter yet",
            ErrorCodes::NOT_IMPLEMENTED);
    }

    if (!continue_reading)
    {
        total_rows_read = 0;
        next_row_number_to_read = data_part->index_granularity.getMarkStartingRow(from_mark);
    }

    size_t total_marks = data_part->index_granularity.getMarksCount();
    if (from_mark >= total_marks)
        throw Exception("Mark " + toString(from_mark) + " is out of bound. Max mark: "
            + toString(total_marks), ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    size_t num_columns = res_columns.size();
    checkNumberOfColumns(num_columns);

    size_t part_rows = part_in_memory->block.rows();
    if (total_rows_read >= part_rows)
        throw Exception("Cannot read data in MergeTreeReaderInMemory. Rows already read: "
            + toString(total_rows_read) + ". Rows in part: " + toString(part_rows), ErrorCodes::CANNOT_READ_ALL_DATA);

    size_t rows_to_read = std::min(max_rows_to_read, part_rows - total_rows_read);
    auto column_it = columns.begin();
    for (size_t i = 0; i < num_columns; ++i, ++column_it)
    {
        auto name_type = getColumnFromPart(*column_it);

        /// Copy offsets, if array of Nested column is missing in part.
        auto offsets_it = positions_for_offsets.find(name_type.name);
        if (offsets_it != positions_for_offsets.end() && !name_type.isSubcolumn())
        {
            const auto & source_offsets = assert_cast<const ColumnArray &>(
                *part_in_memory->block.getByPosition(offsets_it->second).column).getOffsets();

            if (res_columns[i] == nullptr)
                res_columns[i] = name_type.type->createColumn();

            auto mutable_column = res_columns[i]->assumeMutable();
            auto & res_offstes = assert_cast<ColumnArray &>(*mutable_column).getOffsets();
            size_t start_offset = total_rows_read ? source_offsets[total_rows_read - 1] : 0;
            for (size_t row = 0; row < rows_to_read; ++row)
                res_offstes.push_back(source_offsets[total_rows_read + row] - start_offset);

            res_columns[i] = std::move(mutable_column);
        }
        else if (part_in_memory->hasColumnFiles(name_type))
        {
            auto block_column = getColumnFromBlock(part_in_memory->block, name_type);
            if (rows_to_read == part_rows)
            {
                res_columns[i] = block_column;
            }
            else
            {
                if (res_columns[i] == nullptr)
                    res_columns[i] = name_type.type->createColumn();

                auto mutable_column = res_columns[i]->assumeMutable();
                mutable_column->insertRangeFrom(*block_column, total_rows_read, rows_to_read);
                res_columns[i] = std::move(mutable_column);
            }
        }
        else if (isMapImplicitKey(name_type.name)) /// handle implicit key
        {
            auto & part_all_columns = part_in_memory->getColumns();
            auto map_column_name = parseMapNameFromImplicitColName(name_type.name);
            auto key_name = parseKeyNameFromImplicitColName(name_type.name, map_column_name);
            auto map_column = std::find_if(part_all_columns.begin(), part_all_columns.end(), [&](auto & val) { return val.name == map_column_name; });
            if (map_column != part_all_columns.end())
            {
                auto block_column = getColumnFromBlock(part_in_memory->block, *map_column);
                const auto & column_map = typeid_cast<const ColumnMap &>(*block_column);

                /// Attention: key_name has been quoted, we need to remove the quote.
                if (auto col = part_columns.tryGetByName(map_column_name); !col.has_value() || !col->type->isByteMap())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Map column '{}' doesn't exist", map_column_name);
                else
                {
                    auto const & map_key_type = dynamic_cast<const DataTypeMap *>(col->type.get())->getKeyType();
                    key_name = map_key_type->stringToVisitorString(key_name);
                }
                res_columns[i] = column_map.getValueColumnByKey(key_name, rows_to_read);
            }
        }
        else if (name_type.name == "_part_row_number")
        {
            if (res_columns[i] == nullptr)
                res_columns[i] = name_type.type->createColumn();

            auto mutable_column = res_columns[i]->assumeMutable();
            auto & column = assert_cast<ColumnUInt64 &>(*mutable_column);
            size_t row_number = total_rows_read;
            size_t row_number_end = row_number + rows_to_read;
            while (row_number < row_number_end)
                 column.insertValue(row_number++);
            res_columns[i] = std::move(mutable_column);
        }
    }

    total_rows_read += rows_to_read;
    next_row_number_to_read += rows_to_read;
    return rows_to_read;
}

}
