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
#include "config_formats.h"

#if USE_ARROW || USE_ORC || USE_PARQUET

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <arrow/type.h>
#include <Columns/ColumnVector.h>
#include <arrow/table.h>
#include <arrow/array.h>
#include <arrow/buffer.h>
#include <Processors/Chunk.h>
#include <Core/Block.h>
#include <Formats/FormatSettings.h>


namespace DB
{

class ArrowColumnToCHColumn
{
public:
    using NameToColumnPtr = std::unordered_map<std::string, std::shared_ptr<arrow::ChunkedArray>>;

    ArrowColumnToCHColumn(
        const Block & header_,
        const std::string & format_name_,
        bool import_nested_,
        bool allow_missing_columns_,
        bool null_as_default_,
        FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior_,
        bool case_insensitive_matching_ = false);

    void arrowTableToCHChunk(Chunk & res, std::shared_ptr<arrow::Table> & table, size_t num_rows, BlockMissingValues * block_missing_values = nullptr);

    void arrowColumnsToCHChunk(Chunk & res, NameToColumnPtr & name_to_column_ptr, size_t num_rows, BlockMissingValues * block_missing_values = nullptr);

    /// Transform arrow schema to ClickHouse header. If hint_header is provided,
    /// we will skip columns in schema that are not in hint_header.
    static Block arrowSchemaToCHHeader(
        const arrow::Schema & schema,
        const std::string & format_name,
        bool skip_columns_with_unsupported_types = false,
        const Block * hint_header = nullptr,
        bool ignore_case = false);

    struct DictionaryInfo
    {
        std::shared_ptr<ColumnWithTypeAndName> values;
        Int64 default_value_index = -1;
        UInt64 dictionary_size;
    };

private:
    struct ArrowColumn
    {
        std::shared_ptr<arrow::ChunkedArray> column;
        std::shared_ptr<arrow::Field> field;
    };

    using NameToArrowColumn = std::unordered_map<std::string, ArrowColumn>;
    void arrowColumnsToCHChunk(Chunk & res, const NameToArrowColumn & name_to_arrow_column, size_t num_rows, BlockMissingValues * block_missing_values = nullptr);

    static ColumnPtr castArrayColumnToBitmapColumn(ColumnWithTypeAndName & column, const DataTypePtr & target_type);

    const Block & header;
    const std::string format_name;
    bool import_nested;
    /// If false, throw exception if some columns in header not exists in arrow table.
    bool allow_missing_columns;
    bool null_as_default;
    FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior;
    bool case_insensitive_matching;

    /// Map {column name : dictionary column}.
    /// To avoid converting dictionary from Arrow Dictionary
    /// to LowCardinality every chunk we save it and reuse.
    std::unordered_map<std::string, DictionaryInfo> dictionary_infos;
};
}
#endif
