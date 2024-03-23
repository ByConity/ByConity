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

#include <Processors/ISource.h>
#include "Interpreters/Context_fwd.h"

#include <memory>


namespace DB
{

struct SelectQueryInfo;

/// Used to pass info from header between different InputFormats in ParallelParsing
struct ColumnMapping
{
    /// Non-atomic because there is strict `happens-before` between read and write access
    /// See InputFormatParallelParsing
    bool is_set{false};
    /// Maps indexes of columns in the input file to indexes of table columns
    using OptionalIndexes = std::vector<std::optional<size_t>>;
    OptionalIndexes column_indexes_for_input_fields;

    /// Tracks which columns we have read in a single read() call.
    /// For columns that are never read, it is initialized to false when we
    /// read the file header, and never changed afterwards.
    /// For other columns, it is updated on each read() call.
    std::vector<UInt8> read_columns;


    /// Whether we have any columns that are not read from file at all,
    /// and must be always initialized with defaults.
    bool have_always_default_columns{false};
};

using ColumnMappingPtr = std::shared_ptr<ColumnMapping>;

class ReadBuffer;

/** Input format is a source, that reads data from ReadBuffer.
  */
class IInputFormat : public ISource
{
protected:

    /// Skip GCC warning: ‘maybe_unused’ attribute ignored
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wattributes"

    ReadBuffer & in [[maybe_unused]];

#pragma GCC diagnostic pop

public:
    IInputFormat(Block header, ReadBuffer & in_);

    /// If the format is used by a SELECT query, this method may be called.
    /// The format may use it for filter pushdown.
    virtual void setQueryInfo(const SelectQueryInfo &, ContextPtr) {}

    /** In some usecase (hello Kafka) we need to read a lot of tiny streams in exactly the same format.
     * The recreating of parser for each small stream takes too long, so we introduce a method
     * resetParser() which allow to reset the state of parser to continue reading of
     * source stream w/o recreating that.
     * That should be called after current buffer was fully read.
     */
    virtual void resetParser();

    virtual void setReadBuffer(ReadBuffer & in_);

    virtual const BlockMissingValues & getMissingValues() const
    {
        static const BlockMissingValues none;
        return none;
    }

    /// Must be called from ParallelParsingInputFormat after readSuffix
    ColumnMappingPtr getColumnMapping() const { return column_mapping; }
    /// Must be called from ParallelParsingInputFormat before readPrefix
    void setColumnMapping(ColumnMappingPtr column_mapping_) { column_mapping = column_mapping_; }

    size_t getCurrentUnitNumber() const { return current_unit_number; }
    void setCurrentUnitNumber(size_t current_unit_number_) { current_unit_number = current_unit_number_; }

    void addBuffer(std::unique_ptr<ReadBuffer> buffer) { owned_buffers.emplace_back(std::move(buffer)); }

protected:
    ColumnMappingPtr column_mapping{};

private:
    /// Number of currently parsed chunk (if parallel parsing is enabled)
    size_t current_unit_number = 0;

    std::vector<std::unique_ptr<ReadBuffer>> owned_buffers;
};

}
