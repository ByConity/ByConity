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

#include <Processors/Chunk.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <Columns/ColumnConst.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int POSITION_OUT_OF_BOUND;
}

Chunk::Chunk(DB::Columns columns_, UInt64 num_rows_) : columns(std::move(columns_)), num_rows(num_rows_)
{
    checkNumRowsIsConsistent();
}

Chunk::Chunk(Columns columns_, UInt64 num_rows_, ChunkInfoPtr chunk_info_)
    : columns(std::move(columns_)), num_rows(num_rows_), chunk_info(std::move(chunk_info_))
{
    checkNumRowsIsConsistent();
}

Chunk & Chunk::operator=(Chunk && other) noexcept
{
    columns = std::move(other.columns);
    chunk_info = std::move(other.chunk_info);
    if (other.owned_side_block && other.owned_side_block->columns() > 0)
        owned_side_block = std::move(other.owned_side_block);
    num_rows = other.num_rows;
    other.num_rows = 0;
    owner_info = other.owner_info;
    other.owner_info.reset();
    return *this;
}

void Chunk::swap(Chunk & other)
{
    columns.swap(other.columns);
    chunk_info.swap(other.chunk_info);
    owned_side_block.swap(other.owned_side_block);
    std::swap(num_rows, other.num_rows);
    std::swap(owner_info, other.owner_info);
}

void Chunk::clear()
{
    num_rows = 0;
    columns.clear();
    chunk_info.reset();
    owned_side_block.reset();
    owner_info.reset();
}

static Columns unmuteColumns(MutableColumns && mut_columns)
{
    Columns columns;
    columns.reserve(mut_columns.size());
    for (auto & col : mut_columns)
        columns.emplace_back(std::move(col));

    return columns;
}

Chunk::Chunk(MutableColumns columns_, UInt64 num_rows_)
    : columns(unmuteColumns(std::move(columns_))), num_rows(num_rows_)
{
    checkNumRowsIsConsistent();
}

Chunk::Chunk(MutableColumns columns_, UInt64 num_rows_, ChunkInfoPtr chunk_info_)
    : columns(unmuteColumns(std::move(columns_))), num_rows(num_rows_), chunk_info(std::move(chunk_info_))
{
    checkNumRowsIsConsistent();
}

Chunk Chunk::clone() const
{
    Chunk res(getColumns(), getNumRows(), chunk_info);
    if (owned_side_block && owned_side_block->columns() > 0)
    for (auto column : *owned_side_block)
        res.addColumnToSideBlock(std::move(column));
    res.owner_info = owner_info;
    return res;
}

void Chunk::setColumns(Columns columns_, UInt64 num_rows_)
{
    columns = std::move(columns_);
    num_rows = num_rows_;
    checkNumRowsIsConsistent();
}

void Chunk::setColumns(MutableColumns columns_, UInt64 num_rows_)
{
    columns = unmuteColumns(std::move(columns_));
    num_rows = num_rows_;
    checkNumRowsIsConsistent();
}

void Chunk::checkNumRowsIsConsistent()
{
    for (size_t i = 0; i < columns.size(); ++i)
    {
        auto & column = columns[i];
        if (column->size() != num_rows)
            throw Exception("Invalid number of rows in Chunk column " + column->getName()+ " position " + toString(i) + ": expected " +
                            toString(num_rows) + ", got " + toString(column->size()), ErrorCodes::LOGICAL_ERROR);
    }
}

MutableColumns Chunk::mutateColumns()
{
    size_t num_columns = columns.size();
    MutableColumns mut_columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        mut_columns[i] = IColumn::mutate(std::move(columns[i]));

    columns.clear();
    num_rows = 0;

    return mut_columns;
}

MutableColumns Chunk::cloneEmptyColumns() const
{
    size_t num_columns = columns.size();
    MutableColumns mut_columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        mut_columns[i] = columns[i]->cloneEmpty();
    return mut_columns;
}

Columns Chunk::detachColumns()
{
    num_rows = 0;
    return std::move(columns);
}

void Chunk::addColumn(ColumnPtr column)
{
    if (empty())
        num_rows = column->size();
    else if (column->size() != num_rows)
        throw Exception("Invalid number of rows in Chunk column " + column->getName()+ ": expected " +
                        toString(num_rows) + ", got " + toString(column->size()), ErrorCodes::LOGICAL_ERROR);

    columns.emplace_back(std::move(column));
}

void Chunk::addColumn(size_t position, ColumnPtr column)
{
    if (position >= columns.size())
        throw Exception(ErrorCodes::POSITION_OUT_OF_BOUND,
                        "Position {} out of bound in Chunk::addColumn(), max position = {}",
                        position, columns.size() - 1);
    if (empty())
        num_rows = column->size();
    else if (column->size() != num_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Invalid number of rows in Chunk column {}: expected {}, got {}",
                        column->getName(), num_rows, column->size());

    columns.emplace(columns.begin() + position, std::move(column));
}

void Chunk::erase(size_t position)
{
    if (columns.empty())
        throw Exception("Chunk is empty", ErrorCodes::POSITION_OUT_OF_BOUND);

    if (position >= columns.size())
        throw Exception("Position " + toString(position) + " out of bound in Chunk::erase(), max position = "
                        + toString(columns.size() - 1), ErrorCodes::POSITION_OUT_OF_BOUND);

    columns.erase(columns.begin() + position);
}

UInt64 Chunk::bytes() const
{
    UInt64 res = 0;
    for (const auto & column : columns)
        res += column->byteSize();

    return res;
}

UInt64 Chunk::allocatedBytes() const
{
    UInt64 res = 0;
    for (const auto & column : columns)
        res += column->allocatedBytes();

    return res;
}

std::string Chunk::dumpStructure() const
{
    WriteBufferFromOwnString out;
    for (const auto & column : columns)
        out << ' ' << column->dumpStructure();

    return out.str();
}

void Chunk::append(const Chunk & chunk)
{
    append(chunk, 0, chunk.getNumRows());
}

void Chunk::append(const Chunk & chunk, size_t from, size_t length)
{
    MutableColumns mutable_columns = mutateColumns();
    for (size_t position = 0; position < mutable_columns.size(); ++position)
    {
        auto column = chunk.getColumns()[position];
        mutable_columns[position]->insertRangeFrom(*column, from, length);
    }
    size_t rows = mutable_columns[0]->size();
    setColumns(std::move(mutable_columns), rows);
}

void ChunkMissingValues::setBit(size_t column_idx, size_t row_idx)
{
    RowsBitMask & mask = rows_mask_by_column_id[column_idx];
    mask.resize(row_idx + 1);
    mask[row_idx] = true;
}

const ChunkMissingValues::RowsBitMask & ChunkMissingValues::getDefaultsBitmask(size_t column_idx) const
{
    static RowsBitMask none;
    auto it = rows_mask_by_column_id.find(column_idx);
    if (it != rows_mask_by_column_id.end())
        return it->second;
    return none;
}

void Chunk::addColumnToSideBlock(ColumnWithTypeAndName && col)
{
    if (!owned_side_block)
        owned_side_block = std::make_unique<Block>();
    if (!owned_side_block->has(col.name))
        owned_side_block->insert(std::move(col));
}

const OwnerInfo & Chunk::getOwnerInfo() const
{
    return owner_info;
}

void Chunk::setOwnerInfo(OwnerInfo owner_info_)
{
    owner_info = std::move(owner_info_);
}

Chunk cloneConstWithDefault(const Chunk & chunk, size_t num_rows)
{
    auto columns = chunk.cloneEmptyColumns();
    for (auto & column : columns)
    {
        column->insertDefault();
        column = ColumnConst::create(std::move(column), num_rows);
    }

    return Chunk(std::move(columns), num_rows);
}

}
