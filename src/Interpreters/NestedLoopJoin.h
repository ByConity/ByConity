/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <Common/Logger.h>
#include <memory>
#include <shared_mutex>

#include <Poco/Logger.h>
#include <Common/LRUCache.h>
#include <Common/filesystemHelpers.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <Columns/FilterDescription.h>
#include <Interpreters/IJoin.h>
#include <DataStreams/SizeLimits.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>

namespace Poco { class Logger; }

namespace DB
{

class TableJoin;

struct BlockFilterDescriptions
{
    std::vector<int> block_indexes;
    std::vector<Block> blocks;
    std::vector<std::shared_ptr<FilterDescription>> holders;
    std::vector<size_t> filtered_size;
    int total_filtered_size = 0;

    void add(int index, Block _block, std::shared_ptr<FilterDescription> holder, int _filtered_size)
    {
        block_indexes.push_back(index);
        blocks.emplace_back(_block);
        holders.emplace_back(holder);
        filtered_size.push_back(_filtered_size);
        total_filtered_size += _filtered_size;
    }

    std::shared_ptr<FilterDescription> getHolderByIndex(int index)
    {
        return holders.at(index);
    }

    int getFilteredSizeByIndex(int index)
    {
        return filtered_size.at(index);
    }

    Block getBlockByIndex(int index)
    {
        return blocks.at(index);
    }

    int getTotalFilteredSize()
    {
        return total_filtered_size;
    }
};

class NestedLoopJoin : public IJoin
{
public:
    NestedLoopJoin(std::shared_ptr<TableJoin> table_join_, const Block & right_sample_block_, const ContextPtr& context);

    JoinType getType() const override { return JoinType::NestedLoop; }

    bool addJoinedBlock(const Block & block, bool check_limits = true) override;
    void joinBlock(Block &, ExtraBlockPtr & not_processed) override;
    void setTotals(const Block &) override;
    const Block & getTotals() const override;
    const TableJoin & getTableJoin() const override { return *table_join; }
    TableJoin & getTableJoin() override { return *table_join; }

    size_t getTotalRowCount() const override;
    size_t getTotalByteCount() const override;

private:
    LoggerPtr log = getLogger("NestedLoopJoin");

    using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;
    std::shared_ptr<TableJoin> table_join;
    Block right_sample_block;
    SizeLimits size_limits;
    const bool nullable_right_side;
    const bool is_any_join;
    const bool is_left;
    const size_t max_rows_in_right_block;
    ContextPtr context;
    Block right_table_keys;
    Block right_columns_to_add;
    Block header;
    mutable std::shared_mutex rwlock;
    Block totals;
    BlocksList right_blocks;

    bool saveRightBlock(Block && block);

    void paddingRightBlockWithConstColumn(Block &left_block, size_t left_row_index, Block &right_block) const;

    bool isConstFromLeftTable(const ColumnWithTypeAndName & rightCol, const std::unordered_set<std::string> & left_column_names);

    void joinImpl(
        const ExpressionActionsPtr & actions,
        const String & filter_name,
        Block & left_block);

    void completeHeader(Block & left_block);

    void completeColumnsAfterJoin(NamesAndTypesList & total_columns);

};

}
