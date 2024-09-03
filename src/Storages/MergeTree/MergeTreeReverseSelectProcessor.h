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
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeThreadSelectBlockInputProcessor.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{


/// Used to read data from single part with select query
/// Cares about PREWHERE, virtual columns, indexes etc.
/// To read data from multiple parts, Storage (MergeTree) creates multiple such objects.
class MergeTreeReverseSelectProcessor : public MergeTreeSelectProcessor
{
public:
    template <typename... Args>
    explicit MergeTreeReverseSelectProcessor(Args &&... args)
        : MergeTreeSelectProcessor{std::forward<Args>(args)...}
    {
//        LOG_TRACE(log, "Reading {} ranges in reverse order from part {}, approx. {} rows starting from {}",
//                  part_detail.ranges.size(), part_detail.data_part->name, total_rows,
//                  part_detail.data_part->index_granularity.getMarkStartingRow(part_detail.ranges.front().begin));
    }

    ~MergeTreeReverseSelectProcessor() override;

    String getName() const override { return "MergeTreeReverse"; }

    /// Closes readers and unlock part locks
    void finish();

protected:

    bool getNewTask() override;
    Chunk readFromPart() override;

private:
    Chunks chunks;
};

}
