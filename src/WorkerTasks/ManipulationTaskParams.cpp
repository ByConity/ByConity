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

#include <WorkerTasks/ManipulationTaskParams.h>

#include <sstream>
#include <Catalog/DataModelPartWrapper.h>
#include <CloudServices/CnchPartsHelper.h>
#include <Parsers/formatAST.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOG_ERROR;
    extern const int MERGE_BAD_PART_NAME;
}

String ManipulationTaskParams::toDebugString() const
{
    std::ostringstream oss;

    oss << "ManipulationTask {" << task_id << "}, type " << typeToString(type) << ", ";
    oss << storage->getStorageID().getNameForLogs() << ": ";
    if (!source_parts.empty())
    {
        oss << " source_parts: { " << source_parts.front()->name();
        for (size_t i = 1; i < source_parts.size(); ++i)
            oss << ", " << source_parts[i]->name();
        oss << " }";
    }
    if (!source_data_parts.empty())
    {
        oss << " source_parts: { " << source_data_parts.front()->name;
        for (size_t i = 1; i < source_data_parts.size(); ++i)
            oss << ", " << source_data_parts[i]->name;
        oss << " }";
    }
    if (!new_part_names.empty())
    {
        oss << " new_part_names: { " << new_part_names.front();
        for (size_t i = 1; i < new_part_names.size(); ++i)
            oss << ", " << new_part_names[i];
        oss << " }";
    }
    if (mutation_commands)
    {
        oss << " mutation_commands: " << serializeAST(*mutation_commands->ast());
    }

    if (columns_commit_time)
        oss << " columns_commit_time: " << columns_commit_time;

    if (mutation_commit_time)
        oss << " mutation_commit_time: " << mutation_commit_time;

    if (last_modification_time)
        oss << " last_modification_time: " << last_modification_time;

    return oss.str();
}

template <class Vec>
static String toPartNames(const Vec & parts)
{
    WriteBufferFromOwnString wb;
    for (const auto & p : parts)
        wb << p->get_name() << ", ";
    return wb.str();
}

template <class Vec>
void ManipulationTaskParams::calcNewPartNames(const Vec & parts, UInt64 ts)
{
    if (unlikely(type == Type::Empty))
        throw Exception("Expected non-empty manipulate type", ErrorCodes::LOGICAL_ERROR);

    if (parts.empty())
        return;

    if (ts)
    {
        MergeTreePartInfo part_info;
        part_info.partition_id = parts.front()->get_info().partition_id;
        part_info.min_block = ts;
        part_info.max_block = ts;
        part_info.level = 0;
        part_info.mutation = txn_id; 
        new_part_names.push_back(part_info.getPartName());
        return;
    }

    auto left = parts.begin();
    auto right = parts.begin();

    while (left != parts.end())
    {
        if (type == ManipulationType::Merge)
        {
            while (right != parts.end() && (*left)->get_partition().value == (*right)->get_partition().value)
                ++right;
        }
        else
        {
            ++right;
        }

        MergeTreePartInfo part_info;
        part_info.partition_id = (*left)->get_info().partition_id;
        part_info.min_block = (*left)->get_info().min_block;
        part_info.max_block = (*std::prev(right))->get_info().max_block;
        part_info.level = (*left)->get_info().level + 1;
        part_info.mutation = txn_id;

        for (auto it = left; it != right; ++it)
        {
            part_info.level = std::max(part_info.level, (*it)->get_info().level + 1);
        }

        /// If merged_part's name is same with some source part, it means there will be duplicate part names in result parts
        /// (merged_part and some tombstone part). It's undefined behavior when committing such parts to KV.
        /// So check the part name before executing.
        /// Skip the check for single-part merge (parts.size == 1), as it acquire new block id on worker.
        if (type == ManipulationType::Merge && parts.size() > 1)
        {
            for (const auto & p : parts)
            {
                if (p->get_info().min_block == part_info.min_block && p->get_info().max_block == part_info.max_block)
                    throw Exception(ErrorCodes::MERGE_BAD_PART_NAME,
                        "Merged part has the same part name with some source part: {}", toPartNames(parts));
            }
        }

        new_part_names.push_back(part_info.getPartName());

        left = right;
    }
}

/// For server (CnchMergeMutateThread)
void ManipulationTaskParams::assignSourceParts(ServerDataPartsVector parts)
{
    /// Make sure there are only visible parts when doing calculating new part names.
    calcNewPartNames(parts);
    /// Then, flatten parts so that the RPC request contains all parts.
    CnchPartsHelper::flattenPartsVector(parts);
    source_parts = std::move(parts);
}

/// For part merger
void ManipulationTaskParams::assignSourceParts(MergeTreeDataPartsVector parts)
{
    calcNewPartNames(parts);
    /// Do we need flatten parts for part merger?
    source_data_parts = std::move(parts);
}

/// For worker. The input parts are flattened.
void ManipulationTaskParams::assignParts(MergeTreeMutableDataPartsVector parts, const std::function<UInt64()> & ts_getter)
{
    for (auto & part: parts)
        all_parts.emplace_back(std::move(part));
    /// Make sure there are only visible parts when doing calculating new part names.
    source_data_parts = CnchPartsHelper::calcVisibleParts(all_parts, false);
    calcNewPartNames(source_data_parts, (source_data_parts.size() == 1 && type == Type::Merge) ? ts_getter() : 0);
}

}
