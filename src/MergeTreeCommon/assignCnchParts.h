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

#include <vector>
#include <Catalog/DataModelPartWrapper_fwd.h>
#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/SourceTask.h>
#include <Interpreters/WorkerGroupHandle.h>
#include <Storages/DataPart_fwd.h>
#include <Storages/Hive/HiveFile/IHiveFile_fwd.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h>
#include <Common/ConsistentHashUtils/ConsistentHashRing.h>

namespace DB
{
using WorkerList = std::vector<String>;
using ServerAssignmentMap = std::unordered_map<String, ServerDataPartsVector>;
// map: hostname -> (map: part_index -> mark ranges)
using VirtualPartAssignmentMap = std::unordered_map<String, std::map<int, std::unique_ptr<MarkRanges>>>;
using FilePartsAssignMap = std::unordered_map<String, FileDataPartsCNCHVector>;
using AssignmentMap = std::unordered_map<String, MergeTreeDataPartsCNCHVector>;
using BucketNumbersAssignmentMap = std::unordered_map<String, std::set<Int64>>;

using HivePartsAssignMap = std::unordered_map<String, HiveFiles>;

struct BucketNumberAndServerPartsAssignment
{
    ServerAssignmentMap parts_assignment_map;
    BucketNumbersAssignmentMap bucket_number_assignment_map;
};

// the hive has different allocate logic, thus separate it.
FilePartsAssignMap assignCnchFileParts(const WorkerGroupHandle & worker_group, const FileDataPartsCNCHVector & parts);
HivePartsAssignMap assignCnchHiveParts(const WorkerGroupHandle & worker_group, const HiveFiles & parts);

template <typename DataPartsCnchVector>
std::unordered_map<String, DataPartsCnchVector> assignCnchParts(const WorkerGroupHandle & worker_group, const DataPartsCnchVector & parts, const ContextPtr & context, MergeTreeSettingsPtr settings, std::optional<Context::PartAllocator> allocator = std::nullopt);

BucketNumbersAssignmentMap assignBuckets(const std::set<Int64> & required_bucket_numbers, const WorkerList & workers, bool replicated);

/**
 * splitCnchParts will split server parts into bucketed parts and leftover server parts.
 * This is useful for partially clustered tables so that parts are assigned to their corresponding
 * workers for multiple queries in order to prevent frequent cache misses.
 */
std::pair<ServerDataPartsVector, ServerDataPartsVector>
splitCnchParts(const ContextPtr & context, const IStorage & storage, const ServerDataPartsVector & parts);
void moveBucketTablePartsToAssignedParts(
    std::unordered_map<String, ServerDataPartsVector> & assigned_map,
    ServerDataPartsVector & bucket_parts,
    const WorkerList & workers,
    std::set<Int64> required_bucket_numbers = {},
    bool replicated = false);
BucketNumberAndServerPartsAssignment assignCnchPartsForBucketTable(
    const ServerDataPartsVector & parts, WorkerList workers, std::set<Int64> required_bucket_numbers = {}, bool replicated = false);

bool satisfyBucketWorkerRelation(const StoragePtr & storage, const Context & query_context);

/////////////////////////////////////////////////
////////       Hybrid allocation related methods
/////////////////////////////////////////////////

struct HybridPart
{
    String key;
    bool is_virtual;
    // part index, only used in one allocation
    size_t index;
    UInt64 marks_count;
    size_t begin;
    size_t end;

    HybridPart(const String & _key, bool _is_virtual, size_t _index, UInt64 _marks_count, size_t _begin, size_t _end)
        : key(_key), is_virtual(_is_virtual), index(_index), marks_count(_marks_count), begin(_begin), end(_end)
    {
    }

    String toString() const
    {
        std::stringstream ss;
        ss << key << ", ";
        ss << is_virtual << ", ";
        ss << index << ", ";
        ss << marks_count << ", ";
        ss << begin << ", ";
        ss << end << ".";
        return ss.str();
    }
};

size_t computeVirtualPartSize(size_t min_rows_per_vp, size_t index_granularity);

std::pair<ServerAssignmentMap, VirtualPartAssignmentMap> assignCnchHybridParts(
    const WorkerGroupHandle & worker_group, const ServerDataPartsVector & parts, size_t virtual_part_size /* unit = num marks */, const ContextPtr & context);

void sortByMarksCount(std::vector<HybridPart> & hybrid_parts);

void splitHybridParts(const ServerDataPartsVector & parts, size_t virtual_part_size, std::vector<HybridPart> & hybrid_parts);

void mergeConsecutiveRanges(VirtualPartAssignmentMap & virtual_part_assignment);

ServerVirtualPartVector getVirtualPartVector(const ServerDataPartsVector & parts, std::map<int, std::unique_ptr<MarkRanges>> & parts_entry);

void filterParts(Coordination::IMergeTreeDataPartsVector & parts, const SourceTaskFilter & filter);
}
