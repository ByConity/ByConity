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

#include <Catalog/Catalog.h>
#include <Catalog/DataModelPartWrapper.h>
#include <Interpreters/WorkerGroupHandle.h>
#include <MergeTreeCommon/assignCnchParts.h>
#include <Storages/RemoteFile/CnchFileCommon.h>
#include <Storages/RemoteFile/CnchFileSettings.h>
#include <Storages/MergeTree/DeleteBitmapMeta.h>
#include <Storages/Hive/HiveFile/IHiveFile.h>
#include "Common/HostWithPorts.h"
#include "common/types.h"
#include <common/logger_useful.h>
#include "Catalog/DataModelPartWrapper_fwd.h"
#include "Interpreters/Context.h"

#include <sstream>
#include <unordered_map>

namespace ProfileEvents
{
    extern const Event CnchDiskCacheNodeUnLocalityParts;
}

namespace DB
{

template<typename M>
inline void reportStats(Poco::Logger * log, const M & map, const String & name, size_t num_workers)
{
    std::stringstream ss;
    ss << name << " : ";
    double sum = 0;
    double max_load = 0;
    for (const auto & it : map)
    {
        sum += it.second.size();
        max_load = std::max(max_load, static_cast<double>(it.second.size()));
        ss << it.first << " -> " << it.second.size() << "; ";
    }
    /// calculate coefficient of variance of the distribution
    double mean = sum / num_workers;
    double sum_of_squares = 0;
    for (const auto & it : map)
    {
        double diff = it.second.size() - mean;
        sum_of_squares += diff * diff;
    }
    /// coefficient of variance, lower is better
    double co_var = std::sqrt(sum_of_squares / num_workers) / mean;
    /// peak over average ratio, lower is better and must not exceed the load factor if use bounded load balancing
    double peak_over_avg = max_load / mean;
    ss << "stats: " << co_var << " " << peak_over_avg << std::endl;
    LOG_DEBUG(log, ss.str());
}

/// explicit instantiation for server part and cnch data part.
template ServerAssignmentMap assignCnchParts<ServerDataPartsVector>(const WorkerGroupHandle & worker_group, const ServerDataPartsVector & parts, const ContextPtr & query_context, MergeTreeSettingsPtr settings, std::optional<Context::PartAllocator> allocator = std::nullopt);
template AssignmentMap assignCnchParts<MergeTreeDataPartsCNCHVector>(const WorkerGroupHandle & worker_group, const MergeTreeDataPartsCNCHVector & parts, const ContextPtr & query_context, MergeTreeSettingsPtr settings, std::optional<Context::PartAllocator> allocator = std::nullopt);
template std::unordered_map<String, DataModelPartWrapperVector> assignCnchParts<DataModelPartWrapperVector>(const WorkerGroupHandle & worker_group, const DataModelPartWrapperVector &, const ContextPtr & query_context, MergeTreeSettingsPtr settings, std::optional<Context::PartAllocator> allocator = std::nullopt);
template std::unordered_map<String, DeleteBitmapMetaPtrVector> assignCnchParts<DeleteBitmapMetaPtrVector>(const WorkerGroupHandle & worker_group, const DeleteBitmapMetaPtrVector &, const ContextPtr & query_context, MergeTreeSettingsPtr settings, std::optional<Context::PartAllocator> allocator = std::nullopt);

template <typename DataPartsCnchVector>
std::unordered_map<String, DataPartsCnchVector> assignCnchParts(const WorkerGroupHandle & worker_group, const DataPartsCnchVector & parts, const ContextPtr & query_context, MergeTreeSettingsPtr settings, std::optional<Context::PartAllocator> allocator)
{
    static auto * log = &Poco::Logger::get("assignCnchParts");
    Context::PartAllocator part_allocation_algorithm = allocator.value_or(query_context->getPartAllocationAlgo(settings));

    switch (part_allocation_algorithm)
    {
        case Context::PartAllocator::JUMP_CONSISTENT_HASH:
        {
            auto ret = assignCnchPartsWithJump(worker_group->getWorkerIDVec(), worker_group->getIdHostPortsMap(), parts);
            reportStats(log, ret, "Jump Consistent Hash", worker_group->getWorkerIDVec().size());
            return ret;
        }
        case Context::PartAllocator::RING_CONSISTENT_HASH:
        {
            auto ret = assignCnchPartsWithRingAndBalance(log, worker_group->getWorkerIDVec(), worker_group->getIdHostPortsMap(), worker_group->getRing(), parts);
            reportStats(log, ret, "Bounded-load Consistent Hash", worker_group->getRing().size());
            return ret;
        }
        case Context::PartAllocator::STRICT_RING_CONSISTENT_HASH:
        {
            auto ret = assignCnchPartsWithStrictBoundedHash(log, worker_group->getWorkerIDVec(), worker_group->getIdHostPortsMap(), worker_group->getRing(), parts, true);
            reportStats(log, ret, "Strict Consistent Hash", worker_group->getRing().size());
            return ret;
        }
        case Context::PartAllocator::DISK_CACHE_STEALING_DEBUG: //Note: Now just used for test disk cache stealing so not used for online
        {
            auto ret = assignCnchPartsWithStealingCache(worker_group->getWorkerIDVec(), worker_group->getIdHostPortsMap(), parts);
            reportStats(log, ret, "disk cache stealing debug", worker_group->getWorkerIDVec().size());
            return ret;
        }
    }
}

template <typename DataPartsCnchVector>
std::unordered_map<String, DataPartsCnchVector> assignCnchPartsWithStealingCache(WorkerList worker_ids, const std::unordered_map<String, HostWithPorts> & worker_hosts, const DataPartsCnchVector & parts)
{
    std::unordered_map<String, DataPartsCnchVector> ret;
    /// we don't know the order of workers returned from consul so sort then explicitly now
    sort(worker_ids.begin(), worker_ids.end());
    auto num_workers = worker_ids.size();

    for (size_t i = 0; i < parts.size(); ++i)
    {
        /// use crc64 as original implementation, may change to other hash later
        auto part_name = parts[i]->getNameForAllocation();
        auto hash_val = fio_crc64(reinterpret_cast<const unsigned char *>(part_name.c_str()), part_name.length());
        auto index_consistent = JumpConsistentHash(hash_val, num_workers);

        auto index_simple = (index_consistent + 1) % num_workers;
        ret[worker_ids[index_simple]].emplace_back(parts[i]);

        auto disk_cache_host_port = worker_hosts.at(worker_ids[index_consistent]).getRPCAddress();
        auto assign_compute_host_port = worker_hosts.at(worker_ids[index_simple]).getRPCAddress();
        parts[i]->setHostPort(disk_cache_host_port, assign_compute_host_port);
        if (parts[i]->assign_compute_host_port != parts[i]->disk_cache_host_port)
                ProfileEvents::increment(ProfileEvents::CnchDiskCacheNodeUnLocalityParts, 1);
    }
    return ret;
}

/// worker_ids should be sorted
template <typename DataPartsCnchVector>
std::unordered_map<String, DataPartsCnchVector> assignCnchPartsWithJump(
    const WorkerList & worker_ids, const std::unordered_map<String, HostWithPorts> & worker_hosts, const DataPartsCnchVector & parts)
{
    std::unordered_map<String, DataPartsCnchVector> ret;
    auto num_workers = worker_ids.size();

    for (const auto & part : parts)
    {
        /// use crc64 as original implementation, may change to other hash later
        auto part_name = part->getNameForAllocation();
        auto hash_val = fio_crc64(reinterpret_cast<const unsigned char *>(part_name.c_str()), part_name.length());
        auto index = JumpConsistentHash(hash_val, num_workers);

        auto host_port = worker_hosts.at(worker_ids[index]).getRPCAddress();
        part->setHostPort(host_port, host_port);
        ret[worker_ids[index]].emplace_back(part);
    }
    return ret;
}

/// 2 round apporach
template <typename DataPartsCnchVector>
std::unordered_map<String, DataPartsCnchVector> assignCnchPartsWithRingAndBalance(Poco::Logger * log, WorkerList worker_ids, const std::unordered_map<String, HostWithPorts> & worker_hosts, const ConsistentHashRing & ring, const DataPartsCnchVector & parts)
{
    LOG_INFO(log, "Consistent Hash: Start to allocate part with bounded ring based hash policy.");
    std::unordered_map<String, DataPartsCnchVector> ret;
    size_t num_parts = parts.size();
    auto cap_limit = ring.getCapLimit(num_parts);
    DataPartsCnchVector exceed_parts;
    std::unordered_map<String, UInt64> stats;

    // first round, try respect original hash mapping as much as possible
    sort(worker_ids.begin(), worker_ids.end());
    auto num_workers = worker_ids.size();

    auto disk_cache_worker_index = 0;
    String disk_cache_host_port = "";
    for (auto & part : parts)
    {
        auto part_name = part->getNameForAllocation();
        auto hash_val = fio_crc64(reinterpret_cast<const unsigned char *>(part_name.c_str()), part_name.length());
        disk_cache_worker_index = JumpConsistentHash(hash_val, num_workers);
        disk_cache_host_port = worker_hosts.at(worker_ids[disk_cache_worker_index]).getRPCAddress();

        if (auto host_id = ring.tryFind(part->getNameForAllocation(), cap_limit, stats); !host_id.empty())
        {
            ret[host_id].emplace_back(part);

            auto assign_compute_host_port = worker_hosts.at(host_id).getRPCAddress();
            part->setHostPort(disk_cache_host_port, assign_compute_host_port);
            if (part->assign_compute_host_port != part->disk_cache_host_port)
                ProfileEvents::increment(ProfileEvents::CnchDiskCacheNodeUnLocalityParts, 1);
        }
        else
            exceed_parts.emplace_back(part);
    }

    // second round to assign the overloaded parts, reuse the one round apporach `findAndRebalance`.
    for (auto & part: exceed_parts)
    {
        auto host_id = ring.findAndRebalance(part->getNameForAllocation(), cap_limit, stats);
        ret[host_id].emplace_back(part);

        auto assign_compute_host_port = worker_hosts.at(host_id).getRPCAddress();
        part->setHostPort(disk_cache_host_port, assign_compute_host_port);
        if (part->assign_compute_host_port != part->disk_cache_host_port)
                ProfileEvents::increment(ProfileEvents::CnchDiskCacheNodeUnLocalityParts, 1);
    }


    LOG_INFO(log,
             "Consistent Hash: Finish allocate part with bounded ring based hash policy, # of overloaded parts {}.", exceed_parts.size());
    return ret;
}

// 1 round approach
template <typename DataPartsCnchVector>
static std::unordered_map<String, DataPartsCnchVector> assignCnchPartsWithStrictBoundedHash(Poco::Logger * log, WorkerList worker_ids, const std::unordered_map<String, HostWithPorts> & worker_hosts, const ConsistentHashRing & ring, const DataPartsCnchVector & parts, bool strict)
{
    LOG_DEBUG(log, "Strict Bounded Consistent Hash: Start to allocate part with bounded ring based hash policy under strict mode " + std::to_string(strict) + ".");
    std::unordered_map<String, DataPartsCnchVector> ret;
    size_t cap_limit = 0;
    std::unordered_map<String, UInt64> stats;

    sort(worker_ids.begin(), worker_ids.end());
    auto num_workers = worker_ids.size();

    for (size_t i = 0; i < parts.size(); ++i)
    {
        auto & part = parts[i];

        auto part_name = part->getNameForAllocation();
        auto hash_val = fio_crc64(reinterpret_cast<const unsigned char *>(part_name.c_str()), part_name.length());
        auto index = JumpConsistentHash(hash_val, num_workers);

        cap_limit = ring.getCapLimit(i + 1, strict);
        auto host_id = ring.findAndRebalance(part->getNameForAllocation(), cap_limit, stats);
        ret[host_id].emplace_back(part);

        auto disk_cache_host_port = worker_hosts.at(worker_ids[index]).getRPCAddress();
        auto assign_compute_host_port = worker_hosts.at(host_id).getRPCAddress();
        part->setHostPort(disk_cache_host_port, assign_compute_host_port);
        if (part->assign_compute_host_port != part->disk_cache_host_port)
                ProfileEvents::increment(ProfileEvents::CnchDiskCacheNodeUnLocalityParts, 1);
    }

    LOG_DEBUG(log, "Strict Bounded Consistent Hash: Finish allocate part with strict bounded ring based hash policy under strict mode " + std::to_string(strict) + ".");
    return ret;
}

HivePartsAssignMap assignCnchHiveParts(const WorkerGroupHandle & worker_group, const HiveFiles & parts)
{
    auto worker_ids = worker_group->getWorkerIDVec();
    /// we don't know the order of workers returned from consul so sort explicitly now
    sort(worker_ids.begin(), worker_ids.end());

    auto num_workers = worker_ids.size();
    HivePartsAssignMap ret;

    for (const auto & file : parts)
    {
        size_t assigned_worker_idx = -1;
        if (auto bucket_id = file->getBucketId(); bucket_id)
            assigned_worker_idx = *bucket_id % num_workers;
        else
            assigned_worker_idx = consistentHashForString(file->file_path, num_workers);

        ret[worker_ids[assigned_worker_idx]].emplace_back(file);
    }

    return ret;
}

FilePartsAssignMap assignCnchFileParts(const WorkerGroupHandle & worker_group, const FileDataPartsCNCHVector & parts)
{
    auto workers = worker_group->getWorkerIDVec();
    auto num_workers = workers.size();
    FilePartsAssignMap ret;
    for (size_t i = 0 ; i < parts.size(); i++)
    {
        auto index = i % num_workers;
        ret[workers[index]].emplace_back(parts[i]);
    }
    return ret;
}


std::pair<ServerDataPartsVector, ServerDataPartsVector>
splitCnchParts(const ContextPtr & context, const IStorage & storage, const ServerDataPartsVector & parts)
{
    if (!storage.isBucketTable())
        return {{}, std::move(parts)};
    if (context->getCnchCatalog()->isTableClustered(storage.getStorageUUID()))
        return {std::move(parts), {}};

    std::pair<ServerDataPartsVector, ServerDataPartsVector> res;
    auto & bucket_parts = res.first;
    auto & leftover_parts = res.second;

    auto table_definition_hash = storage.getTableHashForClusterBy();
    std::for_each(parts.begin(), parts.end(), [&](auto part)
    {
        bool is_clustered = table_definition_hash.match(part->part_model().table_definition_hash()) && part->part_model().bucket_number() != -1;
        if (is_clustered)
            bucket_parts.emplace_back(part);
        else
            leftover_parts.emplace_back(part);
    });

    return res;
}

void moveBucketTablePartsToAssignedParts(
    std::unordered_map<String, ServerDataPartsVector> & assigned_map,
    ServerDataPartsVector & bucket_parts,
    const WorkerList & workers,
    std::set<Int64> required_bucket_numbers,
    bool replicated)
{
    if(bucket_parts.empty())
        return;
    auto bucket_parts_assignment_map
        = assignCnchPartsForBucketTable(bucket_parts, workers, required_bucket_numbers, replicated).parts_assignment_map;
    for (auto & [worker_id, bucket_assigned_parts]: bucket_parts_assignment_map ) {
        auto & assigned_parts = assigned_map[worker_id];
        std::move(bucket_assigned_parts.begin(), bucket_assigned_parts.end(), std::back_inserter(assigned_parts));
    }
}

BucketNumbersAssignmentMap assignBuckets(const std::set<Int64> & required_bucket_numbers, const WorkerList & workers, bool replicated)
{
    BucketNumbersAssignmentMap assignment;
    if (replicated)
    {
        for (const auto & worker : workers)
            assignment[worker] = required_bucket_numbers;
    }
    else
    {
        for (auto bucket : required_bucket_numbers)
            assignment[workers[bucket % workers.size()]].insert(bucket);
    }
    return assignment;
}

BucketNumberAndServerPartsAssignment assignCnchPartsForBucketTable(
    const ServerDataPartsVector & parts, WorkerList workers, std::set<Int64> required_bucket_numbers, bool replicated)
{
    std::sort(workers.begin(), workers.end());
    BucketNumberAndServerPartsAssignment assignment;

    /// Leverage unordered_set to gain O(1) contains
    std::unordered_set<Int64> required_bucket_numbers_set(required_bucket_numbers.begin(), required_bucket_numbers.end());

    for (const auto & part : parts)
    {
        // For bucket tables, the parts with the same bucket number is assigned to the same worker.
        Int64 bucket_number = part->part_model().bucket_number();
        // if required_bucket_numbers is empty, assign parts as per normal
        if (required_bucket_numbers_set.empty() || required_bucket_numbers_set.contains(bucket_number))
        {
            if (replicated)
            {
                for (size_t i = 0; i < workers.size(); ++i)
                {
                    assignment.parts_assignment_map[workers[i]].emplace_back(part);
                    assignment.bucket_number_assignment_map[workers[i]].insert(bucket_number);
                }
            }
            else
            {
                auto index = bucket_number % workers.size();
                assignment.parts_assignment_map[workers[index]].emplace_back(part);
                assignment.bucket_number_assignment_map[workers[index]].insert(bucket_number);
            }
        }
    }
    return assignment;
}

bool satisfyBucketWorkerRelation(const StoragePtr & storage, const Context & query_context)
{
    if (const auto * merge_tree = dynamic_cast<MergeTreeMetaBase *>(storage.get());
        merge_tree && merge_tree->isBucketTable() && merge_tree->getInMemoryMetadataPtr()->getBucketNumberFromClusterByKey() % query_context.getCurrentWorkerGroup()->size() == 0)
        return true;
    return false;
}

/////////////////////////////////////////////////
////////       Hybrid allocation related methods
/////////////////////////////////////////////////

// virtual part size(marks) = virtual part rows / rows per mark
size_t computeVirtualPartSize(size_t min_rows_per_vp, size_t index_granularity)
{
    /// TODO: handle index_granularity_bytes
    return min_rows_per_vp % index_granularity ? min_rows_per_vp / index_granularity + 1 : min_rows_per_vp / index_granularity;
}

static std::pair<ServerAssignmentMap, VirtualPartAssignmentMap> assignCnchHybridPartsWithMod(
    Poco::Logger * log, const WorkerGroupHandle & worker_group, const ServerDataPartsVector & parts, size_t virtual_part_size /* unit = num marks */)
{
    std::pair<ServerAssignmentMap, VirtualPartAssignmentMap> res;
    auto & physical_assignment = res.first;
    auto & virtual_assignment = res.second;
    const auto & shard_infos = worker_group->getShardsInfo();
    auto hasher = std::hash<String>{};
    for (size_t i = 0; i < parts.size(); ++i)
    {
        const auto & part = parts[i];
        UInt64 marks_cnt = part->marksCount();
        // For little parts, we don't split it as virtual parts any more.
        if (marks_cnt <= virtual_part_size || part->isPartial())
        {
            auto index = hasher(part->info().getBasicPartName()) % shard_infos.size();
            String hostname = shard_infos[index].worker_id;
            physical_assignment[hostname].emplace_back(part);
        }
        else
        {
            // First virtual part, [0, virtual_part_size)
            size_t begin = 0, end = virtual_part_size;
            String base_name = part->info().getBasicPartName();
            while (begin < end)
            {
                /// TODO: control number of virtual parts
                String key = fmt::format("{}{}_{}", base_name, begin, end);
                auto index = hasher(key) % shard_infos.size();
                String hostname = shard_infos[index].worker_id;
                LOG_TRACE(log, "assignCnchHybridPartsWithMod: virtual part key {} assign to worker {}", key, hostname);

                auto insert_entry = virtual_assignment.try_emplace(hostname).first;
                auto & virtual_parts = insert_entry->second;
                auto & mark_ranges = virtual_parts[i];
                if (mark_ranges == nullptr)
                {
                    mark_ranges = std::make_unique<MarkRanges>();
                }
                /// Try to merge consecutive ranges
                if (mark_ranges->empty() || mark_ranges->back().end < begin)
                    mark_ranges->emplace_back(begin, end);
                else
                    mark_ranges->back().end = end;
                /// Next virtual part
                begin = end;
                end = std::min(marks_cnt, end + virtual_part_size);
            }
        }
    }
    return res;
}

static std::pair<ServerAssignmentMap, VirtualPartAssignmentMap> assignCnchHybridPartsWithBoundedHash(
    const WorkerGroupHandle & worker_group, const ServerDataPartsVector & parts, size_t virtual_part_size /* unit = num marks */)
{
    std::pair<ServerAssignmentMap, VirtualPartAssignmentMap> res;
    auto & physical_assignment = res.first;
    auto & virtual_assignment = res.second;
    std::vector<HybridPart> hybrid_parts;

    // break part to virtual parts if needed
    splitHybridParts(parts, virtual_part_size, hybrid_parts);

    // sort by marks count
    sortByMarksCount(hybrid_parts);

    // start allocate
    const auto & ring = worker_group->getRing();
    auto cap_limit = ring.getCapLimit(hybrid_parts.size());
    std::vector<HybridPart> exceed_hybrid_parts;
    std::unordered_map<String, UInt64> stats;

    // first round allocation
    for (const auto & hybrid_part : hybrid_parts)
    {
        if (hybrid_part.is_virtual)
        {
            if (auto hostname = ring.tryFind(hybrid_part.key, cap_limit, stats); !hostname.empty())
            {
                auto insert_entry = virtual_assignment.try_emplace(hostname).first;
                auto & virtual_parts = insert_entry->second;
                auto & mark_ranges = virtual_parts[hybrid_part.index];
                if (mark_ranges == nullptr)
                {
                    mark_ranges = std::make_unique<MarkRanges>();
                }
                mark_ranges->emplace_back(hybrid_part.begin, hybrid_part.end);
            }
            else
                exceed_hybrid_parts.emplace_back(hybrid_part);
        }
        else
        {
            if (auto hostname = ring.tryFind(hybrid_part.key, cap_limit, stats); !hostname.empty())
                physical_assignment[hostname].emplace_back(parts[hybrid_part.index]);
            else
                exceed_hybrid_parts.emplace_back(hybrid_part);
        }
    }

    // second round allocation
    for (const auto & hybrid_part : exceed_hybrid_parts)
    {
        auto hostname = ring.findAndRebalance(hybrid_part.key, cap_limit, stats);
        if (hybrid_part.is_virtual)
        {
            auto insert_entry = virtual_assignment.try_emplace(hostname).first;
            auto & virtual_parts = insert_entry->second;
            auto & mark_ranges = virtual_parts[hybrid_part.index];
            if (mark_ranges == nullptr)
            {
                mark_ranges = std::make_unique<MarkRanges>();
            }
            mark_ranges->emplace_back(hybrid_part.begin, hybrid_part.end);
        }
        else
            physical_assignment[hostname].emplace_back(parts[hybrid_part.index]);
    }

    // try to merge consecutive ranges
    mergeConsecutiveRanges(virtual_assignment);

    return res;
}

static std::pair<ServerAssignmentMap, VirtualPartAssignmentMap> assignCnchHybridPartsWithStrictBoundedHash(
    Poco::Logger * log,
    const WorkerGroupHandle & worker_group,
    const ServerDataPartsVector & parts,
    size_t virtual_part_size /* unit = num marks */,
    bool strict)
{
    LOG_DEBUG(
        log,
        "Strict Bounded Consistent Hash for Hybrid Allocation: "
        "Start to allocate part with strict bounded ring based hash policy under strict mode {}.",
        std::to_string(strict));

    std::pair<ServerAssignmentMap, VirtualPartAssignmentMap> res;
    auto & physical_assignment = res.first;
    auto & virtual_assignment = res.second;
    std::vector<HybridPart> hybrid_parts;

    // break part to virtual parts if needed
    splitHybridParts(parts, virtual_part_size, hybrid_parts);

    // sort by marks count
    sortByMarksCount(hybrid_parts);

    // Start allocate, allocate with bounded consistent hash in one round
    const auto & ring = worker_group->getRing();
    size_t cap_limit = 0;
    std::unordered_map<String, UInt64> stats;

    for (size_t i = 0; i < hybrid_parts.size(); ++i)
    {
        const auto & hybrid_part = hybrid_parts[i];
        // STRICT only decides if the cap limit can use load_factor
        cap_limit = ring.getCapLimit(i + 1, strict);
        auto hostname = ring.findAndRebalance(hybrid_part.key, cap_limit, stats);
        LOG_TRACE(
            log,
            "Strict bounded consistent hash: Allocate hybrid part {} to host {}",
            hybrid_part.toString(), hostname);
        if (hybrid_part.is_virtual)
        {
            auto insert_entry = virtual_assignment.try_emplace(hostname).first;
            auto & virtual_parts = insert_entry->second;
            auto & mark_ranges = virtual_parts[hybrid_part.index];
            if (mark_ranges == nullptr)
            {
                mark_ranges = std::make_unique<MarkRanges>();
            }
            mark_ranges->emplace_back(hybrid_part.begin, hybrid_part.end);
        }
        else
            physical_assignment[hostname].emplace_back(parts[hybrid_part.index]);
    }

    // try to merge consecutive ranges
    mergeConsecutiveRanges(virtual_assignment);

    LOG_DEBUG(
        log,
        "Strict Bounded Consistent Hash for Hybrid Allocation: "
        "Finish allocate part with strict bounded ring based hash policy under strict mode {}.",
        std::to_string(strict));
    return res;
}

static std::pair<ServerAssignmentMap, VirtualPartAssignmentMap> assignCnchHybridPartsWithConsistentHash(
    Poco::Logger * log, const WorkerGroupHandle & worker_group, const ServerDataPartsVector & parts, size_t virtual_part_size /* unit = num marks */)
{
    const auto & ring = worker_group->getRing();
    std::pair<ServerAssignmentMap, VirtualPartAssignmentMap> res;
    auto & physical_assignment = res.first;
    auto & virtual_assignment = res.second;
    for (size_t i = 0; i < parts.size(); ++i)
    {
        const auto & part = parts[i];
        UInt64 marks_cnt = part->marksCount();
        if (marks_cnt <= virtual_part_size || part->isPartial())
        {
            /// only 1 single parts
            String hostname = ring.find(part->info().getBasicPartName());
            physical_assignment[hostname].emplace_back(part);
        }
        else
        {
            size_t begin = 0, end = std::min(marks_cnt, virtual_part_size);
            String base_name = part->info().getBasicPartName();
            while (begin < end)
            {
                /// TODO: control number of virtual parts
                String key = fmt::format("{}{}_{}", base_name, begin, end);
                String hostname = ring.find(key);
                auto insert_entry = virtual_assignment.try_emplace(hostname).first;

                LOG_TRACE(log, "assignCnchHybridPartsWithConsistentHash: virtual part key {} assign to worker {}", key, hostname);
                auto & virtual_parts = insert_entry->second;
                auto & mark_ranges = virtual_parts[i];
                if (mark_ranges == nullptr)
                {
                    mark_ranges = std::make_unique<MarkRanges>();
                }
                /// Try to merge consecutive ranges
                if (mark_ranges->empty() || mark_ranges->back().end < begin)
                    mark_ranges->emplace_back(begin, end);
                else
                    mark_ranges->back().end = end;
                /// Next virtual part
                begin = end;
                end = std::min(marks_cnt, end + virtual_part_size);
            }
        }
    }
    return res;
}

void sortByMarksCount(std::vector<HybridPart> & hybrid_parts)
{
    std::sort(hybrid_parts.begin(), hybrid_parts.end(), [](const auto & lhs, const auto & rhs) {
        if (lhs.marks_count == rhs.marks_count)
            // the key is unique
            return lhs.key < rhs.key;
        else
            return lhs.marks_count > rhs.marks_count;
    });
}

void splitHybridParts(const ServerDataPartsVector & parts, size_t virtual_part_size, std::vector<HybridPart> & hybrid_parts)
{
    for (size_t i = 0; i < parts.size(); ++i)
    {
        const auto & part = parts[i];
        UInt64 marks_cnt = part->marksCount();
        String base_name = part->info().getBasicPartName();
        if (marks_cnt <= virtual_part_size || part->isPartial())
        {
            // physical part
            hybrid_parts.emplace_back(base_name, false, i, marks_cnt, 0, marks_cnt);
        }
        else
        {
            // virtual part
            size_t begin = 0, end = virtual_part_size;
            while (begin < end)
            {
                String key = fmt::format("{}{}_{}", base_name, begin, end);
                hybrid_parts.emplace_back(key, true, i, end - begin, begin, end);
                /// Next virtual part
                begin = end;
                end = std::min(marks_cnt, end + virtual_part_size);
            }
        }
    }
}

void mergeConsecutiveRanges(VirtualPartAssignmentMap & virtual_part_assignment)
{
    for (const auto & items_per_host : virtual_part_assignment)
        for (const auto & items_per_part : items_per_host.second)
        {
            auto & marks = *items_per_part.second;
            // sort by begin marks
            std::sort(marks.begin(), marks.end(), [](const auto & lhs, const auto & rhs) { return lhs.begin < rhs.begin; });

            size_t current = 0;
            for (size_t next = current + 1; next < marks.size(); ++next)
            {
                if (marks[current].end == marks[next].begin)
                    marks[current].end = marks[next].end;
                else
                {
                    ++current;
                    if (current < next)
                        marks[current] = marks[next];
                }
            }
            marks.erase(marks.begin() + current + 1, marks.end());
        }
}

static void reportHybridAllocStats(Poco::Logger * log, ServerAssignmentMap & physical_parts, VirtualPartAssignmentMap & virtual_parts, const String & name)
{
    std::unordered_map<String, size_t> allocated_marks;
    for (const auto & physical_part : physical_parts)
    {
        const auto & key = physical_part.first;
        const auto & parts = physical_part.second;
        size_t total_marks
            = std::accumulate(parts.begin(), parts.end(), std::size_t(0), [](std::size_t result, const ServerDataPartPtr & part) {
                  return result + part->marksCount();
              });
        allocated_marks[key] += total_marks;
    }

    for (const auto & virtual_part : virtual_parts)
    {
        const auto & key = virtual_part.first;
        const auto & marks_map = virtual_part.second;
        for (const auto & part_marks : marks_map)
        {
            auto & marks = *(part_marks.second);
            size_t total_marks
                = std::accumulate(marks.begin(), marks.end(), std::size_t(0), [](std::size_t result, const MarkRange & mark) {
                      return result + (mark.end - mark.begin);
                  });
            allocated_marks[key] += total_marks;
        }
    }

    double sum = 0;
    double max_load = 0;
    size_t sz = allocated_marks.size();
    std::stringstream ss;
    ss << name << " : ";
    for (const auto & [host, mark_count] : allocated_marks)
    {
        sum += mark_count;
        max_load = std::max(max_load, static_cast<double>(mark_count));
        ss << host << " -> " << mark_count << "; ";
    }
    /// calculate coefficient of variance of the distribution
    double mean = sum / sz;
    double sum_of_squares = 0;
    for (const auto & v : allocated_marks)
    {
        double diff = v.second - mean;
        sum_of_squares += diff * diff;
    }
    /// coefficient of variance, lower is better
    double co_var = std::sqrt(sum_of_squares / sz) / mean;
    /// peak over average ratio, lower is better and must not exceed the load factor if use bounded load balancing
    double peak_over_avg = max_load / mean;
    ss << "; stats: " << co_var << " " << peak_over_avg << std::endl;
    LOG_DEBUG(log, ss.str());
}

ServerVirtualPartVector getVirtualPartVector(const ServerDataPartsVector & parts, std::map<int, std::unique_ptr<MarkRanges>> & parts_entry)
{
    ServerVirtualPartVector res;
    res.reserve(parts_entry.size());
    for (auto & [index, mark_ranges] : parts_entry)
    {
        res.emplace_back(std::make_shared<ServerVirtualPart>(parts[index], std::move(mark_ranges)));
    }
    return res;
}

std::pair<ServerAssignmentMap, VirtualPartAssignmentMap> assignCnchHybridParts(
    const WorkerGroupHandle & worker_group, const ServerDataPartsVector & parts, size_t virtual_part_size /* unit = num marks */, const ContextPtr & query_context)
{
    static auto * log = &Poco::Logger::get("assignCnchHybridParts");
    // If part_allocation_algorithm is specified in SQL Level, use it with high priority
    Context::HybridPartAllocator part_allocation_algorithm;
    if (query_context->getSettingsRef().cnch_hybrid_part_allocation_algorithm.changed)
        part_allocation_algorithm = query_context->getHybridPartAllocationAlgo();
    else
        part_allocation_algorithm = worker_group->getContext()->getHybridPartAllocationAlgo();

    switch (part_allocation_algorithm)
    {
        case Context::HybridPartAllocator::HYBRID_MODULO_CONSISTENT_HASH: {
            auto res = assignCnchHybridPartsWithMod(log, worker_group, parts, virtual_part_size);
            reportHybridAllocStats(log, res.first, res.second, "Hybrid Allocation (Modular Hashing)");
            return res;
        }
        case Context::HybridPartAllocator::HYBRID_RING_CONSISTENT_HASH: {
            auto res = assignCnchHybridPartsWithConsistentHash(log, worker_group, parts, virtual_part_size);
            reportHybridAllocStats(log, res.first, res.second, "Hybrid Allocation (Ring Consistent Hashing)");
            return res;
        }
        case Context::HybridPartAllocator::HYBRID_BOUNDED_LOAD_CONSISTENT_HASH: {
            auto res = assignCnchHybridPartsWithBoundedHash(worker_group, parts, virtual_part_size);
            reportHybridAllocStats(log, res.first, res.second, "Hybrid Allocation (Bounded Ring Consistent Hashing)");
            return res;
        }
        case Context::HybridPartAllocator::HYBRID_RING_CONSISTENT_HASH_ONE_STAGE: {
            auto res = assignCnchHybridPartsWithStrictBoundedHash(log, worker_group, parts, virtual_part_size, false);
            reportHybridAllocStats(log, res.first, res.second, "Hybrid Allocation (One Stage Bounded Consistent Hashing)");
            return res;
        }
        case Context::HybridPartAllocator::HYBRID_STRICT_RING_CONSISTENT_HASH_ONE_STAGE: {
            auto res = assignCnchHybridPartsWithStrictBoundedHash(log, worker_group, parts, virtual_part_size, true);
            reportHybridAllocStats(log, res.first, res.second, "Hybrid Allocation (Strict One Stage Bounded Consistent Hashing)");
            return res;
        }
        default: {
            auto res = assignCnchHybridPartsWithBoundedHash(worker_group, parts, virtual_part_size);
            reportHybridAllocStats(log, res.first, res.second, "Hybrid Allocation (Bounded Ring Consistent Hashing)");
            return res;
        }
    }
}

void filterParts(IMergeTreeDataPartsVector & parts, const SourceTaskFilter & filter)
{
    if (filter.index && filter.count)
    {
        size_t index = *filter.index, count = *filter.count;
        if (count == 0 || index >= count)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot filterParts, invalid {}", filter.toString());
        size_t c = 0;
        for (auto iter = parts.begin(); iter != parts.end();)
        {
            if (c % count != index)
                iter = parts.erase(iter);
            else
                iter++;
            c++;
        }
    }
    else if (filter.buckets)
    {
        const auto & buckets = *filter.buckets;
        if (buckets.size() == 1 && *buckets.begin() == -1)
        {
            parts.clear();
            return;
        }
        for (auto iter = parts.begin(); iter != parts.end();)
        {
            const auto & part = *iter;
            if (!buckets.contains(part->bucket_number))
                iter = parts.erase(iter);
            else
                iter++;
        }
    }
}
}
