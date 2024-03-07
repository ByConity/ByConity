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

#include <MergeTreeCommon/assignCnchParts.h>
#include <Storages/RemoteFile/CnchFileCommon.h>
#include <Storages/RemoteFile/CnchFileSettings.h>
#include <Catalog/Catalog.h>
#include <Catalog/DataModelPartWrapper.h>
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
inline void reportStats(const M & map, const String & name, size_t num_workers)
{
    std::stringstream ss;
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
    LOG_DEBUG(&Poco::Logger::get(name), ss.str());
}

/// explicit instantiation for server part and cnch data part.
template ServerAssignmentMap assignCnchParts<ServerDataPartsVector>(const WorkerGroupHandle & worker_group, const ServerDataPartsVector & parts);
template AssignmentMap assignCnchParts<MergeTreeDataPartsCNCHVector>(const WorkerGroupHandle & worker_group, const MergeTreeDataPartsCNCHVector & parts);

template <typename DataPartsCnchVector>
std::unordered_map<String, DataPartsCnchVector> assignCnchParts(const WorkerGroupHandle & worker_group, const DataPartsCnchVector & parts)
{
    switch (worker_group->getContext()->getPartAllocationAlgo())
    {
        case Context::PartAllocator::JUMP_CONSISTENT_HASH:
        {
            auto ret = assignCnchPartsWithJump(worker_group->getWorkerIDVec(), worker_group->getIdHostPortsMap(), parts);
            reportStats(ret, "Jump Consistent Hash", worker_group->getWorkerIDVec().size());
            return ret;
        }
        case Context::PartAllocator::RING_CONSISTENT_HASH:
        {
            if (!worker_group->hasRing())
            {
                LOG_WARNING(&Poco::Logger::get("Consistent Hash"), "Attempt to use ring-base consistent hash, but ring is empty; fall back to jump");
                return assignCnchPartsWithJump(worker_group->getWorkerIDVec(), worker_group->getIdHostPortsMap(), parts);
            }
            auto ret = assignCnchPartsWithRingAndBalance(worker_group->getWorkerIDVec(), worker_group->getIdHostPortsMap(), worker_group->getRing(), parts);
            reportStats(ret, "Bounded-load Consistent Hash", worker_group->getRing().size());
            return ret;
        }
        case Context::PartAllocator::STRICT_RING_CONSISTENT_HASH:
        {
            if (!worker_group->hasRing())
            {
                LOG_WARNING(&Poco::Logger::get("Strict Consistent Hash"), "Attempt to use ring-base consistent hash, but ring is empty; fall back to jump");
                return assignCnchPartsWithJump(worker_group->getWorkerIDVec(), worker_group->getIdHostPortsMap(), parts);
            }
            auto ret = assignCnchPartsWithStrictBoundedHash(worker_group->getWorkerIDVec(), worker_group->getIdHostPortsMap(), worker_group->getRing(), parts, true);
            reportStats(ret, "Strict Consistent Hash", worker_group->getRing().size());
            return ret;
        }
        case Context::PartAllocator::SIMPLE_HASH: //Note: Now just used for test disk cache stealing so not used for online
        {
            auto ret = assignCnchPartsWithSimpleHash(worker_group->getWorkerIDVec(), worker_group->getIdHostPortsMap(), parts);
            reportStats(ret, "Simple Hash", worker_group->getWorkerIDVec().size());
            return ret;
        }
    }
}

template <typename DataPartsCnchVector>
std::unordered_map<String, DataPartsCnchVector> assignCnchPartsWithSimpleHash(WorkerList worker_ids, const std::unordered_map<String, HostWithPorts> & worker_hosts, const DataPartsCnchVector & parts)
{
    std::unordered_map<String, DataPartsCnchVector> ret;
    /// we don't know the order of workers returned from consul so sort then explicitly now
    sort(worker_ids.begin(), worker_ids.end());
    auto num_workers = worker_ids.size();

    for (size_t i = 0; i < parts.size(); ++i)
    {
        /// use crc64 as original implementation, may change to other hash later
        auto part_name = parts[i]->get_info().getBasicPartName();
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

template <typename DataPartsCnchVector>
std::unordered_map<String, DataPartsCnchVector> assignCnchPartsWithJump(WorkerList worker_ids, const std::unordered_map<String, HostWithPorts> & worker_hosts, const DataPartsCnchVector & parts)
{
    std::unordered_map<String, DataPartsCnchVector> ret;
    /// we don't know the order of workers returned from consul so sort then explicitly now
    sort(worker_ids.begin(), worker_ids.end());
    auto num_workers = worker_ids.size();

    for (const auto & part : parts)
    {
        /// use crc64 as original implementation, may change to other hash later
        auto part_name = part->get_info().getBasicPartName();
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
std::unordered_map<String, DataPartsCnchVector> assignCnchPartsWithRingAndBalance(WorkerList worker_ids, const std::unordered_map<String, HostWithPorts> & worker_hosts, const ConsistentHashRing & ring, const DataPartsCnchVector & parts)
{
    LOG_INFO(&Poco::Logger::get("Consistent Hash"), "Start to allocate part with bounded ring based hash policy.");
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
        auto part_name = part->get_info().getBasicPartName();
        auto hash_val = fio_crc64(reinterpret_cast<const unsigned char *>(part_name.c_str()), part_name.length());
        disk_cache_worker_index = JumpConsistentHash(hash_val, num_workers);
        disk_cache_host_port = worker_hosts.at(worker_ids[disk_cache_worker_index]).getRPCAddress();

        if (auto host_id = ring.tryFind(part->get_info().getBasicPartName(), cap_limit, stats); !host_id.empty())
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
        auto host_id = ring.findAndRebalance(part->get_info().getBasicPartName(), cap_limit, stats);
        ret[host_id].emplace_back(part);

        auto assign_compute_host_port = worker_hosts.at(host_id).getRPCAddress();
        part->setHostPort(disk_cache_host_port, assign_compute_host_port);
        if (part->assign_compute_host_port != part->disk_cache_host_port)
                ProfileEvents::increment(ProfileEvents::CnchDiskCacheNodeUnLocalityParts, 1);
    }


    LOG_INFO(&Poco::Logger::get("Consistent Hash"),
             "Finish allocate part with bounded ring based hash policy, # of overloaded parts {}.", exceed_parts.size());
    return ret;
}

// 1 round approach
template <typename DataPartsCnchVector>
std::unordered_map<String, DataPartsCnchVector> assignCnchPartsWithStrictBoundedHash(WorkerList worker_ids, const std::unordered_map<String, HostWithPorts> & worker_hosts, const ConsistentHashRing & ring, const DataPartsCnchVector & parts, bool strict)
{
    LOG_INFO(&Poco::Logger::get("Strict Bounded Consistent Hash"), "Start to allocate part with bounded ring based hash policy under strict mode " + std::to_string(strict) + ".");
    std::unordered_map<String, DataPartsCnchVector> ret;
    size_t cap_limit = 0;
    std::unordered_map<String, UInt64> stats;

    sort(worker_ids.begin(), worker_ids.end());
    auto num_workers = worker_ids.size();

    for (size_t i = 0; i < parts.size(); ++i)
    {
        auto & part = parts[i];

        auto part_name = part->get_info().getBasicPartName();
        auto hash_val = fio_crc64(reinterpret_cast<const unsigned char *>(part_name.c_str()), part_name.length());
        auto index = JumpConsistentHash(hash_val, num_workers);

        cap_limit = ring.getCapLimit(i + 1, strict);
        auto host_id = ring.findAndRebalance(part->get_info().getBasicPartName(), cap_limit, stats);
        ret[host_id].emplace_back(part);

        auto disk_cache_host_port = worker_hosts.at(worker_ids[index]).getRPCAddress();
        auto assign_compute_host_port = worker_hosts.at(host_id).getRPCAddress();
        part->setHostPort(disk_cache_host_port, assign_compute_host_port);
        if (part->assign_compute_host_port != part->disk_cache_host_port)
                ProfileEvents::increment(ProfileEvents::CnchDiskCacheNodeUnLocalityParts, 1);
    }

    LOG_INFO(&Poco::Logger::get("Strict Bounded Consistent Hash"), "Finish allocate part with strict bounded ring based hash policy under strict mode " + std::to_string(strict) + ".");
    return ret;
}

HivePartsAssignMap assignCnchHiveParts(const WorkerGroupHandle & worker_group, const HiveFiles & parts)
{
    auto workers = worker_group->getWorkerIDVec();
    auto num_workers = workers.size();
    HivePartsAssignMap ret;

    for (const auto & file : parts)
    {
        auto idx = consistentHashForString(file->file_path, num_workers);
        ret[workers[idx]].emplace_back(file);
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

    std::for_each(parts.begin(), parts.end(), [&](auto part)
    {
        bool is_clustered = part->part_model().table_definition_hash() == storage.getTableHashForClusterBy() && part->part_model().bucket_number() != -1;
        if (is_clustered)
            bucket_parts.emplace_back(part);
        else
            leftover_parts.emplace_back(part);
    });

    return res;
}

void moveBucketTablePartsToAssignedParts(std::unordered_map<String, ServerDataPartsVector> & assigned_map, ServerDataPartsVector & bucket_parts, const WorkerList & workers, std::set<Int64> required_bucket_numbers)
{
    if(bucket_parts.empty())
        return;
    auto bucket_parts_assignment_map = assignCnchPartsForBucketTable(bucket_parts, workers, required_bucket_numbers).parts_assignment_map;
    for (auto & [worker_id, bucket_assigned_parts]: bucket_parts_assignment_map ) {
        auto & assigned_parts = assigned_map[worker_id];
        std::move(bucket_assigned_parts.begin(), bucket_assigned_parts.end(), std::back_inserter(assigned_parts));
    }
}



BucketNumberAndServerPartsAssignment assignCnchPartsForBucketTable(const ServerDataPartsVector & parts, WorkerList workers, std::set<Int64> required_bucket_numbers)
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
            auto index = bucket_number % workers.size();
            assignment.parts_assignment_map[workers[index]].emplace_back(part);
            assignment.bucket_number_assignment_map[workers[index]].insert(bucket_number);
        }
    }
    return assignment;
}

}
