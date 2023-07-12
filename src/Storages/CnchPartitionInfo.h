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

#include <atomic>
#include <Core/Types.h>
#include <Common/RWLock.h>
#include <Protos/data_models.pb.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreePartition.h>

namespace DB
{
struct PartitionMetrics
{
    std::atomic_int64_t total_parts_size{0};
    std::atomic_int64_t total_parts_number{0};
    std::atomic_int64_t total_rows_count{0};
    std::atomic_bool has_bucket_number_neg_one{false}; // will be true if there is one part that has bucket_number == -1
    std::atomic_bool is_single_table_definition_hash{true}; // false if there are multiple table_definition_hash in this partition
    std::atomic_uint64_t table_definition_hash{0}; // will represent the single table_definition_hash if true, else any other table_definition_hash
    std::atomic_bool is_deleted{true}; // do not consider deleted parts

    PartitionMetrics & operator=(const PartitionMetrics & other)
    {
        total_parts_size = other.total_parts_size.load();
        total_parts_number = other.total_parts_number.load();
        total_rows_count = other.total_rows_count.load();
        return *this;
    }

    void update(const Protos::DataModelPart & part_model)
    {
        auto is_partial_part = part_model.part_info().hint_mutation();
        /// do not count partial part
        if (is_partial_part)
            return;

        auto is_deleted_part = part_model.has_deleted() && part_model.deleted();

        /// To minimize costs, we don't calculate part visibility when updating PartitionMetrics. For those parts marked as delete,
        /// just subtract them from statistics. And the non-deleted parts added into statistics. The non-deleted parts are added into
        /// statistics. For drop range, we have save all coreved parts info into it, it can be processed as deleted part directly.

        if (is_deleted_part)
        {
            total_rows_count -= (part_model.has_covered_parts_rows() ? part_model.covered_parts_rows() : part_model.rows_count());
            total_parts_size -= (part_model.has_covered_parts_size() ? part_model.covered_parts_size() : part_model.size());
            total_parts_number -= (part_model.has_covered_parts_count() ? part_model.covered_parts_count() : 1);
        }
        else
        {
            total_rows_count += part_model.rows_count();
            total_parts_size += part_model.size();
            total_parts_number += 1;
            if (part_model.bucket_number() == -1)
                has_bucket_number_neg_one = true;
            if (table_definition_hash != 0 && table_definition_hash != part_model.table_definition_hash())
                is_single_table_definition_hash = false;
            table_definition_hash = part_model.table_definition_hash();
            is_deleted = false;
        }
    }

    bool validateMetrics() { return total_rows_count >= 0 && total_parts_size >= 0 && total_parts_number >= 0; }
};

namespace CacheStatus
{
    static constexpr UInt32 UINIT = 0;
    static constexpr UInt32 LOADING = 1;
    static constexpr UInt32 LOADED = 2;
}

class CnchPartitionInfo
{
public:
    explicit CnchPartitionInfo(const std::shared_ptr<MergeTreePartition> & partition_, const std::string & partition_id_)
        : partition_ptr(partition_), partition_id(partition_id_)
    {
    }

    typedef RWLockImpl::LockHolder PartitionLockHolder;

    std::shared_ptr<MergeTreePartition> partition_ptr;
    std::string partition_id;
    std::atomic<UInt32> cache_status = CacheStatus::UINIT;
    std::shared_ptr<PartitionMetrics> metrics_ptr = std::make_shared<PartitionMetrics>();

    inline PartitionLockHolder readLock() const
    {
        return partition_mutex->getLock(RWLockImpl::Read, CurrentThread::getQueryId().toString());
    }

    inline PartitionLockHolder writeLock() const
    {
        return partition_mutex->getLock(RWLockImpl::Write, CurrentThread::getQueryId().toString());
    }

private:
    RWLock partition_mutex = RWLockImpl::create();
};

/***
 * Used when get partition metrics. We will fill the partition and first_partition without modify CnchPartitionInfo in cache
 */
class CnchPartitionInfoFull
{
public:
    explicit CnchPartitionInfoFull(const std::shared_ptr<CnchPartitionInfo> & parition_info)
        : partition_info_ptr(parition_info)
    {
    }
    String partition;
    String first_partition;
    std::shared_ptr<CnchPartitionInfo> partition_info_ptr;
};

using PartitionMetricsPtr = std::shared_ptr<PartitionMetrics>;
using PartitionInfoPtr = std::shared_ptr<CnchPartitionInfo>;
using PartitionFullPtr = std::shared_ptr<CnchPartitionInfoFull>;
}
