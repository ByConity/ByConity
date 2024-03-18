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
#include <Protos/data_models.pb.h>
#include <Storages/CnchTablePartitionMetrics.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Common/RWLock.h>

namespace DB
{
namespace CacheStatus
{
    static constexpr UInt32 UINIT = 0;
    static constexpr UInt32 LOADING = 1;
    static constexpr UInt32 LOADED = 2;
}

class CnchPartitionInfo
{
public:
    explicit CnchPartitionInfo(
        const String & table_uuid_, const std::shared_ptr<MergeTreePartition> & partition_, const std::string & partition_id_, bool newly_inserted = false)
        : partition_ptr(partition_), partition_id(partition_id_), metrics_ptr(std::make_shared<PartitionMetrics>(table_uuid_, partition_id, newly_inserted))
    {
    }

    using PartitionLockHolder = RWLockImpl::LockHolder;

    std::shared_ptr<MergeTreePartition> partition_ptr;
    std::string partition_id;
    std::atomic<UInt32> cache_status = CacheStatus::UINIT;
    std::atomic<UInt32> delete_bitmap_cache_status = CacheStatus::UINIT;
    std::shared_ptr<PartitionMetrics> metrics_ptr;

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

using PartitionInfoPtr = std::shared_ptr<CnchPartitionInfo>;
using PartitionFullPtr = std::shared_ptr<CnchPartitionInfoFull>;
}
