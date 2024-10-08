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
#include <common/getThreadId.h>

namespace DB
{
namespace CacheStatus
{
    static constexpr UInt32 UINIT = 0;
    static constexpr UInt32 LOADING = 1;
    static constexpr UInt32 LOADED = 2;
}

struct DataCacheStatus
{
    std::atomic<UInt32> load_status = CacheStatus::UINIT;
    std::atomic<UInt64> loading_thread_id = 0;

    bool isUnInit()
    {
        return load_status == CacheStatus::UINIT;
    }
    bool isLoaded()
    {
        return load_status == CacheStatus::LOADED;
    }
    bool isLoading()
    {
        return load_status == CacheStatus::LOADING;
    }
    bool isLoadingByCurrentThread()
    {
        return load_status == CacheStatus::LOADING && loading_thread_id == getThreadId();
    }

    /// calls to setXXX should always be protected by partition write lock
    void setToLoading()
    {
        load_status = CacheStatus::LOADING;
        loading_thread_id = getThreadId();
    }
    void setToLoaded()
    {
        load_status = CacheStatus::LOADED;
        loading_thread_id = 0;
    }
    void reset()
    {
        load_status = CacheStatus::UINIT;
        loading_thread_id = 0;
    }
};

class CnchPartitionInfo
{
public:
    explicit CnchPartitionInfo(
        const String & table_uuid_,
        const std::shared_ptr<MergeTreePartition> & partition_,
        const std::string & partition_id_,
        RWLock partition_lock,
        bool newly_inserted = false)
        : partition_ptr(partition_)
        , partition_id(partition_id_)
        , metrics_ptr(std::make_shared<PartitionMetrics>(table_uuid_, partition_id, newly_inserted))
        , partition_mutex(partition_lock)
    {
    }

    using PartitionLockHolder = RWLockImpl::LockHolder;

    std::shared_ptr<MergeTreePartition> partition_ptr;
    std::string partition_id;
    DataCacheStatus part_cache_status;
    DataCacheStatus delete_bitmap_cache_status;
    std::atomic<UInt64> gctime = {0};
    std::shared_ptr<PartitionMetrics> metrics_ptr;

    // cache of serialized partition value; only be set when query the MV table
    mutable std::optional<String> partition_value;

    inline PartitionLockHolder readLock() const
    {
        return partition_mutex->getLock(RWLockImpl::Read, RWLockImpl::NO_QUERY);
    }

    inline PartitionLockHolder writeLock() const
    {
        return partition_mutex->getLock(RWLockImpl::Write, RWLockImpl::NO_QUERY);
    }

    String getPartitionValue(const MergeTreeMetaBase & storage) const
    {
        {
            auto lock = readLock();
            if (partition_value)
                return *partition_value;
        }
        {
            auto lock = writeLock();
            if (!partition_value)
            {
                String partition_value_;
                WriteBufferFromString write_buffer(partition_value_);
                partition_ptr->store(storage, write_buffer);
                partition_value = partition_value_;
            }
            return *partition_value;
        }
    }

    /// For test only.
    RWLock getPartitionLock() {
        return partition_mutex;
    }

private:
    RWLock partition_mutex;
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
