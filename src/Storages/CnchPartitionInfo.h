#pragma once

#include <atomic>
#include <Core/Types.h>
#include <Storages/MergeTree/MergeTreePartition.h>

namespace DB
{
struct PartitionMetrics
{
    std::atomic<UInt64> total_parts_size {0};
    std::atomic<UInt64> total_parts_number {0};
    std::atomic<UInt64> total_rows_count {0};
};

enum class CacheStatus
{
    UINIT,
    LOADING,
    LOADED
};

class CnchPartitionInfo
{
public:
    explicit CnchPartitionInfo(const std::shared_ptr<MergeTreePartition> & partition_)
        : partition_ptr(partition_)
    {
    }

    std::shared_ptr<MergeTreePartition> partition_ptr;
    CacheStatus cache_status {CacheStatus::UINIT};
    std::shared_ptr<PartitionMetrics> metrics_ptr = std::make_shared<PartitionMetrics>();
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
