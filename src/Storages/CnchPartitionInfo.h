#pragma once

#include <atomic>
#include <Core/Types.h>
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
        }
    }

    bool validateMetrics() { return total_rows_count >= 0 && total_parts_size >= 0 && total_parts_number >= 0; }
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
