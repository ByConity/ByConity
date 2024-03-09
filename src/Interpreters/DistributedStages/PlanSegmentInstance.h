#pragma once
#include <cstddef>
#include <limits>
#include <memory>
#include <optional>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <common/types.h>
namespace DB {
struct PlanSegmentInstanceId
{
    UInt32 segment_id = std::numeric_limits<UInt32>::max();
    UInt32 parallel_id = std::numeric_limits<UInt32>::max();
    bool operator==(const PlanSegmentInstanceId & other) const
    {
        return segment_id == other.segment_id && parallel_id == other.parallel_id;
    }
    bool operator<(const PlanSegmentInstanceId & other) const
    {
        if (segment_id < other.segment_id)
            return true;
        else if (segment_id > other.segment_id)
            return true;
        else
            return parallel_id < other.parallel_id;
    }
};

class PlanSegment;


struct PlanSegmentExecutionInfo
{
    UInt32 parallel_id = std::numeric_limits<UInt32>::max();
    AddressInfo execution_address;
    std::optional<size_t> source_task_index;
    std::optional<size_t> source_task_count;
};

struct PlanSegmentInstance
{
    PlanSegmentExecutionInfo info;
    std::unique_ptr<PlanSegment> plan_segment;
};

using PlanSegmentInstancePtr = std::unique_ptr<PlanSegmentInstance>;
}

template <>
struct std::hash<DB::PlanSegmentInstanceId>
{
    std::size_t operator()(const DB::PlanSegmentInstanceId & id) const
    {
        return (static_cast<size_t>(id.segment_id) << 32ULL) ^ id.parallel_id;
    }
};
