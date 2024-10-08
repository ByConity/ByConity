#pragma once
#include <cstddef>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/SourceTask.h>
#include <common/types.h>


namespace DB {
struct PlanSegmentInstanceId
{
    UInt32 segment_id = std::numeric_limits<UInt32>::max();
    UInt32 parallel_index = std::numeric_limits<UInt32>::max();
    bool operator==(const PlanSegmentInstanceId & other) const
    {
        return segment_id == other.segment_id && parallel_index == other.parallel_index;
    }
    bool operator<(const PlanSegmentInstanceId & other) const
    {
        if (segment_id < other.segment_id)
            return true;
        else if (segment_id > other.segment_id)
            return false;
        else
            return parallel_index < other.parallel_index;
    }
    String toString() const
    {
        return fmt::format("instance_id[{}, {}]", segment_id, parallel_index);
    }
};

struct PlanSegmentInstanceAttempt
{
    UInt32 segment_id = std::numeric_limits<UInt32>::max();
    UInt32 parallel_index = std::numeric_limits<UInt32>::max();
    UInt32 attempt_id = std::numeric_limits<UInt32>::max();
    bool operator==(const PlanSegmentInstanceAttempt & other) const
    {
        return segment_id == other.segment_id && parallel_index == other.parallel_index && attempt_id == other.attempt_id;
    }
    String toString() const
    {
        return fmt::format("instance_attempt[{}, {}, {}]", segment_id, parallel_index, attempt_id);
    }

    class Hash
    {
    public:
        size_t operator()(const PlanSegmentInstanceAttempt & key) const
        {
            return (key.segment_id << 13) + (key.parallel_index << 6) + key.attempt_id;
        }
    };
};

class PlanSegment;


struct PlanSegmentExecutionInfo
{
    UInt32 parallel_id = std::numeric_limits<UInt32>::max();
    AddressInfo execution_address;
    SourceTaskFilter source_task_filter;
    UInt32 attempt_id = std::numeric_limits<UInt32>::max();
    std::unordered_map<UInt64, std::vector<PlanSegmentMultiPartitionSource>> sources;
    UInt32 worker_epoch{0};
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
        return (static_cast<size_t>(id.segment_id) << 32ULL) ^ id.parallel_index;
    }
};
