#pragma once
#include <limits>
#include <memory>
#include <common/types.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
namespace DB {
struct PlanSegmentInstanceId
{
    UInt32 segment_id = std::numeric_limits<UInt32>::max();
    UInt32 parallel_id = std::numeric_limits<UInt32>::max();
};

class PlanSegment;


struct PlanSegmentExecutionInfo
{
    UInt32 parallel_id = std::numeric_limits<UInt32>::max();
    AddressInfo execution_address;
};

struct PlanSegmentInstance
{
    PlanSegmentExecutionInfo info;
    std::unique_ptr<PlanSegment> plan_segment;
};

using PlanSegmentInstancePtr = std::unique_ptr<PlanSegmentInstance>;
}

