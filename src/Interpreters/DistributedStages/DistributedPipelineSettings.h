#pragma once

#include <Interpreters/DistributedStages/AddressInfo.h>

namespace DB
{
class PlanSegment;

struct DistributedPipelineSettings
{
    bool is_distributed = false;
    String query_id{};
    size_t plan_segment_id = 0;
    size_t parallel_size = 1;
    AddressInfo coordinator_address{};
    AddressInfo current_address{};

    static DistributedPipelineSettings fromPlanSegment(PlanSegment * plan_segment);
};
}
