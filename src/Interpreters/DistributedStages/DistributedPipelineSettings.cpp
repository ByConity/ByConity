#include <Interpreters/DistributedStages/DistributedPipelineSettings.h>

#include <Interpreters/DistributedStages/PlanSegment.h>

namespace DB
{
DistributedPipelineSettings DistributedPipelineSettings::fromPlanSegment(PlanSegment * plan_segment)
{
    DistributedPipelineSettings settings;
    settings.is_distributed = true;
    settings.query_id = plan_segment->getQueryId();
    settings.plan_segment_id = plan_segment->getPlanSegmentId();
    settings.parallel_size = plan_segment->getParallelSize();
    settings.coordinator_address = plan_segment->getCoordinatorAddress();
    settings.current_address = plan_segment->getCurrentAddress();
    return settings;
}
}
