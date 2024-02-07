#include "MPPScheduler.h"

namespace DB
{

/// MPP schduler logic
void MPPScheduler::submitTasks(PlanSegment * plan_segment_ptr, const SegmentTask & task)
{
    const auto & selector_info = node_selector_result[task.task_id];
    for (size_t idx = 0; idx < selector_info.worker_nodes.size(); idx++)
    {
        dispatchTask(plan_segment_ptr, task, idx);
    }
}

void MPPScheduler::onSegmentScheduled(const SegmentTask & task)
{
    removeDepsAndEnqueueTask(task);
}

}
