#include "MPPScheduler.h"

namespace DB
{

void MPPScheduler::genTasks()
{
    genBatchTasks();
}

void MPPScheduler::genBatchTasks()
{
    LOG_DEBUG(log, "Begin generate batch tasks");
    auto batch_task = std::make_shared<BatchTask>();
    batch_task->reserve(dag_graph_ptr->leaf_segments.size());
    for (auto leaf_id : dag_graph_ptr->leaf_segments)
    {
        LOG_TRACE(log, "Generate task for leaf segment {}", leaf_id);
        if (leaf_id == dag_graph_ptr->final)
            continue;
        auto it = dag_graph_ptr->id_to_segment.find(leaf_id);
        if (it == dag_graph_ptr->id_to_segment.end())
            throw Exception("Logical error: source segment can not be found", ErrorCodes::LOGICAL_ERROR);

        for (auto & [target_id, source_set] : plansegment_topology)
        {
            if (source_set.erase(leaf_id))
                LOG_TRACE(log, "Find source id {} for target plansegment: {}", leaf_id, target_id);
        }
        batch_task->emplace_back(leaf_id, true);
        plansegment_topology.erase(leaf_id);
        LOG_TRACE(log, "Task for leaf segment {} generated", leaf_id);
    }

    while (plansegment_topology.size() > 0)
    {
        std::vector<size_t> target_ids;
        for (const auto & [id, source_set] : plansegment_topology)
        {
            if (source_set.empty())
                target_ids.emplace_back(id);
        }

        for (auto target_id : target_ids)
        {
            auto it = dag_graph_ptr->id_to_segment.find(target_id);
            if (it == dag_graph_ptr->id_to_segment.end())
                throw Exception("Logical error: plan segment segment can not be found", ErrorCodes::LOGICAL_ERROR);

            for (auto & [next_target_id, source_set] : plansegment_topology)
            {
                if (source_set.erase(target_id))
                    LOG_TRACE(log, "Find source id {} for target plansegment: {}", target_id, next_target_id);
            }
            plansegment_topology.erase(target_id);
            batch_task->emplace_back(target_id, false);
        }
    }
    addBatchTask(std::move(batch_task));
    LOG_DEBUG(log, "End generate batch tasks");
}

/// MPP schduler logic
void MPPScheduler::submitTasks(PlanSegment * plan_segment_ptr, const SegmentTask & task)
{
    const auto & selector_info = node_selector_result[task.task_id];
    for (size_t idx = 0; idx < selector_info.worker_nodes.size(); idx++)
    {
        dispatchOrSaveTask(plan_segment_ptr, task, idx);
    }
}

}
