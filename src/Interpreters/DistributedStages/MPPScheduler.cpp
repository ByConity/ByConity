#include "MPPScheduler.h"
#include <unordered_map>
#include "Interpreters/DistributedStages/PlanSegment.h"

namespace DB
{

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
        batch_task->emplace_back(leaf_id, true, dag_graph_ptr->table_scan_or_value_segments.contains(leaf_id));
        plansegment_topology.erase(leaf_id);
        LOG_TRACE(log, "Task for leaf segment {} generated", leaf_id);
    }

    while (!plansegment_topology.empty())
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
            batch_task->emplace_back(target_id, false, dag_graph_ptr->table_scan_or_value_segments.contains(target_id));
        }
    }
    addBatchTask(std::move(batch_task));
    LOG_DEBUG(log, "End generate batch tasks");
}

bool MPPScheduler::getBatchTaskToSchedule(BatchTaskPtr & task)
{
    auto now = time_in_milliseconds(std::chrono::system_clock::now());
    if (query_expiration_ms <= now)
        return false;
    else
        return queue.tryPop(task, query_expiration_ms - now);
}

PlanSegmentExecutionInfo MPPScheduler::schedule()
{
    Stopwatch sw;
    genTopology();
    genBatchTasks();

    /// Leave final segment alone.
    while (!dag_graph_ptr->plan_segment_status_ptr->is_final_stage_start)
    {
        auto curr = time_in_milliseconds(std::chrono::system_clock::now());
        if (curr > query_expiration_ms && !stopped.load(std::memory_order_relaxed))
        {
            throw Exception(
                fmt::format("schedule timeout, current ts {} expire ts {}", curr, query_expiration_ms), ErrorCodes::TIMEOUT_EXCEEDED);
        }
        /// nullptr means invalid task
        BatchTaskPtr batch_task;
        if (getBatchTaskToSchedule(batch_task) && batch_task)
        {
            for (auto task : *batch_task)
            {
                LOG_INFO(log, "Schedule segment {}", task.segment_id);
                if (task.segment_id == 0)
                {
                    prepareFinalTask();
                    break;
                }
                auto * plan_segment_ptr = dag_graph_ptr->getPlanSegmentPtr(task.segment_id);
                plan_segment_ptr->setCoordinatorAddress(local_address);
                scheduleTask(plan_segment_ptr, task);
                onSegmentScheduled(task);
            }
        }
        if (batch_schedule)
            batchScheduleTasks();
    }

    dag_graph_ptr->joinAsyncRpcAtLast();
    LOG_DEBUG(log, "Scheduling takes {} ms", sw.elapsedMilliseconds());
    return generateExecutionInfo(0, 0);
}

/// MPP schduler logic
void MPPScheduler::submitTasks(PlanSegment * plan_segment_ptr, const SegmentTask & task)
{
    const auto & selector_info = node_selector_result[task.segment_id];
    for (size_t idx = 0; idx < selector_info.worker_nodes.size(); idx++)
    {
        dispatchOrCollectTask(plan_segment_ptr, {task.segment_id, idx});
    }
}

PlanSegmentExecutionInfo MPPScheduler::generateExecutionInfo(size_t /*task_id*/, size_t index)
{
    PlanSegmentExecutionInfo execution_info;
    execution_info.parallel_id = index;
    return execution_info;
}

bool MPPScheduler::addBatchTask(BatchTaskPtr batch_task)
{
    return queue.push(batch_task);
}

void MPPScheduler::prepareTask(PlanSegment * plan_segment_ptr, NodeSelectorResult & selector_info, const SegmentTask & /*task*/)
{
    for (const auto & plan_segment_input : plan_segment_ptr->getPlanSegmentInputs())
    {
        if (auto iter = selector_info.source_addresses.find(plan_segment_input->getExchangeId());
            iter != selector_info.source_addresses.end())
        {
            for (const auto & addr : iter->second)
            {
                plan_segment_input->insertSourceAddress(*addr);
            }
        }
    }
}

void MPPScheduler::prepareFinalTaskImpl(PlanSegment * final_plan_segment, const AddressInfo & addr)
{
    NodeSelectorResult result{.worker_nodes = {WorkerNode(addr, NodeType::Local)}};
    std::map<PlanSegmentInstanceId, std::vector<UInt32>> read_partitions;
    node_selector.setSources(final_plan_segment, &result, read_partitions);
    LOG_DEBUG(log, "Set address {} for final segment, node_selector_result is {}", addr.toShortString(), result.toString());
    SegmentTask final_task(0, dag_graph_ptr->leaf_segments.contains(0), dag_graph_ptr->table_scan_or_value_segments.contains(0));
    prepareTask(final_plan_segment, result, final_task);
}
}
