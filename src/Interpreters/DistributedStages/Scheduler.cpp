#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/DistributedStages/PlanSegmentInstance.h>
#include <Interpreters/DistributedStages/executePlanSegment.h>
#include <Interpreters/NodeSelector.h>
#include <Interpreters/WorkerStatusManager.h>
#include <Interpreters/sendPlanSegment.h>
#include <butil/iobuf.h>
#include <fmt/core.h>
#include <Common/Stopwatch.h>
#include <Common/time.h>
#include <common/types.h>

#include "Scheduler.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void Scheduler::dispatchOrCollectTask(PlanSegment * plan_segment_ptr, const SegmentTaskInstance & task)
{
    WorkerNode worker_node;
    NodeSelectorResult selector_info;
    auto idx = task.parallel_index;
    {
        std::unique_lock<std::mutex> lock(node_selector_result_mutex);
        selector_info = node_selector_result[task.segment_id];
        worker_node = selector_info.worker_nodes[idx];
    }
    PlanSegmentExecutionInfo execution_info = generateExecutionInfo(task.segment_id, idx);
    std::shared_ptr<butil::IOBuf> plan_segment_buf_ptr;
    {
        std::unique_lock<std::mutex> lk(segment_bufs_mutex);
        plan_segment_buf_ptr = segment_bufs[task.segment_id];
    }
    AddressInfo address = local_address;
    WorkerId worker_id;
    if (worker_node.type != NodeType::Local)
    {
        address = worker_node.address;
        const auto worker_group = query_context->tryGetCurrentWorkerGroup();
        if (worker_group)
            worker_id = WorkerStatusManager::getWorkerId(worker_group->getVWName(), worker_group->getID(), worker_node.id);
    }
    if (batch_schedule)
    {
        PlanSegmentHeader header
            = {.instance_id = {static_cast<UInt32>(plan_segment_ptr->getPlanSegmentId()), execution_info.parallel_id},
               .plan_segment_buf_size = plan_segment_buf_ptr->size(),
               .plan_segment_buf_ptr = plan_segment_buf_ptr};
        batch_segment_headers[{address, worker_id}].emplace_back(std::move(header));
    }
    else
    {
        // NodeType::Local can be optimize
        send_plan_segment_func(address, plan_segment_ptr, execution_info, query_context, dag_graph_ptr, plan_segment_buf_ptr, worker_id);
    }

    if (const auto & id_to_addr_iter = dag_graph_ptr->id_to_address.find(task.segment_id);
        id_to_addr_iter != dag_graph_ptr->id_to_address.end())
    {
        id_to_addr_iter->second.at(idx) = worker_node.address;
    }
    else
    {
        AddressInfos infos(selector_info.worker_nodes.size());
        dag_graph_ptr->id_to_address.emplace(task.segment_id, std::move(infos));
        dag_graph_ptr->id_to_address[task.segment_id].at(idx) = worker_node.address;
    }
}

TaskResult Scheduler::scheduleTask(PlanSegment * plan_segment_ptr, const SegmentTask & task)
{
    TaskResult res;
    sendResources(plan_segment_ptr);
    NodeSelectorResult selector_info = selectNodes(plan_segment_ptr, task);
    prepareTask(plan_segment_ptr, selector_info, task);
    dag_graph_ptr->scheduled_segments.emplace(task.segment_id);
    dag_graph_ptr->segment_parallel_size_map[task.segment_id] = selector_info.worker_nodes.size();

    std::shared_ptr<butil::IOBuf> plan_segment_buf_ptr;
    if (!dag_graph_ptr->query_common_buf.empty())
    {
        plan_segment_buf_ptr = std::make_shared<butil::IOBuf>();
        butil::IOBufAsZeroCopyOutputStream wrapper(plan_segment_buf_ptr.get());
        Protos::PlanSegment plansegment_proto;
        plan_segment_ptr->toProto(plansegment_proto);
        plansegment_proto.SerializeToZeroCopyStream(&wrapper);
        {
            std::unique_lock<std::mutex> lk(segment_bufs_mutex);
            segment_bufs.emplace(task.segment_id, std::move(plan_segment_buf_ptr));
        }
    }
    else
    {
        // no need to setCoordinatorAddress since this addersss is already stored at query_common_buf
        plan_segment_ptr->setCoordinatorAddress(local_address);
    }

    submitTasks(plan_segment_ptr, task);

    dag_graph_ptr->joinAsyncRpcPerStage();
    res.status = TaskStatus::Success;
    return res;
}

void Scheduler::batchScheduleTasks()
{
    for (const auto & iter : batch_segment_headers)
        sendPlanSegmentsToAddress(iter.first.address_info, iter.second, query_context, dag_graph_ptr, iter.first.worker_id);
    batch_segment_headers.clear();
}

void Scheduler::genTopology()
{
    LOG_DEBUG(log, "Generate dependency topology for segments");
    for (const auto & [id, plan_segment_ptr] : dag_graph_ptr->id_to_segment)
    {
        auto & current = plansegment_topology[id];
        for (auto & plan_segment_input : plan_segment_ptr->getPlanSegmentInputs())
        {
            if (plan_segment_input->getPlanSegmentType() == PlanSegmentType::SOURCE)
            {
                // segment has more than one input which one is table
                continue;
            }
            auto depend_id = plan_segment_input->getPlanSegmentId();
            current.emplace(depend_id);
            LOG_INFO(log, "Segment {} depends on {} by exchange_id {}", id, depend_id, plan_segment_input->getExchangeId());
        }
    }
}

void Scheduler::prepareFinalTask()
{
    if (stopped.load(std::memory_order_relaxed))
    {
        LOG_INFO(log, "Schedule interrupted before schedule final task");
        return;
    }
    PlanSegment * final_segment = dag_graph_ptr->getPlanSegmentPtr(dag_graph_ptr->final);

    const auto & final_address_info = getLocalAddress(*query_context);
    final_segment->setCoordinatorAddress(final_address_info);
    prepareFinalTaskImpl(final_segment, final_address_info);

    dag_graph_ptr->plan_segment_status_ptr->is_final_stage_start = true;
}

void Scheduler::removeDepsAndEnqueueTask(size_t task_id)
{
    std::lock_guard<std::mutex> guard(plansegment_topology_mutex);
    auto batch_task = std::make_shared<BatchTask>();
    LOG_INFO(log, "Remove dependency {} for segments", task_id);

    for (auto & [id, dependencies] : plansegment_topology)
    {
        if (dependencies.erase(task_id))
            LOG_INFO(log, "Erase dependency {} for segment {}", task_id, id);
        if (dependencies.empty())
        {
            batch_task->emplace_back(
                id, dag_graph_ptr->leaf_segments.contains(id), dag_graph_ptr->table_scan_or_value_segments.contains(id));
        }
    }
    for (const auto & t : *batch_task)
    {
        plansegment_topology.erase(t.segment_id);
    }
    addBatchTask(std::move(batch_task));
}

}
