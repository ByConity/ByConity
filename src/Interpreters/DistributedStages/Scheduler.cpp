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
#include <Common/Stopwatch.h>
#include <Common/time.h>
#include <common/types.h>

#include "Scheduler.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int WAIT_FOR_RESOURCE_TIMEOUT;
}

bool Scheduler::addBatchTask(BatchTaskPtr batch_task)
{
    return queue.push(batch_task);
}

bool Scheduler::getBatchTaskToSchedule(BatchTaskPtr & task)
{
    return queue.tryPop(task, timespec_to_duration(query_context->getQueryExpirationTimeStamp()).count() / 1000);
}

void Scheduler::dispatchTask(PlanSegment * plan_segment_ptr, const SegmentTask & task, const size_t idx)
{
    WorkerNode worker_node;
    NodeSelectorResult selector_info;
    {
        std::unique_lock<std::mutex> lock(node_selector_result_mutex);
        selector_info = node_selector_result[task.task_id];
        worker_node = selector_info.worker_nodes[idx];
    }
    PlanSegmentExecutionInfo execution_info = generateExecutionInfo(task.task_id, idx);
    std::shared_ptr<butil::IOBuf> plan_segment_buf_ptr;
    {
        std::unique_lock<std::mutex> lk(segment_bufs_mutex);
        plan_segment_buf_ptr = segment_bufs[task.task_id];
    }
    if (worker_node.type == NodeType::Local)
    {
        sendPlanSegmentToAddress(local_address, plan_segment_ptr, execution_info, query_context, dag_graph_ptr, plan_segment_buf_ptr);
    }
    else
    {
        const auto worker_group = query_context->tryGetCurrentWorkerGroup();
        sendPlanSegmentToAddress(
            worker_node.address,
            plan_segment_ptr,
            execution_info,
            query_context,
            dag_graph_ptr,
            plan_segment_buf_ptr,
            worker_group ? WorkerStatusManager::getWorkerId(worker_group->getVWName(), worker_group->getID(), worker_node.id) : WorkerId{});
    }

    if (const auto & id_to_addr_iter = dag_graph_ptr->id_to_address.find(task.task_id);
        id_to_addr_iter != dag_graph_ptr->id_to_address.end())
    {
        id_to_addr_iter->second.at(idx) = worker_node.address;
    }
    else
    {
        AddressInfos infos(selector_info.worker_nodes.size());
        dag_graph_ptr->id_to_address.emplace(task.task_id, std::move(infos));
        dag_graph_ptr->id_to_address[task.task_id].at(idx) = worker_node.address;
    }
}

TaskResult Scheduler::scheduleTask(PlanSegment * plan_segment_ptr, const SegmentTask & task)
{
    TaskResult res;
    sendResources(plan_segment_ptr);
    NodeSelectorResult selector_info;
    {
        std::unique_lock<std::mutex> lock(node_selector_result_mutex);
        auto selector_result = node_selector_result.emplace(task.task_id, node_selector.select(plan_segment_ptr, task.is_source));
        selector_info = selector_result.first->second;
    }
    prepareTask(plan_segment_ptr, selector_info.worker_nodes.size());
    dag_graph_ptr->scheduled_segments.emplace(task.task_id);
    dag_graph_ptr->segment_parallel_size_map[task.task_id] = selector_info.worker_nodes.size();

    PlanSegmentExecutionInfo execution_info;

    for (auto & plan_segment_input : plan_segment_ptr->getPlanSegmentInputs())
    {
        if (auto iter = selector_info.source_addresses.find(plan_segment_input->getPlanSegmentId());
            iter != selector_info.source_addresses.end())
        {
            plan_segment_input->insertSourceAddresses(iter->second.addresses);
        }
    }
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
            segment_bufs.emplace(task.task_id, std::move(plan_segment_buf_ptr));
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

void Scheduler::schedule()
{
    Stopwatch sw;
    genTopology();
    genSourceTasks();

    /// Leave final segment alone.
    while (!dag_graph_ptr->plan_segment_status_ptr->is_final_stage_start)
    {
        if (stopped.load(std::memory_order_relaxed))
        {
            LOG_INFO(log, "Schedule interrupted");
            return;
        }
        /// nullptr means invalid task
        BatchTaskPtr batch_task;
        if (getBatchTaskToSchedule(batch_task) && batch_task)
        {
            for (auto task : *batch_task)
            {
                LOG_DEBUG(log, "Schedule segment {}", task.task_id);
                if (task.task_id == 0)
                {
                    prepareFinalTask();
                    break;
                }
                auto * plan_segment_ptr = dag_graph_ptr->getPlanSegmentPtr(task.task_id);
                plan_segment_ptr->setCoordinatorAddress(local_address);
                scheduleTask(plan_segment_ptr, task);
                onSegmentScheduled(task);
            }
        }
    }

    dag_graph_ptr->joinAsyncRpcAtLast();
    LOG_DEBUG(log, "Scheduling takes {} ms", sw.elapsedMilliseconds());
}

void Scheduler::genSourceTasks()
{
    LOG_DEBUG(log, "Begin generate source tasks");
    auto batch_task = std::make_shared<BatchTask>();
    batch_task->reserve(dag_graph_ptr->sources.size());
    for (auto source_id : dag_graph_ptr->sources)
    {
        LOG_TRACE(log, "Generate task for source segment {}", source_id);
        if (source_id == dag_graph_ptr->final)
            continue;

        batch_task->emplace_back(source_id, true);
        plansegment_topology.erase(source_id);
        LOG_TRACE(log, "Task for source segment {} generated", source_id);
    }
    addBatchTask(std::move(batch_task));
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
            LOG_TRACE(log, "{} depends on {} by exchange_id:{}", id, depend_id, plan_segment_input->getExchangeId());
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
    LOG_DEBUG(log, "Set address {} for final segment", final_address_info.toString());
    final_segment->setCoordinatorAddress(final_address_info);

    NodeSelectorResult selector_info;
    if (query_context->getSettingsRef().bsp_mode)
    {
        bool is_source = dag_graph_ptr->sources.find(dag_graph_ptr->final) != dag_graph_ptr->sources.end();
        auto selector_result = node_selector_result.emplace(dag_graph_ptr->final, node_selector.select(final_segment, is_source));
        selector_info = selector_result.first->second;
    }

    for (auto & plan_segment_input : final_segment->getPlanSegmentInputs())
    {
        // segment has more than one input which one is table
        if (plan_segment_input->getPlanSegmentType() != PlanSegmentType::EXCHANGE)
            continue;
        if (query_context->getSettingsRef().bsp_mode)
        {
            if (auto iter = selector_info.source_addresses.find(plan_segment_input->getPlanSegmentId());
                iter != selector_info.source_addresses.end())
                plan_segment_input->insertSourceAddresses(iter->second.addresses);
        }
        else
        {
            auto address_it = dag_graph_ptr->id_to_address.find(plan_segment_input->getPlanSegmentId());
            if (address_it == dag_graph_ptr->id_to_address.end())
                throw Exception(
                    "Logical error: address of segment " + std::to_string(plan_segment_input->getPlanSegmentId()) + " not found",
                    ErrorCodes::LOGICAL_ERROR);
            if (plan_segment_input->getSourceAddresses().empty())
                plan_segment_input->insertSourceAddresses(address_it->second);
        }
    }
    dag_graph_ptr->plan_segment_status_ptr->is_final_stage_start = true;
}

void Scheduler::removeDepsAndEnqueueTask(const SegmentTask & task)
{
    std::lock_guard<std::mutex> guard(plansegment_topology_mutex);
    auto batch_task = std::make_shared<BatchTask>();
    const auto & task_id = task.task_id;
    LOG_TRACE(log, "Remove dependency {} for segments", task_id);

    for (auto & [id, dependencies] : plansegment_topology)
    {
        if (dependencies.erase(task_id))
            LOG_TRACE(log, "Erase dependency {} for segment {}", task_id, id);
        if (dependencies.empty())
        {
            batch_task->emplace_back(id);
        }
    }
    for (const auto & t : *batch_task)
    {
        plansegment_topology.erase(t.task_id);
    }
    addBatchTask(std::move(batch_task));
}

}
