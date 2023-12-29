#include <chrono>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <CloudServices/CnchServerResource.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/DistributedStages/executePlanSegment.h>
#include <Interpreters/Scheduler.h>
#include <Interpreters/sendPlanSegment.h>
#include "common/types.h"
#include <Common/Stopwatch.h>
#include <Common/time.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int WAIT_FOR_RESOURCE_TIMEOUT;
}

bool IScheduler::addBatchTask(BatchTaskPtr batch_task)
{
    return queue.push(batch_task);
}

bool IScheduler::getBatchTaskToSchedule(BatchTaskPtr & task)
{
    return queue.pop(task);
}

TaskResult IScheduler::scheduleTask(const SegmentTask & task, PlanSegment * plan_segment_ptr)
{
    TaskResult res;
    sendResource(task.task_id, plan_segment_ptr);
    if (query_context->getSettingsRef().bsp_mode)
    {
        for (const auto & output : plan_segment_ptr->getPlanSegmentOutputs())
        {
            query_context->getExchangeDataTracker()->registerExchange(
                query_context->getCurrentQueryId(), output->getExchangeId(), plan_segment_ptr->getParallelSize());
        }
    }
    auto selector_result = node_selector_result.emplace(task.task_id, node_selector.select(plan_segment_ptr, task.is_source));
    dag_graph_ptr->scheduled_segments.emplace(task.task_id);
    auto & selector_info = selector_result.first->second;
    dag_graph_ptr->segment_paralle_size_map.emplace(task.task_id, selector_info.worker_nodes.size());

    for (auto & plan_segment_input : plan_segment_ptr->getPlanSegmentInputs())
    {
        if (auto iter = selector_info.source_addresses.find(plan_segment_input->getPlanSegmentId());
            iter != selector_info.source_addresses.end())
        {
            plan_segment_input->insertSourceAddresses(iter->second.addresses, query_context->getSettingsRef().bsp_mode);
        }
    }

    std::unordered_set<size_t> idx_to_schedule;
    for (size_t i = 0; i < selector_info.worker_nodes.size(); i++)
        idx_to_schedule.insert(i);
    while (!idx_to_schedule.empty())
    {
        std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
        if (!has_available_worker
            && !nodes_alloc_cv.wait_until(
                lk, timespec_to_timepoint(query_context->getQueryExpirationTimeStamp()), [this]() { return has_available_worker; }))
            throw Exception("Wait for available workers timedout", ErrorCodes::WAIT_FOR_RESOURCE_TIMEOUT);
        for (auto idx_iter = idx_to_schedule.begin(); idx_iter != idx_to_schedule.end();)
        {
            const auto & idx = *idx_iter;
            auto & worker_node = selector_info.worker_nodes[idx];
            const auto & iter = busy_nodes.find(task.task_id);
            // If the target node is busy, skip it.
            if (iter != busy_nodes.end() && iter->second.contains(worker_node.address))
            {
                idx_iter++;
                continue;
            }
            for (auto & plan_segment_input : plan_segment_ptr->getPlanSegmentInputs())
            {
                auto source_iter = selector_info.source_addresses.find(plan_segment_input->getPlanSegmentId());
                if (source_iter != selector_info.source_addresses.end() && source_iter->second.parallel_index)
                {
                    plan_segment_input->setParallelIndex(*source_iter->second.parallel_index);
                }
                else
                    plan_segment_input->setParallelIndex(selector_info.indexes[idx]);
            }
            plan_segment_ptr->setParallelIndex(idx);
            if (worker_node.type == NodeType::Local)
            {
                sendPlanSegmentToLocal(plan_segment_ptr, query_context, dag_graph_ptr);
            }
            else
            {
                const auto worker_group = query_context->tryGetCurrentWorkerGroup();
                sendPlanSegmentToRemote(
                    worker_node.address,
                    query_context,
                    plan_segment_ptr,
                    dag_graph_ptr,
                    worker_group ? WorkerStatusManager::getWorkerId(worker_group->getVWName(), worker_group->getID(), worker_node.id)
                                 : WorkerId{});
            }
            if (iter != busy_nodes.end())
            {
                iter->second.insert(worker_node.address);
            }
            else
            {
                std::unordered_set<AddressInfo, AddressInfo::Hash> nodes{worker_node.address};
                busy_nodes.emplace(task.task_id, std::move(nodes));
            }
            const auto & parallel_nodes_iter = segment_parallel_nodes.find(task.task_id);
            if (parallel_nodes_iter != segment_parallel_nodes.end())
            {
                parallel_nodes_iter->second.insert({idx, worker_node.address});
            }
            else
            {
                std::unordered_map<UInt64, AddressInfo> info{std::make_pair(idx, worker_node.address)};
                segment_parallel_nodes.emplace(task.task_id, std::move(info));
            }

            idx_iter = idx_to_schedule.erase(idx_iter);
        }
        has_available_worker = false;
    }
    {
        std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
        has_available_worker = true;
    }
    dag_graph_ptr->joinAsyncRpcPerStage();
    res.status = TaskStatus::Success;
    return res;
}

void IScheduler::schedule()
{
    Stopwatch sw;
    genTopology();
    genSourceTasks();
    BatchTaskPtr batch_task;
    /// Leave final segment alone.
    while (true)
    {
        if (dag_graph_ptr->plan_segment_status_ptr->is_final_stage_start)
            break;
        getBatchTaskToSchedule(batch_task);
        if (stopped)
        {
            LOG_INFO(log, "Schedule interrupted");
            return;
        }
        for (auto task : *batch_task)
        {
            auto iter = dag_graph_ptr->id_to_segment.find(task.task_id);

            if (iter == dag_graph_ptr->id_to_segment.end())
            {
                throw Exception("Logical error: segment " + std::to_string(task.task_id) + " not found", ErrorCodes::LOGICAL_ERROR);
            }
            LOG_DEBUG(log, "Schedule segment {}", task.task_id);
            if (task.task_id == 0)
            {
                prepareFinalTask();
                break;
            }
            auto plan_segment_ptr = iter->second;
            auto local_address = getLocalAddress(query_context);
            plan_segment_ptr->setCoordinatorAddress(local_address);
            scheduleTask(task, plan_segment_ptr);
            onSegmentScheduled(task);
        }
    }

    dag_graph_ptr->joinAsyncRpcAtLast();
    LOG_DEBUG(log, "Scheduling takes {} ms", sw.elapsedMilliseconds());
}

void IScheduler::genSourceTasks()
{
    LOG_DEBUG(log, "Begin generate source tasks");
    auto batch_task = std::make_shared<BatchTask>();
    batch_task->reserve(dag_graph_ptr->sources.size());
    for (auto source_id : dag_graph_ptr->sources)
    {
        LOG_TRACE(log, "Generate task for source segment {}", source_id);
        if (source_id == dag_graph_ptr->final)
            continue;
        auto it = dag_graph_ptr->id_to_segment.find(source_id);
        if (it == dag_graph_ptr->id_to_segment.end())
            throw Exception("Logical error: source segment" + std::to_string(source_id) + " not found", ErrorCodes::LOGICAL_ERROR);

        batch_task->emplace_back(source_id, true);
        plansegment_topology.erase(source_id);
        LOG_TRACE(log, "Task for source segment {} generated", source_id);
    }
    addBatchTask(std::move(batch_task));
}

void IScheduler::genTopology()
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

void IScheduler::sendResource(SegmentTask task, PlanSegment * plan_segment_ptr)
{
    if (query_context->getSettingsRef().bsp_mode)
    {
        LOG_TRACE(log, "Send resource for segment {}", task.task_id);
        ResourceOption option;
        for (auto & plan_segment_input : plan_segment_ptr->getPlanSegmentInputs())
        {
            auto storage_id = plan_segment_input->getStorageID();
            if (storage_id && storage_id->hasUUID())
            {
                option.table_ids.emplace(storage_id->uuid);
                LOG_TRACE(log, "Storage id {}", storage_id->getFullTableName());
            }
        }
        if (!option.table_ids.empty())
        {
            query_context->getCnchServerResource()->setSendMutations(true);
            query_context->getCnchServerResource()->sendResources(query_context, option);
        }
    }
}

void IScheduler::prepareFinalTask()
{
    if (stopped)
    {
        LOG_INFO(log, "Schedule interrupted before schedule final task");
        return;
    }
    auto final_it = dag_graph_ptr->id_to_segment.find(dag_graph_ptr->final);
    if (final_it == dag_graph_ptr->id_to_segment.end())
        throw Exception("Logical error: final segment is not found", ErrorCodes::LOGICAL_ERROR);

    const auto & final_address_info = getLocalAddress(query_context);
    LOG_DEBUG(log, "Set address {} for final segment", final_address_info.toString());
    final_it->second->setCurrentAddress(final_address_info);
    final_it->second->setCoordinatorAddress(final_address_info);
    final_it->second->setParallelIndex(0);

    for (auto & plan_segment_input : final_it->second->getPlanSegmentInputs())
    {
        // segment has more than one input which one is table
        if (plan_segment_input->getPlanSegmentType() != PlanSegmentType::EXCHANGE)
            continue;
        plan_segment_input->setParallelIndex(0);
        auto address_it = dag_graph_ptr->id_to_address.find(plan_segment_input->getPlanSegmentId());
        if (address_it == dag_graph_ptr->id_to_address.end())
            throw Exception(
                "Logical error: address of segment " + std::to_string(plan_segment_input->getPlanSegmentId()) + " not found",
                ErrorCodes::LOGICAL_ERROR);
        if (plan_segment_input->getSourceAddresses().empty())
            plan_segment_input->insertSourceAddresses(address_it->second, query_context->getSettingsRef().bsp_mode);
    }
    dag_graph_ptr->plan_segment_status_ptr->is_final_stage_start = true;
}

void IScheduler::removeDepsAndEnqueueTask(const SegmentTask & task)
{
    std::lock_guard<std::mutex> guard(plansegment_topology_mutex);
    auto batch_task = std::make_shared<BatchTask>();
    const auto & task_id = task.task_id;
    LOG_TRACE(log, "Remove dependency {} for segments", task_id);
    auto it = dag_graph_ptr->id_to_segment.find(task_id);
    if (it == dag_graph_ptr->id_to_segment.end())
        throw Exception(String("Logical error: segment ") + std::to_string(task_id) + " not found", ErrorCodes::LOGICAL_ERROR);

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

/// MPP schduler logic
void MPPScheduler::onSegmentScheduled(const SegmentTask & task)
{
    removeDepsAndEnqueueTask(task);
}

/// BSP scheduler logic
void BSPScheduler::onSegmentFinished(const size_t & segment_id, bool is_succeed, bool is_canceled)
{
    if (is_succeed)
    {
        removeDepsAndEnqueueTask({segment_id});
    }
    // on exception
    if (!is_succeed && !is_canceled)
    {
        stopped = true;
        // emplace a fake task.
        auto batch_task = std::make_shared<BatchTask>();
        batch_task->emplace_back(0);
        addBatchTask(std::move(batch_task));
    }
}

void BSPScheduler::onQueryFinished()
{
    UInt64 query_unique_id = query_context->getCurrentTransactionID().toUInt64();
    for (const auto & address : dag_graph_ptr->plan_send_addresses)
    {
        try
        {
            cleanupExchangeDataForQuery(address, query_unique_id);
            LOG_TRACE(
                log,
                "cleanup exchange data successfully query_id:{} query_unique_id:{} address:{}",
                query_id,
                query_unique_id,
                address.toString());
        }
        catch (...)
        {
            tryLogCurrentException(
                log,
                fmt::format(
                    "cleanup exchange data failed for query_id:{} query_unique_id:{} address:{}",
                    query_id,
                    query_unique_id,
                    address.toString()));
        }
    }
}

void BSPScheduler::updateSegmentStatusCounter(const size_t & segment_id, const UInt64 & parallel_index)
{
    std::unique_lock<std::mutex> lock(segment_status_counter_mutex);
    auto segment_status_counter_iterator = segment_status_counter.find(segment_id);
    if (segment_status_counter_iterator == segment_status_counter.end())
    {
        segment_status_counter[segment_id] = {};
    }
    segment_status_counter[segment_id].insert(parallel_index);

    if (segment_status_counter[segment_id].size() == dag_graph_ptr->segment_paralle_size_map[segment_id])
    {
        onSegmentFinished(segment_id, /*is_succeed=*/true, /*is_canceled=*/true);
    }

    std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
    const auto & addr = segment_parallel_nodes[segment_id][parallel_index];
    busy_nodes[segment_id].erase(addr);
    has_available_worker = true;
    nodes_alloc_cv.notify_all();
}
}
