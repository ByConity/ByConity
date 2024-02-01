#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <CloudServices/CnchServerResource.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/DistributedStages/PlanSegmentInstance.h>
#include <Interpreters/DistributedStages/executePlanSegment.h>
#include <Interpreters/NodeSelector.h>
#include <Interpreters/Scheduler.h>
#include <Interpreters/WorkerStatusManager.h>
#include <Interpreters/sendPlanSegment.h>
#include <butil/iobuf.h>
#include <Common/Stopwatch.h>
#include <Common/time.h>
#include <common/types.h>

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
    return queue.tryPop(task, timespec_to_duration(query_context->getQueryExpirationTimeStamp()).count() / 1000);
}

void IScheduler::dispatchTask(PlanSegment * plan_segment_ptr, const SegmentTask & task, const size_t idx)
{
    const auto & selector_info = node_selector_result[task.task_id];
    const auto & worker_node = selector_info.worker_nodes[idx];
    PlanSegmentExecutionInfo execution_info;
    execution_info.parallel_id = idx;
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
        id_to_addr_iter->second.push_back(worker_node.address);
    }
    else
    {
        dag_graph_ptr->id_to_address.emplace(task.task_id, AddressInfos{worker_node.address});
    }
}

TaskResult IScheduler::scheduleTask(PlanSegment * plan_segment_ptr, const SegmentTask & task)
{
    TaskResult res;
    sendResource(plan_segment_ptr);
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

    PlanSegmentExecutionInfo execution_info;

    for (auto & plan_segment_input : plan_segment_ptr->getPlanSegmentInputs())
    {
        if (auto iter = selector_info.source_addresses.find(plan_segment_input->getPlanSegmentId());
            iter != selector_info.source_addresses.end())
        {
            plan_segment_input->insertSourceAddresses(iter->second.addresses, query_context->getSettingsRef().bsp_mode);
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

void IScheduler::schedule()
{
    Stopwatch sw;
    genTopology();
    genSourceTasks();

    /// Leave final segment alone.
    while (!dag_graph_ptr->plan_segment_status_ptr->is_final_stage_start)
    {
        if (stopped.load(std::memory_order_acquire))
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

void IScheduler::sendResource(PlanSegment * plan_segment_ptr)
{
    if (query_context->getSettingsRef().bsp_mode)
    {
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
    if (stopped.load(std::memory_order_acquire))
    {
        LOG_INFO(log, "Schedule interrupted before schedule final task");
        return;
    }
    PlanSegment * final_segment = dag_graph_ptr->getPlanSegmentPtr(dag_graph_ptr->final);

    const auto & final_address_info = getLocalAddress(*query_context);
    LOG_DEBUG(log, "Set address {} for final segment", final_address_info.toString());
    final_segment->setCoordinatorAddress(final_address_info);

    for (auto & plan_segment_input : final_segment->getPlanSegmentInputs())
    {
        // segment has more than one input which one is table
        if (plan_segment_input->getPlanSegmentType() != PlanSegmentType::EXCHANGE)
            continue;
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

/// BSP scheduler logic
void BSPScheduler::submitTasks(PlanSegment * plan_segment_ptr, const SegmentTask & task)
{
    (void)plan_segment_ptr;
    const auto selector_info = node_selector_result[task.task_id];
    for (size_t i = 0; i < selector_info.worker_nodes.size(); i++)
    {
        std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
        // Is it solid to check empty address via hostname?
        if (selector_info.worker_nodes[i].address.getHostName().empty())
        {
            pending_task_instances.no_prefs.emplace(task.task_id, i);
        }
        else
        {
            pending_task_instances.for_nodes[selector_info.worker_nodes[i].address].emplace(task.task_id, i);
        }
    }
    triggerDispatch(cluster_nodes.rank_workers);
}

void BSPScheduler::onSegmentFinished(const size_t & segment_id, bool is_succeed, bool is_canceled)
{
    if (is_succeed)
    {
        removeDepsAndEnqueueTask(SegmentTask{segment_id});
    }
    // on exception
    if (!is_succeed && !is_canceled)
    {
        stopped.store(true, std::memory_order_release);
        // emplace a fake task.
        addBatchTask(nullptr);
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

    AddressInfo available_worker;
    {
        std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
        available_worker = segment_parallel_locations[segment_id][parallel_index];
        occupied_workers[segment_id].erase(available_worker);
    }
    triggerDispatch({WorkerNode{available_worker}});
}

std::pair<bool, SegmentTaskInstance> BSPScheduler::getInstanceToSchedule(const AddressInfo & worker)
{
    if (pending_task_instances.for_nodes.contains(worker))
    {
        for (auto instance = pending_task_instances.for_nodes[worker].begin(); instance != pending_task_instances.for_nodes[worker].end();)
        {
            const auto & node_iter = occupied_workers.find(instance->task_id);
            // If the target node is busy, skip it.
            if (node_iter != occupied_workers.end() && node_iter->second.contains(worker))
            {
                instance++;
                continue;
            }
            else
            {
                pending_task_instances.for_nodes[worker].erase(instance);
                return std::make_pair(true, *instance);
            }
        }
    }

    for (auto no_pref = pending_task_instances.no_prefs.begin(); no_pref != pending_task_instances.no_prefs.end();)
    {
        const auto & node_iter = occupied_workers.find(no_pref->task_id);
        // If the target node is busy, skip it.
        if (node_iter != occupied_workers.end() && node_iter->second.contains(worker))
        {
            no_pref++;
            continue;
        }
        else
        {
            pending_task_instances.no_prefs.erase(no_pref);
            return std::make_pair(true, *no_pref);
        }
    }
    return std::make_pair(false, SegmentTaskInstance(0, 0));
}

void BSPScheduler::triggerDispatch(const std::vector<WorkerNode> & available_workers)
{
    for (const auto & worker : available_workers)
    {
        std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
        const auto & address = worker.address;
        const auto & [has_result, result] = getInstanceToSchedule(address);
        if (!has_result)
            continue;

        auto & worker_node = node_selector_result[result.task_id].worker_nodes[result.parallel_index];
        if (worker_node.address.getHostName().empty())
            worker_node.address = address;

        dispatchTask(dag_graph_ptr->getPlanSegmentPtr(result.task_id), SegmentTask{result.task_id}, result.parallel_index);

        const auto & node_iter = occupied_workers.find(result.task_id);
        if (node_iter != occupied_workers.end())
        {
            node_iter->second.insert(address);
        }
        else
        {
            occupied_workers.emplace(result.task_id, std::unordered_set<AddressInfo, AddressInfo::Hash>{address});
        }
        const auto & parallel_nodes_iter = segment_parallel_locations.find(result.task_id);
        if (parallel_nodes_iter != segment_parallel_locations.end())
        {
            parallel_nodes_iter->second.insert({result.parallel_index, address});
        }
        else
        {
            segment_parallel_locations.emplace(
                result.task_id, std::unordered_map<UInt64, AddressInfo>{std::make_pair(result.parallel_index, address)});
        }
    }
}
}
