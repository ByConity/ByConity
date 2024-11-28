#include "BSPScheduler.h"

#include <atomic>
#include <initializer_list>
#include <limits>
#include <memory>
#include <mutex>
#include <CloudServices/CnchServerResource.h>
#include <bthread/mutex.h>
#include <Poco/Logger.h>
#include <Common/CurrentThread.h>
#include <Common/ErrorCodes.h>
#include <Common/ProfileEvents.h>
#include <common/logger_useful.h>

#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/ExchangeMode.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/DistributedStages/PlanSegmentInstance.h>
#include <Interpreters/DistributedStages/RuntimeSegmentsStatus.h>
#include <Interpreters/DistributedStages/ScheduleEvent.h>
#include <Interpreters/DistributedStages/Scheduler.h>
#include <Interpreters/NodeSelector.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/QueryPlan.h>
#include <QueryPlan/TableWriteStep.h>
#include <ResourceManagement/ResourceManagerClient.h>

#include <cstddef>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <CloudServices/CnchServerResource.h>

namespace ProfileEvents
{
extern const Event QueryBspRetryCount;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int WORKER_RESTARTED;
    extern const int EPOCH_MISMATCH;
}

std::unordered_set<int> BSPScheduler::unrecoverable_reasons{ErrorCodes::LOGICAL_ERROR, ErrorCodes::QUERY_WAS_CANCELLED};

/// BSP scheduler logic
bool BSPScheduler::postEvent(std::shared_ptr<ScheduleEvent> event)
{
    return queue.push(event);
}

bool BSPScheduler::postHighPriorityEvent(std::shared_ptr<ScheduleEvent> event)
{
    return high_priority_queue.push(event);
}

bool BSPScheduler::getEventToProcess(std::shared_ptr<ScheduleEvent> & event)
{
    if (!high_priority_queue.empty())
        return high_priority_queue.pop(event);
    auto now = time_in_milliseconds(std::chrono::system_clock::now());
    if (query_expiration_ms <= now)
        return false;
    else
        return queue.tryPop(event, query_expiration_ms - now);
}

bool BSPScheduler::hasEvent() const
{
    return !high_priority_queue.empty() || !queue.empty();
}

bool BSPScheduler::processEvent(const ScheduleEvent & event)
{
    switch (event.getType())
    {
        case ScheduleEventType::Abort: {
            if (stopped.load(std::memory_order_relaxed))
            {
                finish_segment_instance_cv.notify_all();
                const auto & abort = dynamic_cast<const AbortEvent &>(event);
                if (abort.error_msg.empty())
                {
                    LOG_INFO(log, "Schedule interrupted");
                    return true;
                }
                else
                {
                    throw Exception(abort.error_msg, abort.code);
                }
            }
            break;
        }
        case ScheduleEventType::ResendResource: {
            const auto & resend_event = dynamic_cast<const ResendResourceEvent &>(event);
            resendResource(resend_event.host_ports);
            break;
        }
        case ScheduleEventType::ScheduleBatchTask: {
            handleScheduleBatchTaskEvent(event);
            break;
        }
        case ScheduleEventType::TriggerDispatch: {
            handleTriggerDispatchEvent(event);
            break;
        }
        case ScheduleEventType::WorkerRestarted: {
            handleWorkerRestartedEvent(event);
            break;
        }
        case ScheduleEventType::SegmentInstanceFinished: {
            handleSegmentInstanceFinishedEvent(event);
            break;
        }
        case ScheduleEventType::SendResourceRequest: {
            handleSendResourceRequestEvent(event);
            break;
        }
        case ScheduleEventType::ResourceRequestGranted: {
            handleResourceRequestGrantedEvent(event);
            break;
        }
        default:
            throw Exception(fmt::format("Unexpected event type {}", event.getType()), ErrorCodes::LOGICAL_ERROR);
    }
    return false;
}

void BSPScheduler::genLeafTasks()
{
    LOG_DEBUG(log, "Begin generate leaf tasks");
    auto batch_task = std::make_shared<BatchTask>();
    batch_task->reserve(dag_graph_ptr->leaf_segments.size());
    for (auto leaf_id : dag_graph_ptr->leaf_segments)
    {
        LOG_TRACE(log, "Generate task for leaf segment {}", leaf_id);
        if (leaf_id == dag_graph_ptr->final)
            continue;

        batch_task->emplace_back(leaf_id, true, dag_graph_ptr->table_scan_or_value_segments.contains(leaf_id));
        plansegment_topology.erase(leaf_id);
        LOG_TRACE(log, "Task for leaf segment {} generated", leaf_id);
    }
    addBatchTask(std::move(batch_task));
}

BSPScheduler::~BSPScheduler() = default;

PlanSegmentExecutionInfo BSPScheduler::schedule()
{
    Stopwatch sw;
    genTopology();
    genLeafTasks();

    /// Leave final segment alone.
    while (!dag_graph_ptr->plan_segment_status_ptr->is_final_stage_start)
    {
        auto curr = time_in_milliseconds(std::chrono::system_clock::now());
        if (curr > query_expiration_ms && !stopped.load(std::memory_order_relaxed))
        {
            stopped.store(true, std::memory_order_relaxed);
            String error_msg = fmt::format("Schedule timeout, current ts {} expire ts {}", curr, query_expiration_ms);
            postHighPriorityEvent(std::make_shared<AbortEvent>(error_msg, ErrorCodes::TIMEOUT_EXCEEDED));
        }

        std::shared_ptr<ScheduleEvent> event;
        if (getEventToProcess(event))
        {
            bool aborted = processEvent(*event);
            if (aborted)
                return {};
        }
    }

    dag_graph_ptr->joinAsyncRpcAtLast();
    LOG_DEBUG(log, "Scheduling takes {} ms", sw.elapsedMilliseconds());
    return generateExecutionInfo(0, 0);
}

void BSPScheduler::submitTasks(PlanSegment * plan_segment_ptr, const SegmentTask & task)
{
    (void)plan_segment_ptr;
    NodeSelectorResult selector_info;
    {
        std::unique_lock<std::mutex> lock(node_selector_result_mutex);
        selector_info = node_selector_result[task.segment_id];
    }
    LOG_INFO(log, "Submit {} tasks for segment {}", selector_info.worker_nodes.size(), task.segment_id);
    for (size_t i = 0; i < selector_info.worker_nodes.size(); i++)
    {
        std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
        // Is it solid to check empty address via hostname?
        if (selector_info.worker_nodes[i].address.getHostName().empty())
        {
            pending_task_instances.no_prefs.emplace(task.segment_id, i);
        }
        else
        {
            pending_task_instances.for_nodes[selector_info.worker_nodes[i].address].emplace(task.segment_id, i);
        }
    }

    postEvent(std::make_shared<TriggerDispatchEvent>());
}

void BSPScheduler::onSegmentFinished(const size_t & segment_id, bool is_succeed, bool /*is_canceled*/)
{
    if (is_succeed)
    {
        removeDepsAndEnqueueTask(segment_id);
    }
    // on exception
    if (!is_succeed)
    {
        stopped.store(true, std::memory_order_relaxed);
        postEvent(std::make_shared<AbortEvent>(""));
    }
}

void BSPScheduler::onQueryFinished()
{
    std::set<AddressInfo> plan_send_addresses;
    {
        std::unique_lock<bthread::Mutex> lock(dag_graph_ptr->status_mutex);
        plan_send_addresses = dag_graph_ptr->plan_send_addresses;
    }
    for (const auto & address : plan_send_addresses)
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

SegmentInstanceStatusUpdateResult
BSPScheduler::segmentInstanceFinished(size_t segment_id, UInt64 parallel_index, const RuntimeSegmentStatus & status)
{
    postEvent(std::make_shared<SegmentInstanceFinishedEvent>(segment_id, parallel_index, status));
    auto attempt_id = PlanSegmentInstanceAttempt{
        static_cast<UInt32>(segment_id), static_cast<UInt32>(parallel_index), static_cast<UInt32>(status.attempt_id)};
    std::unique_lock<std::mutex> lock(finish_segment_instance_mutex);
    auto now = time_in_milliseconds(std::chrono::system_clock::now());
    std::chrono::milliseconds duration;
    if (query_expiration_ms <= now)
        return SegmentInstanceStatusUpdateResult::UpdateFailed;
    else
        duration = std::chrono::milliseconds(query_expiration_ms - now);
    finish_segment_instance_cv.wait_for(
        lock, duration, [this, &attempt_id] { return segment_status_update_results.contains(attempt_id) || stopped.load(); });
    if (stopped.load())
        return SegmentInstanceStatusUpdateResult::UpdateFailed;
    if (const auto & iter = segment_status_update_results.find(attempt_id); iter != segment_status_update_results.end())
    {
        auto ret = iter->second;
        segment_status_update_results.erase(iter);
        return ret;
    }
    else
        // wait timed out.
        return SegmentInstanceStatusUpdateResult::UpdateFailed;
}

SegmentInstanceStatusUpdateResult
BSPScheduler::onSegmentInstanceFinished(size_t segment_id, UInt64 parallel_index, const RuntimeSegmentStatus & status)
{
    if (isOutdated(status))
        return SegmentInstanceStatusUpdateResult::UpdateFailed;
    auto instance_id = PlanSegmentInstanceId{static_cast<UInt32>(segment_id), static_cast<UInt32>(parallel_index)};
    if (status.is_succeed)
    {
        WorkerNode running_worker;
        {
            std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
            auto running_worker_maybe = segment_parallel_locations[segment_id][parallel_index];
            if (!running_worker_maybe.has_value())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    fmt::format(
                        "Segment {} parallel {} hasn't run yet, this is a erroreous segment status message", segment_id, parallel_index));
            running_worker = running_worker_maybe.value();
            running_segment_to_workers[segment_id].erase(running_worker.address);
            running_instances->erase(running_worker.address, instance_id);
        }
        {
            /// update finished_address before onSegmentFinished(where new task might be added to queue)
            std::unique_lock<std::mutex> lock(dag_graph_ptr->finished_address_mutex);
            dag_graph_ptr->finished_address[segment_id][parallel_index] = running_worker.address;
        }
        {
            std::unique_lock<std::mutex> lock(segment_status_counter_mutex);
            auto segment_status_counter_iterator = segment_status_counter.find(segment_id);
            if (segment_status_counter_iterator == segment_status_counter.end())
            {
                segment_status_counter[segment_id] = {};
            }
            segment_status_counter[segment_id].insert(parallel_index);

            if (segment_status_counter[segment_id].size() == dag_graph_ptr->segment_parallel_size_map[segment_id])
            {
                onSegmentFinished(segment_id, /*is_succeed=*/true, /*is_canceled=*/false);
            }
        }

        std::vector<WorkerNode> workers{running_worker};
        postEvent(std::make_shared<TriggerDispatchEvent>(workers));
    }
    else if (!status.is_succeed && !status.is_cancelled)
    {
        {
            /// if a task has failed in a node, we wont schedule this segment to it anymore
            std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
            auto worker_maybe = segment_parallel_locations[segment_id][parallel_index];
            if (!worker_maybe.has_value())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    fmt::format(
                        "Segment {} parallel {} hasn't run yet, this is a erroreous segment status message", segment_id, parallel_index));
            running_segment_to_workers[segment_id].erase(worker_maybe.value().address);
            running_instances->erase(worker_maybe.value().address, instance_id);
            failed_segment_to_workers[segment_id].insert(worker_maybe.value().address);
        }
        bool retry_success = retryTaskIfPossible(segment_id, parallel_index, status);
        return retry_success ? SegmentInstanceStatusUpdateResult::RetrySuccess : SegmentInstanceStatusUpdateResult::RetryFailed;
    }
    return SegmentInstanceStatusUpdateResult::UpdateSuccess;
}

bool BSPScheduler::retryTaskIfPossible(size_t segment_id, UInt64 parallel_index, const RuntimeSegmentStatus & status)
{
    // ingore the mismatch, leave the retry to worker restart.
    if (status.code == ErrorCodes::EPOCH_MISMATCH)
        return true;
    if (isUnrecoverableStatus(status))
        return false;
    auto instance_id = PlanSegmentInstanceId{static_cast<UInt32>(segment_id), static_cast<UInt32>(parallel_index)};
    size_t attempt_id;
    String rpc_address;
    {
        std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
        attempt_id = segment_instance_attempts[instance_id];
        if (attempt_id > query_context->getSettingsRef().bsp_max_retry_num)
            return false;
        auto running_worker_maybe = segment_parallel_locations[segment_id][parallel_index];
        if (!running_worker_maybe.has_value())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                fmt::format(
                    "Segment {} parallel {} hasn't run yet, this is a erroreous segment status message", segment_id, parallel_index));
        rpc_address = extractExchangeHostPort(running_worker_maybe.value().address);
    }

    /// currently disable retry for TableFinish
    bool is_table_write = false;
    auto * plansegment = dag_graph_ptr->getPlanSegmentPtr(segment_id);
    for (const auto & node : plansegment->getQueryPlan().getNodes())
    {
        if (auto step = std::dynamic_pointer_cast<TableWriteStep>(node.step))
        {
            if (auto cnch_table = std::dynamic_pointer_cast<MergeTreeMetaBase>(step->getTarget()->getStorage()))
            {
                auto txn = query_context->getCurrentTransaction();
                /// Unique table with can't support retry in non-append write mode when dedup in write suffix stage
                if (cnch_table->commitTxnInWriteSuffixStage(txn->getDedupImplVersion(query_context), query_context))
                    return false;
            }
            is_table_write = true;
        }
        else if (node.step->getType() == IQueryPlanStep::Type::TableFinish)
            return false;
    }

    LOG_INFO(
        log,
        "Retrying segment instance(query_id:{} {} rpc_address:{}) {} times",
        plansegment->getQueryId(),
        instance_id.toString(),
        rpc_address,
        attempt_id);
    if (is_table_write && catalog)
    {
        // execute undos and clear part cache before execution
        auto txn = query_context->getCurrentTransaction();
        auto txn_id = txn->getTransactionID();
        auto undo_buffer = catalog->getUndoBuffer(txn_id, rpc_address, instance_id);
        for (const auto & [uuid, resources] : undo_buffer)
        {
            StoragePtr table = catalog->tryGetTableByUUID(*query_context, uuid, TxnTimestamp::maxTS(), true);
            if (table)
            {
                bool clean_fs_lock_by_scan;
                catalog->applyUndos(txn->getTransactionRecord(), table, resources, clean_fs_lock_by_scan);
            }
        }
        catalog->clearUndoBuffer(txn_id, rpc_address, instance_id);
    }
    {
        std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
        if (dag_graph_ptr->table_scan_or_value_segments.contains(segment_id) ||
            // for local no repartion and local may no repartition, schedule to original node
            dag_graph_ptr->getPlanSegmentPtr(segment_id)->hasLocalInput() ||
            // in case all workers except servers are occupied, simply retry at last node
            failed_segment_to_workers[segment_id].size() == cluster_nodes.all_workers.size())
        {
            auto available_worker_maybe = segment_parallel_locations[segment_id][parallel_index];
            if (!available_worker_maybe.has_value())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    fmt::format(
                        "Segment {} parallel {} hasn't run yet, this is a erroreous segment status message", segment_id, parallel_index));
            pending_task_instances.for_nodes[available_worker_maybe.value().address].insert({segment_id, parallel_index});
            lk.unlock();
            std::vector<WorkerNode> workers{available_worker_maybe.value()};
            postEvent(std::make_shared<TriggerDispatchEvent>(workers));
        }
        else
        {
            pending_task_instances.no_prefs.insert({segment_id, parallel_index});
            lk.unlock();
            postEvent(std::make_shared<TriggerDispatchEvent>());
        }
    }
    CurrentThread::getProfileEvents().increment(ProfileEvents::QueryBspRetryCount, 1);
    return true;
}

void BSPScheduler::workerRestarted(const WorkerId & id, const HostWithPorts & host_ports, UInt32 register_time)
{
    LOG_WARNING(log, "Worker {} restarted, retry all tasks running on it.", id.toString());
    postHighPriorityEvent(std::make_shared<ResendResourceEvent>(host_ports));
    postEvent(std::make_shared<WorkerRestartedEvent>(id, register_time));
}

void BSPScheduler::resendResource(const HostWithPorts & host_ports)
{
    query_context->getCnchServerResource()->setSendMutations(true);
    query_context->getCnchServerResource()->resendResource(query_context, host_ports);
}

void BSPScheduler::handleWorkerRestartedEvent(const ScheduleEvent & event)
{
    const auto & worker_restart_event = dynamic_cast<const WorkerRestartedEvent &>(event);
    auto id = worker_restart_event.worker_id;
    auto register_time = worker_restart_event.register_time;
    for (auto & all_worker : cluster_nodes.all_workers)
    {
        if (all_worker.id == id.id)
        {
            std::unordered_set<PlanSegmentInstanceId> instances_to_retry;
            {
                std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
                instances_to_retry = running_instances->getInstances(all_worker.address, register_time);
            }
            // Status should be failed.
            RuntimeSegmentStatus status{.is_succeed = false, .is_cancelled = false};
            for (const auto & instance : instances_to_retry)
            {
                status.segment_id = instance.segment_id;
                status.parallel_index = instance.parallel_index;
                status.code = ErrorCodes::WORKER_RESTARTED;
                {
                    std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
                    status.attempt_id = segment_instance_attempts[instance];
                }
                auto result = onSegmentInstanceFinished(instance.segment_id, instance.parallel_index, status);
                if (result == SegmentInstanceStatusUpdateResult::UpdateFailed)
                    continue;
                if (result == SegmentInstanceStatusUpdateResult::RetryFailed)
                {
                    stopped.store(true, std::memory_order_relaxed);
                    String error_msg = fmt::format(
                        "Worker {} restared, segment instance {} failed", worker_restart_event.worker_id.toString(), instance.toString());
                    postEvent(std::make_shared<AbortEvent>(error_msg));
                }
            }
        }
    }
}

void BSPScheduler::handleScheduleBatchTaskEvent(const ScheduleEvent & event)
{
    const auto & schedule_batch_task = dynamic_cast<const ScheduleBatchTaskEvent &>(event);
    for (auto task : *(schedule_batch_task.batch_task))
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

void BSPScheduler::handleSegmentInstanceFinishedEvent(const ScheduleEvent & event)
{
    const auto & finish_event = dynamic_cast<const SegmentInstanceFinishedEvent &>(event);
    auto result = onSegmentInstanceFinished(finish_event.segment_id, finish_event.parallel_index, finish_event.status);
    std::unique_lock<std::mutex> lock(finish_segment_instance_mutex);
    PlanSegmentInstanceAttempt attempt{
        static_cast<UInt32>(finish_event.segment_id),
        static_cast<UInt32>(finish_event.parallel_index),
        static_cast<UInt32>(finish_event.status.attempt_id)};
    segment_status_update_results.emplace(attempt, result);
    finish_segment_instance_cv.notify_all();
}

void BSPScheduler::handleTriggerDispatchEvent(const ScheduleEvent & event)
{
    const auto & dispatch_trigger = dynamic_cast<const TriggerDispatchEvent &>(event);
    if (dispatch_trigger.all_workers)
    {
        triggerDispatch(cluster_nodes.all_workers);
    }
    else
    {
        triggerDispatch(dispatch_trigger.workers);
    }
}

std::pair<bool, SegmentTaskInstance> BSPScheduler::getInstanceToSchedule(const AddressInfo & worker)
{
    std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
    if (pending_task_instances.for_nodes.contains(worker))
    {
        // Skip task on worker with more taint.
        for (int level_iter = TaintLevel::FailedOrRunning; level_iter != TaintLevel::Last; level_iter++)
        {
            TaintLevel level = static_cast<TaintLevel>(level_iter);
            for (auto instance = pending_task_instances.for_nodes[worker].begin();
                 instance != pending_task_instances.for_nodes[worker].end();)
            {
                // If the target node does not fit specified taint level, skip it.
                if (isTaintNode(instance->segment_id, worker, level))
                {
                    instance++;
                    continue;
                }
                else
                {
                    SegmentTaskInstance ret = *instance;
                    pending_task_instances.for_nodes[worker].erase(instance);
                    return std::make_pair(true, ret);
                }
            }
        }
    }

    if (worker != local_address)
    {
        // Skip task on worker with more taint.
        for (int level_iter = TaintLevel::FailedOrRunning; level_iter != TaintLevel::Last; level_iter++)
        {
            for (auto no_pref = pending_task_instances.no_prefs.begin(); no_pref != pending_task_instances.no_prefs.end();)
            {
                TaintLevel level = static_cast<TaintLevel>(level_iter);
                // If the target node does not fit specified taint level, skip it.
                if (isTaintNode(no_pref->segment_id, worker, level))
                {
                    no_pref++;
                    continue;
                }
                else
                {
                    SegmentTaskInstance ret = *no_pref;
                    pending_task_instances.no_prefs.erase(no_pref);
                    return std::make_pair(true, ret);
                }
            }
        }
    }
    return std::make_pair(false, SegmentTaskInstance(0, 0));
}

void BSPScheduler::triggerDispatch(const std::vector<WorkerNode> & available_workers)
{
    for (const auto & worker : available_workers)
    {
        const auto & address = worker.address;
        const auto & [has_result, task_instance] = getInstanceToSchedule(address);
        if (has_result)
        {
            {
                std::unique_lock<std::mutex> res_lk(node_selector_result_mutex);
                WorkerNode & worker_node = node_selector_result[task_instance.segment_id].worker_nodes[task_instance.parallel_index];
                if (worker_node.address.getHostName().empty())
                    worker_node.address = address;
            }
            WorkerId worker_id;
            // todo (wangtao.vip) move this vars into scheduler.
            if (!cluster_nodes.vw_name.empty() && !cluster_nodes.worker_group_id.empty())
                worker_id = WorkerStatusManager::getWorkerId(cluster_nodes.vw_name, cluster_nodes.worker_group_id, worker.id);
            {
                // TODO(wangtao.vip): this should be handled with dispatch failure.
                std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
                running_segment_to_workers[task_instance.segment_id].insert(address);
                auto instance_id = PlanSegmentInstanceId{
                    static_cast<UInt32>(task_instance.segment_id), static_cast<UInt32>(task_instance.parallel_index)};
                running_instances->insert(address, worker_id, instance_id);
                segment_parallel_locations[task_instance.segment_id][task_instance.parallel_index] = worker;
                failed_segment_to_workers[task_instance.segment_id].insert(
                    local_address); /// init with server addr, as we wont schedule to server
            }

            if (query_context->getSettingsRef().enable_resource_aware_scheduler)
                // todo (wangtao.vip): to be event driven. combine with retrying.
                sendResourceRequest(task_instance, worker_id);
            else
                dispatchOrCollectTask(dag_graph_ptr->getPlanSegmentPtr(task_instance.segment_id), task_instance);
        }
    }
}

void BSPScheduler::sendResourceRequest(const SegmentTaskInstance & instance, const WorkerId & worker_id)
{
        // TODO(lianxuchao): predicate the resource
        ResourceRequest req{
            .segment_id = static_cast<UInt32>(instance.segment_id),
            .parallel_index = static_cast<UInt32>(instance.parallel_index),
            .worker_id = worker_id.toString(),
            .v_cpu = 1,
            .epoch = 0};
        postEvent(std::make_shared<SendResourceRequestEvent>(std::list{req}));
}

void BSPScheduler::handleSendResourceRequestEvent(const ScheduleEvent & event)
{
    const auto & request_event = dynamic_cast<const SendResourceRequestEvent &>(event);
    if (auto rm_client = query_context->getResourceManagerClient(); rm_client)
    {
        for (const auto & request : request_event.resource_request)
        {
            pending_resource_requests.insert(SegmentTaskInstance{request.segment_id, request.parallel_index}, request);
            rm_client->sendResourceRequest(fillResourceRequestToProto(request));
        }
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Resource manager is needed in resource aware scheduler");
    }
}

void BSPScheduler::handleResourceRequestGrantedEvent(const ScheduleEvent & event)
{
    const auto & grant = dynamic_cast<const ResourceRequestGrantedEvent &>(event);
    const auto instance = SegmentTaskInstance{grant.segment_id, grant.parallel_index};
    if (pending_resource_requests.illegal(instance, grant.epoch))
    {
        if (grant.ok)
        {
            if (pending_resource_requests.pending_requests.contains(instance))
            {
                pending_resource_requests.erase(instance);
                dispatchOrCollectTask(dag_graph_ptr->getPlanSegmentPtr(grant.segment_id), instance);
            }
            else
            {
                LOG_WARNING(
                    log, "Instance {}_{} is not in pending set, may be duplicated grant.", instance.segment_id, instance.parallel_index);
            }
        }
        else
        {
            // todo (wangtao.vip) re-request after an interval, say 10 seconds.
        }
    }
}

void BSPScheduler::sendResources(PlanSegment * plan_segment_ptr)
{
    // Send table resources to worker.
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

void BSPScheduler::prepareTask(PlanSegment * plan_segment_ptr, NodeSelectorResult & selector_info, const SegmentTask & task)
{
    // Register exchange for all outputs.
    for (const auto & output : plan_segment_ptr->getPlanSegmentOutputs())
    {
        query_context->getExchangeDataTracker()->registerExchange(
            query_context->getCurrentQueryId(), output->getExchangeId(), selector_info.worker_nodes.size());
    }
    if (task.has_table_scan_or_value)
    {
        if (!selector_info.buckets_on_workers.empty())
        {
            std::unordered_map<AddressInfo, size_t, AddressInfo::Hash> source_task_index_on_workers;
            for (size_t i = 0; i < selector_info.worker_nodes.size(); i++)
            {
                const auto & addr = selector_info.worker_nodes[i].address;
                size_t idx = source_task_index_on_workers[addr];
                source_task_buckets.emplace(SegmentTaskInstance{task.segment_id, i}, selector_info.buckets_on_workers[addr][idx]);
                source_task_index_on_workers[addr]++;
            }
        }
        else if (!selector_info.source_task_count_on_workers.empty())
        {
            std::unordered_map<AddressInfo, size_t, AddressInfo::Hash> source_task_index_on_workers;
            for (size_t i = 0; i < selector_info.worker_nodes.size(); i++)
            {
                const auto & addr = selector_info.worker_nodes[i].address;
                source_task_idx.emplace(
                    SegmentTaskInstance{task.segment_id, i},
                    std::make_pair(source_task_index_on_workers[addr], selector_info.source_task_count_on_workers[addr]));
                source_task_index_on_workers[addr]++;
            }
        }
    }
}

PlanSegmentExecutionInfo BSPScheduler::generateExecutionInfo(size_t task_id, size_t index)
{
    PlanSegmentExecutionInfo execution_info;
    execution_info.parallel_id = index;
    SegmentTaskInstance instance{task_id, index};
    if (source_task_idx.contains(instance))
    {
        execution_info.source_task_filter.index = source_task_idx[instance].first;
        execution_info.source_task_filter.count = source_task_idx[instance].second;
    }
    else if (source_task_buckets.contains(instance))
    {
        execution_info.source_task_filter.buckets = source_task_buckets[instance];
    }

    PlanSegmentInstanceId instance_id = PlanSegmentInstanceId{static_cast<UInt32>(task_id), static_cast<UInt32>(index)};
    {
        std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
        execution_info.attempt_id = (segment_instance_attempts[instance_id]);
        execution_info.worker_epoch = running_instances->getEpoch(instance_id);
    }
    {
        std::unique_lock<std::mutex> lock(node_selector_result_mutex);
        for (const auto & source : node_selector_result[task_id].sources[instance_id])
        {
            execution_info.sources[source.exchange_id].emplace_back(source);
        }
    }
    return execution_info;
}

bool BSPScheduler::addBatchTask(BatchTaskPtr batch_task)
{
    std::shared_ptr<ScheduleEvent> event = std::make_shared<ScheduleBatchTaskEvent>(batch_task);
    return postEvent(event);
}

void BSPScheduler::prepareFinalTaskImpl(PlanSegment * final_plan_segment, const AddressInfo & addr)
{
    auto read_partitions = node_selector.splitReadPartitions(final_plan_segment);
    NodeSelectorResult result{.worker_nodes = {WorkerNode(addr, NodeType::Local)}};
    node_selector.setSources(final_plan_segment, &result, read_partitions);
    LOG_DEBUG(log, "Set address {} for final segment, node_selector_result is {}", addr.toShortString(), result.toString());
    std::unique_lock<std::mutex> lock(node_selector_result_mutex);
    node_selector_result[0] = std::move(result);
}

bool BSPScheduler::isUnrecoverableStatus(const RuntimeSegmentStatus & status)
{
    if (unrecoverable_reasons.contains(status.code))
    {
        LOG_WARNING(
            log,
            "Segment {} parallel index {} in query {} failed with code {} and reason {}, will not retry",
            status.segment_id,
            status.parallel_index,
            status.query_id,
            ErrorCodes::getName(status.code),
            status.message);
        return true;
    }
    return false;
}

bool BSPScheduler::isOutdated(const RuntimeSegmentStatus & status)
{
    int32_t current_attempt_id = 0;
    auto instance_id = PlanSegmentInstanceId{static_cast<UInt32>(status.segment_id), static_cast<UInt32>(status.parallel_index)};
    {
        std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
        current_attempt_id = segment_instance_attempts[instance_id];
        segment_instance_attempts[instance_id]++;
    }
    if (current_attempt_id != status.attempt_id)
    {
        LOG_WARNING(
            log,
            "Ignore status of segment {} parallel index {} because it attempt id {} does not equal to current attempt id {}",
            status.segment_id,
            status.parallel_index,
            status.attempt_id,
            current_attempt_id);
        return true;
    }
    return false;
}

bool BSPScheduler::isTaintNode(size_t task_id, const AddressInfo & worker, TaintLevel & taint_level)
{
    const auto & running_node_iter = running_segment_to_workers.find(task_id);
    switch (taint_level)
    {
        case TaintLevel::Running:
            return running_node_iter != running_segment_to_workers.end() && running_node_iter->second.contains(worker);
        case TaintLevel::FailedOrRunning: {
            const auto & failed_node_iter = failed_segment_to_workers.find(task_id);
            return (running_node_iter != running_segment_to_workers.end() && running_node_iter->second.contains(worker))
                || (failed_node_iter != failed_segment_to_workers.end() && failed_node_iter->second.contains(worker));
        }
        case TaintLevel::Last:
            throw Exception("Unexpected taint level", ErrorCodes::LOGICAL_ERROR);
    }
}

Protos::SendResourceRequestReq BSPScheduler::fillResourceRequestToProto(const ResourceRequest & req)
{
    Protos::SendResourceRequestReq pb;
    local_address.toProto(*pb.mutable_server_addr());
    pb.set_req_type(::DB::Protos::ResourceRequestType::RESOURCE_REQUEST);
    pb.set_query_id(query_id);
    pb.set_query_start_ts(query_context->getClientInfo().initial_query_start_time_microseconds / 1000);
    pb.set_segment_id(req.segment_id);
    pb.set_parallel_index(req.parallel_index);
    pb.set_worker_id(req.worker_id);
    pb.set_request_vcpu(req.v_cpu);
    pb.set_request_mem(req.mem);
    pb.set_epoch(req.epoch);

    return pb;
}

void BSPScheduler::resourceRequestGranted(const UInt32 segment_id, const UInt32 parallel_index, const UInt32 epoch, bool ok)
{
    postEvent(std::make_shared<ResourceRequestGrantedEvent>(segment_id, parallel_index, epoch, ok));
}
}
