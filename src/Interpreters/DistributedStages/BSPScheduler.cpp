#include <atomic>
#include <memory>
#include <mutex>
#include <CloudServices/CnchServerResource.h>
#include <bthread/mutex.h>
#include <Poco/Logger.h>
#include <Common/ErrorCodes.h>
#include <common/logger_useful.h>

#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/PlanSegmentInstance.h>
#include <Interpreters/DistributedStages/RuntimeSegmentsStatus.h>
#include <Interpreters/DistributedStages/Scheduler.h>
#include <Interpreters/NodeSelector.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/QueryPlan.h>
#include <QueryPlan/TableWriteStep.h>
#include "BSPScheduler.h"

#include <cstddef>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <CloudServices/CnchServerResource.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BRPC_PROTOCOL_VERSION_UNSUPPORT;
    extern const int WORKER_RESTARTED;
}

/// BSP scheduler logic
void BSPScheduler::genTasks()
{
    genLeafTasks();
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

        batch_task->emplace_back(leaf_id, true);
        plansegment_topology.erase(leaf_id);
        LOG_TRACE(log, "Task for leaf segment {} generated", leaf_id);
    }
    addBatchTask(std::move(batch_task));
}

void BSPScheduler::submitTasks(PlanSegment * plan_segment_ptr, const SegmentTask & task)
{
    (void)plan_segment_ptr;
    NodeSelectorResult selector_info;
    {
        std::unique_lock<std::mutex> lock(node_selector_result_mutex);
        selector_info = node_selector_result[task.task_id];
    }
    LOG_INFO(log, "Submit {} tasks for segment {}", selector_info.worker_nodes.size(), task.task_id);
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

    triggerDispatch(cluster_nodes.all_workers);
}

void BSPScheduler::onSegmentFinished(const size_t & segment_id, bool is_succeed, bool /*is_canceled*/)
{
    if (is_succeed)
    {
        removeDepsAndEnqueueTask(SegmentTask{segment_id});
    }
    // on exception
    if (!is_succeed)
    {
        stopped.store(true, std::memory_order_relaxed);
        // emplace a fake task.
        addBatchTask(nullptr);
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

void BSPScheduler::updateSegmentStatusCounter(size_t segment_id, UInt64 parallel_index, const RuntimeSegmentStatus & status)
{
    if (isOutdated(status))
        return;
    auto instance_id = PlanSegmentInstanceId{static_cast<UInt32>(segment_id), static_cast<UInt32>(parallel_index)};
    if (status.is_succeed)
    {
        AddressInfo running_worker;
        {
            std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
            running_worker = segment_parallel_locations[segment_id][parallel_index];
            running_segment_to_workers[segment_id].erase(running_worker);
            worker_to_running_instances[running_worker].erase(instance_id);
        }
        {
            /// update finished_address before onSegmentFinished(where new task might be added to queue)
            std::unique_lock<std::mutex> lock(dag_graph_ptr->finished_address_mutex);
            dag_graph_ptr->finished_address[segment_id][parallel_index] = running_worker;
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

        triggerDispatch({WorkerNode{running_worker}});
    }
    else if (!status.is_succeed && !status.is_cancelled)
    {
        /// if a task has failed in a node, we wont schedule this segment to it anymore
        std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
        auto worker = segment_parallel_locations[segment_id][parallel_index];
        running_segment_to_workers[segment_id].erase(worker);
        worker_to_running_instances[worker].erase(instance_id);
        failed_segment_to_workers[segment_id].insert(worker);
    }
}

bool BSPScheduler::retryTaskIfPossible(size_t segment_id, UInt64 parallel_index, const RuntimeSegmentStatus & status)
{
    // If status is outdated, we skip the retry but treat it like success.
    if (isOutdated(status))
        return true;
    if (isUnrecoverableStatus(status))
        return false;
    auto instance_id = PlanSegmentInstanceId{static_cast<UInt32>(segment_id), static_cast<UInt32>(parallel_index)};
    size_t retry_id;
    String rpc_address;
    {
        std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
        auto & cnt = segment_instance_retry_cnt[instance_id];
        if (cnt >= query_context->getSettingsRef().bsp_max_retry_num)
            return false;
        cnt++;
        retry_id = cnt;
        rpc_address = extractExchangeHostPort(segment_parallel_locations[instance_id.segment_id][instance_id.parallel_id]);
    }

    /// currently disable retry for TableFinish
    bool is_table_write = false;
    auto * plansegment = dag_graph_ptr->getPlanSegmentPtr(segment_id);
    for (const auto & node : plansegment->getQueryPlan().getNodes())
    {
        if (auto step = std::dynamic_pointer_cast<TableWriteStep>(node.step))
        {
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
        retry_id);
    if (is_table_write)
    {
        // execute undos and clear part cache before execution
        auto catalog = query_context->getCnchCatalog();
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
        if (dag_graph_ptr->segments_has_table_scan.contains(segment_id) ||
            // for local no repartion and local may no repartition, schedule to original node
            NodeSelector::tryGetLocalInput(dag_graph_ptr->getPlanSegmentPtr(segment_id)) ||
            // in case all workers except servers are occupied, simply retry at last node
            failed_segment_to_workers[segment_id].size() == cluster_nodes.all_workers.size())
        {
            auto available_worker = segment_parallel_locations[segment_id][parallel_index];
            pending_task_instances.for_nodes[available_worker].insert({segment_id, parallel_index});
            lk.unlock();
            triggerDispatch({WorkerNode{available_worker}});
        }
        else
        {
            pending_task_instances.no_prefs.insert({segment_id, parallel_index});
            lk.unlock();
            triggerDispatch(cluster_nodes.all_workers);
        }
    }
    return true;
}

void BSPScheduler::onWorkerRestarted(const WorkerId & id, const HostWithPorts & host_ports)
{
    LOG_WARNING(log, "Worker {} restarted, retry all tasks running on it.", id.ToString());
    query_context->getCnchServerResource()->setSendMutations(true);
    query_context->getCnchServerResource()->resendResource(query_context, host_ports);
    for (auto worker_iter = cluster_nodes.all_workers.begin(); worker_iter != cluster_nodes.all_workers.end(); worker_iter++)
    {
        if (worker_iter->id == id.id)
        {
            std::unordered_set<PlanSegmentInstanceId> instances_to_retry;
            {
                std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
                instances_to_retry = worker_to_running_instances[worker_iter->address];
            }
            // Status should be failed.
            RuntimeSegmentStatus status{.is_succeed = false, .is_cancelled = false};
            for (const auto & instance : instances_to_retry)
            {
                status.segment_id = instance.segment_id;
                status.parallel_index = instance.parallel_id;
                status.code = ErrorCodes::WORKER_RESTARTED;
                {
                    std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
                    status.retry_id = segment_instance_retry_cnt[instance];
                }
                updateSegmentStatusCounter(instance.segment_id, instance.parallel_id, status);
                if (!retryTaskIfPossible(instance.segment_id, instance.parallel_id, status))
                {
                    stopped.store(true, std::memory_order_relaxed);
                    error_msg = fmt::format("Worker {} restared, segment instance {} failed", id.ToString(), instance.toString());
                    // emplace a fake task.
                    addBatchTask(nullptr);
                }
            }
        }
    }
}

const AddressInfo & BSPScheduler::getSegmentParallelLocation(PlanSegmentInstanceId instance_id)
{
    std::unique_lock<std::mutex> lock(nodes_alloc_mutex);
    return segment_parallel_locations[instance_id.segment_id][instance_id.parallel_id];
}

std::pair<bool, SegmentTaskInstance> BSPScheduler::getInstanceToSchedule(const AddressInfo & worker)
{
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
                if (isTaintNode(instance->task_id, worker, level))
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
                if (isTaintNode(no_pref->task_id, worker, level))
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
        std::optional<SegmentTaskInstance> task_instance;
        {
            std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
            const auto & [has_result, result] = getInstanceToSchedule(address);
            if (!has_result)
            {
                continue;
            }
            task_instance.emplace(result);
        }

        {
            std::unique_lock<std::mutex> res_lk(node_selector_result_mutex);
            WorkerNode & worker_node = node_selector_result[task_instance->task_id].worker_nodes[task_instance->parallel_index];
            if (worker_node.address.getHostName().empty())
                worker_node.address = address;
        }

        {
            // TODO(wangtao.vip): this should be handled with dispatch failure.
            std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
            running_segment_to_workers[task_instance->task_id].insert(address);
            worker_to_running_instances[address].insert(
                PlanSegmentInstanceId{static_cast<UInt32>(task_instance->task_id), static_cast<UInt32>(task_instance->parallel_index)});
            segment_parallel_locations[task_instance->task_id][task_instance->parallel_index] = address;
            failed_segment_to_workers[task_instance->task_id].insert(
                local_address); /// init with server addr, as we wont schedule to server
        }

        dispatchOrSaveTask(
            dag_graph_ptr->getPlanSegmentPtr(task_instance->task_id), SegmentTask{task_instance->task_id}, task_instance->parallel_index);
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
    if (task.has_table_scan)
    {
        if (!selector_info.buckets_on_workers.empty())
        {
            std::unordered_map<AddressInfo, size_t, AddressInfo::Hash> source_task_index_on_workers;
            for (size_t i = 0; i < selector_info.worker_nodes.size(); i++)
            {
                const auto & addr = selector_info.worker_nodes[i].address;
                size_t idx = source_task_index_on_workers[addr];
                source_task_buckets.emplace(SegmentTaskInstance{task.task_id, i}, selector_info.buckets_on_workers[addr][idx]);
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
                    SegmentTaskInstance{task.task_id, i},
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
    {
        std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
        execution_info.retry_id = (segment_instance_retry_cnt[{static_cast<UInt32>(task_id), static_cast<UInt32>(index)}]);
    }
    return execution_info;
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
    int32_t current_retry_id = 0;
    {
        std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
        current_retry_id
            = (segment_instance_retry_cnt[{static_cast<UInt32>(status.segment_id), static_cast<UInt32>(status.parallel_index)}]);
    }
    if (current_retry_id != status.retry_id)
    {
        LOG_WARNING(
            log,
            "Ignore status of segment {} parallel index {} because it retry id {} does not equal to current retry id {}",
            status.segment_id,
            status.parallel_index,
            status.retry_id,
            current_retry_id);
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
}
