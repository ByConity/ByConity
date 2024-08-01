#include <atomic>
#include <memory>
#include <mutex>
#include <CloudServices/CnchServerResource.h>
#include <bthread/mutex.h>
#include <Poco/Logger.h>
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
#include <utility>
#include <CloudServices/CnchServerResource.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BRPC_PROTOCOL_VERSION_UNSUPPORT;
}

/// BSP scheduler logic
void BSPScheduler::submitTasks(PlanSegment * plan_segment_ptr, const SegmentTask & task)
{
    (void)plan_segment_ptr;
    NodeSelectorResult selector_info;
    {
        std::unique_lock<std::mutex> lock(node_selector_result_mutex);
        selector_info = node_selector_result[task.task_id];
    }
    LOG_INFO(log, "Submit {} tasks for segment {}", selector_info.worker_nodes.size(), task.task_id);
    std::unordered_map<AddressInfo, size_t, AddressInfo::Hash> source_task_count_on_workers;
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
            if (task.has_table_scan)
            {
                source_task_count_on_workers[selector_info.worker_nodes[i].address] += 1;
            }
        }
    }
    if (task.has_table_scan)
    {
        std::unordered_map<AddressInfo, size_t, AddressInfo::Hash> source_task_index_on_workers;
        for (size_t i = 0; i < selector_info.worker_nodes.size(); i++)
        {
            const auto & addr = selector_info.worker_nodes[i].address;
            source_task_idx.emplace(
                SegmentTaskInstance{task.task_id, i},
                std::make_pair(source_task_index_on_workers[addr], source_task_count_on_workers[addr]));
            source_task_index_on_workers[addr]++;
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

void BSPScheduler::updateSegmentStatusCounter(size_t segment_id, UInt64 parallel_index, const RuntimeSegmentsStatus & status)
{
    if (status.is_succeed)
    {
        AddressInfo available_worker;
        {
            std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
            available_worker = segment_parallel_locations[segment_id][parallel_index];
            occupied_workers[segment_id].erase(available_worker);
        }
        {
            /// update finished_address before onSegmentFinished(where new task might be added to queue)
            std::unique_lock<std::mutex> lock(dag_graph_ptr->finished_address_mutex);
            dag_graph_ptr->finished_address[segment_id][parallel_index] = available_worker;
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

        triggerDispatch({WorkerNode{available_worker}});
    }
    else if (!status.is_succeed && !status.is_cancelled)
    {
        /// if a task has failed in a node, we wont schedule this segment to it anymore
        std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
        auto failed_worker = segment_parallel_locations[segment_id][parallel_index];
        failed_workers[segment_id].insert(failed_worker);
    }
}

const size_t MIN_MAJOR_VERSION_ENABLE_RETRY_NORMAL_TABLE_WRITE = 2;
const size_t MIN_MINOR_VERSION_ENABLE_RETRY_NORMAL_TABLE_WRITE = 3;

bool BSPScheduler::retryTaskIfPossible(size_t segment_id, UInt64 parallel_index)
{
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
            if (auto cnch_table = step->getTarget()->getStorage())
            {
                // unique table can't support retry
                if (cnch_table->getInMemoryMetadataPtr()->hasUniqueKey())
                    return false;
                is_table_write = true;
            }
        }
        else if (node.step->getType() == IQueryPlanStep::Type::TableFinish)
            return false;
    }
    auto major = query_context->getClientInfo().brpc_protocol_major_version;
    auto effective_minor = std::min(
        query_context->getSettingsRef().min_compatible_brpc_minor_version.value, static_cast<uint64_t>(DBMS_BRPC_PROTOCOL_MINOR_VERSION));
    if (major == MIN_MAJOR_VERSION_ENABLE_RETRY_NORMAL_TABLE_WRITE && effective_minor < MIN_MINOR_VERSION_ENABLE_RETRY_NORMAL_TABLE_WRITE)
    {
        if (is_table_write)
        {
            LOG_ERROR(
                log,
                "Current brpc protocol version is {}.{}(expected at least {}.{}), does not support retry for table write segment",
                major,
                effective_minor,
                major,
                MIN_MINOR_VERSION_ENABLE_RETRY_NORMAL_TABLE_WRITE);
            return false;
        }
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
            failed_workers[segment_id].size() == cluster_nodes.all_workers.size())
        {
            auto available_worker = segment_parallel_locations[segment_id][parallel_index];
            occupied_workers[segment_id].erase(available_worker);
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

const AddressInfo & BSPScheduler::getSegmentParallelLocation(PlanSegmentInstanceId instance_id)
{
    std::unique_lock<std::mutex> lock(nodes_alloc_mutex);
    return segment_parallel_locations[instance_id.segment_id][instance_id.parallel_id];
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
                SegmentTaskInstance ret = *instance;
                pending_task_instances.for_nodes[worker].erase(instance);
                return std::make_pair(true, ret);
            }
        }
    }

    if (worker != local_address)
    {
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
                SegmentTaskInstance ret = *no_pref;
                pending_task_instances.no_prefs.erase(no_pref);
                return std::make_pair(true, ret);
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

        WorkerNode worker_node;
        {
            std::unique_lock<std::mutex> res_lk(node_selector_result_mutex);
            node_selector_result[task_instance->task_id].worker_nodes[task_instance->parallel_index] = worker;
            worker_node = node_selector_result[task_instance->task_id].worker_nodes[task_instance->parallel_index];
            if (worker_node.address.getHostName().empty())
                worker_node.address = address;
        }

        {
            std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
            occupied_workers[task_instance->task_id].insert(address);
            segment_parallel_locations[task_instance->task_id][task_instance->parallel_index] = address;
            failed_workers[task_instance->task_id].insert(local_address); /// init with server addr, as we wont schedule to server
        }

        dispatchTask(
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

void BSPScheduler::prepareTask(PlanSegment * plan_segment_ptr, size_t parallel_size)
{
    // Register exchange for all outputs.
    for (const auto & output : plan_segment_ptr->getPlanSegmentOutputs())
    {
        query_context->getExchangeDataTracker()->registerExchange(
            query_context->getCurrentQueryId(), output->getExchangeId(), parallel_size);
    }
}

PlanSegmentExecutionInfo BSPScheduler::generateExecutionInfo(size_t task_id, size_t index)
{
    PlanSegmentExecutionInfo execution_info;
    execution_info.parallel_id = index;
    SegmentTaskInstance instance{task_id, index};
    if (source_task_idx.contains(instance))
    {
        execution_info.source_task_index = source_task_idx[instance].first;
        execution_info.source_task_count = source_task_idx[instance].second;
    }
    {
        std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
        execution_info.retry_id = (segment_instance_retry_cnt[{static_cast<UInt32>(task_id), static_cast<UInt32>(index)}]);
    }
    return execution_info;
}
}
