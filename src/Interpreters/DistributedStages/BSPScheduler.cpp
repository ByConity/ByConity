#include <atomic>
#include <mutex>
#include <CloudServices/CnchServerResource.h>
#include <bthread/mutex.h>
#include <Poco/Logger.h>

#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/PlanSegmentInstance.h>
#include <Interpreters/DistributedStages/RuntimeSegmentsStatus.h>
#include <Interpreters/DistributedStages/Scheduler.h>
#include <Interpreters/NodeSelector.h>
#include <QueryPlan/QueryPlan.h>
#include "BSPScheduler.h"

#include <cstddef>
#include <string>
#include <unordered_map>
#include <utility>
#include <CloudServices/CnchServerResource.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/Scheduler.h>

namespace DB
{

/// BSP scheduler logic
void BSPScheduler::submitTasks(PlanSegment * plan_segment_ptr, const SegmentTask & task)
{
    (void)plan_segment_ptr;
    NodeSelectorResult selector_info;
    {
        std::unique_lock<std::mutex> lock(node_selector_result_mutex);
        selector_info = node_selector_result[task.task_id];
    }
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
            if (task.is_source)
            {
                source_task_count_on_workers[selector_info.worker_nodes[i].address] += 1;
            }
        }
    }
    if (task.is_source)
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
        auto iter = pending_task_instances.for_nodes[failed_worker].begin();
        while (iter != pending_task_instances.for_nodes[failed_worker].end())
        {
            if (iter->task_id == segment_id)
            {
                pending_task_instances.no_prefs.insert({iter->task_id, iter->parallel_index});
                iter = pending_task_instances.for_nodes[failed_worker].erase(iter);
            }
            else
            {
                iter++;
            }
        }
    }
}

bool BSPScheduler::retryTaskIfPossible(size_t segment_id, UInt64 parallel_index)
{
    if (retry_count.load(std::memory_order_acquire) >= query_context->getSettings().bsp_max_retry_num)
        return false;
    /// currently disable retry for TableWite && TableFinish
    auto * plansegment = dag_graph_ptr->getPlanSegmentPtr(segment_id);
    for (const auto & node : plansegment->getQueryPlan().getNodes())
    {
        if (node.step->getType() == IQueryPlanStep::Type::TableWrite || node.step->getType() == IQueryPlanStep::Type::TableFinish)
            return false;
    }
    auto cnt = retry_count.fetch_add(1, std::memory_order_acq_rel);
    if (cnt < query_context->getSettingsRef().bsp_max_retry_num)
    {
        {
            std::unique_lock<std::mutex> lk(nodes_alloc_mutex);
            // table scan plansegment will retry at the same worker
            if (dag_graph_ptr->any_tables.contains(segment_id) ||
                // in case all workers are occupied, simply retry at last node
                failed_workers[segment_id].size() == cluster_nodes.rank_workers.size() ||
                // for local no repartion and local may no repartition, schedule to original node
                NodeSelector::tryGetLocalInput(dag_graph_ptr->getPlanSegmentPtr(segment_id)))
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
                triggerDispatch(cluster_nodes.rank_workers);
            }
        }
        return true;
    }
    else
    {
        return false;
    }
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
                LOG_INFO(&Poco::Logger::get("debug"), "query_id:{} addr:{} no instance found", query_id, address.toString());
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
    return execution_info;
}
}
