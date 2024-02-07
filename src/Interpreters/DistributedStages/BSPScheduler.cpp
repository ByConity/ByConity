#include <CloudServices/CnchServerResource.h>

#include "BSPScheduler.h"

namespace DB
{

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
        stopped.store(true, std::memory_order_relaxed);
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
                SegmentTaskInstance ret = *instance;
                pending_task_instances.for_nodes[worker].erase(instance);
                return std::make_pair(true, ret);
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
            SegmentTaskInstance ret = *no_pref;
            pending_task_instances.no_prefs.erase(no_pref);
            return std::make_pair(true, ret);
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

void BSPScheduler::prepareTask(PlanSegment * plan_segment_ptr, size_t parallel_size)
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

    // Register exchange for all outputs.
    for (const auto & output : plan_segment_ptr->getPlanSegmentOutputs())
    {
        query_context->getExchangeDataTracker()->registerExchange(
            query_context->getCurrentQueryId(), output->getExchangeId(), parallel_size);
    }
}
}
