#include <mutex>
#include <unordered_map>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/ExchangeMode.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/NodeSelector.h>
#include <bthread/mutex.h>
#include <Common/Exception.h>
#include <common/defines.h>
#include <Common/HostWithPorts.h>
#include <common/logger_useful.h>
#include <common/types.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_QUERY_PARAMETER;
}

bool isLocal(PlanSegment * plan_segment_ptr)
{
    return plan_segment_ptr->getParallelSize() == 0 || plan_segment_ptr->getClusterName().empty();
}

void NodeSelector::setParallelIndexAndSourceAddrs(PlanSegment * plan_segment_ptr, NodeSelectorResult * result)
{
    LOG_TRACE(log, "Set parallel index for segment id {}", plan_segment_ptr->getPlanSegmentId());
    //set parallel index
    for (size_t parallel_index_id_index = 0; parallel_index_id_index < result->worker_nodes.size(); parallel_index_id_index++)
        result->indexes.emplace_back(parallel_index_id_index);

    //set input source addresses
    bool first_local_input = true;
    for (auto & plan_segment_input : plan_segment_ptr->getPlanSegmentInputs())
    {
        auto plan_segment_input_id = plan_segment_input->getPlanSegmentId();
        if (plan_segment_input->getPlanSegmentType() != PlanSegmentType::EXCHANGE)
        {
            continue;
        }

        auto source_iter = result->source_addresses.end();

        // if input mode is local, set parallel index
        if (auto it = dag_graph_ptr->id_to_segment.find(plan_segment_input_id); it != dag_graph_ptr->id_to_segment.end())
        {
            for (auto & plan_segment_output : it->second->getPlanSegmentOutputs())
            {
                if (plan_segment_output->getExchangeId() != plan_segment_input->getExchangeId())
                    continue;
                // if data is write to local, so no need to shuffle data
                if ((plan_segment_output->getExchangeMode() == ExchangeMode::LOCAL_NO_NEED_REPARTITION
                     || plan_segment_output->getExchangeMode() == ExchangeMode::LOCAL_MAY_NEED_REPARTITION)
                    && first_local_input)
                {
                    LOG_TRACE(log, "Local plan segment plan_segment_input_id:{}", plan_segment_input_id);
                    source_iter
                        = result->source_addresses.emplace(plan_segment_input_id, AddressInfos{AddressInfo("localhost", 0, "", "")}).first;
                    source_iter->second.parallel_index = 0;
                    if (query_context->getSettingsRef().bsp_mode)
                        first_local_input = false;
                }
                else
                {
                    LOG_TRACE(log, "Non-local plan segment plan_segment_input_id:{}", plan_segment_input_id);
                    source_iter
                        = result->source_addresses.emplace(plan_segment_input_id, dag_graph_ptr->getAddressInfos(plan_segment_input_id))
                              .first;
                }
            }
            // collect status, useful for debug
#if defined(TASK_ASSIGN_DEBUG)
            size_t parallel_index_id = 0;
            for (const auto & worker_node : result->worker_nodes)
            {
                if (dag_graph_ptr->exchange_data_assign_node_mappings.find(plan_segment_input_id)
                    == dag_graph_ptr->exchange_data_assign_node_mappings.end())
                {
                    dag_graph_ptr->exchange_data_assign_node_mappings.emplace(
                        std::make_pair(plan_segment_input_id, std::vector<std::pair<size_t, AddressInfo>>{}));
                }
                auto current_parallel_index_id
                    = source_iter->second.parallel_index ? *source_iter->second.parallel_index : parallel_index_id;
                dag_graph_ptr->exchange_data_assign_node_mappings.find(plan_segment_input_id)
                    ->second.emplace_back(std::make_pair(current_parallel_index_id, worker_node.address));

                parallel_index_id++;
            }
#endif
        }
    }
    // set prallel size to 1 for those local output
    for (auto & plan_segment_output : plan_segment_ptr->getPlanSegmentOutputs())
    {
        auto plan_segment_output_id = plan_segment_output->getPlanSegmentId();
        if (auto it = dag_graph_ptr->id_to_segment.find(plan_segment_output_id);
            it != dag_graph_ptr->id_to_segment.end() && isLocal(it->second))
        {
            LOG_TRACE(
                log,
                "Set parallel size of output segment {} to 1 for segment id {}",
                plan_segment_output_id,
                plan_segment_ptr->getPlanSegmentId());
            plan_segment_output->setParallelSize(1);
        }
    }
}

NodeSelectorResult LocalNodeSelector::select(PlanSegment *, ContextPtr query_context)
{
    NodeSelectorResult result;
    auto local_address = getLocalAddress(*query_context);
    result.worker_nodes.emplace_back(local_address, NodeType::Local);
    result.indexes.emplace_back(0);
    return result;
}

NodeSelectorResult SourceNodeSelector::select(PlanSegment * plan_segment_ptr, ContextPtr query_context, DAGGraph * dag_graph_ptr)
{
    checkClusterInfo(plan_segment_ptr);
    bool need_stable_schedule = needStableSchedule(plan_segment_ptr);
    NodeSelectorResult result;
    // The one worker excluded is server itself.
    const auto worker_number = cluster_nodes.all_workers.size() - 1;
    if (plan_segment_ptr->getParallelSize() > worker_number && (!query_context->getSettingsRef().bsp_mode || need_stable_schedule))
    {
        throw Exception(
            ErrorCodes::BAD_QUERY_PARAMETER,
            "Logical error: distributed_max_parallel_size({}) of table scan can not be greater than worker number({})",
            plan_segment_ptr->getParallelSize(),
            worker_number);
    }

    // If parallelism is greater than the worker number, we split the parts according to the input size.
    if (plan_segment_ptr->getParallelSize() > worker_number)
    {
        std::unordered_map<HostWithPorts, size_t> size_per_worker;
        for (auto & plan_segment_input : plan_segment_ptr->getPlanSegmentInputs())
        {
            auto storage_id = plan_segment_input->getStorageID();
            if (storage_id && storage_id->hasUUID())
            {
                const auto & size_map = query_context->getCnchServerResource()->getResourceSizeMap(storage_id->uuid);
                for (const auto & [h, s] : size_map)
                {
                    auto & size_on_this_worker = size_per_worker[h];
                    if (size_on_this_worker)
                        size_on_this_worker += s;
                    else
                        size_on_this_worker = s;
                }
            }
        }
        /// TODO(WangTao): Use more fine grained assigned algorithm.
        size_t sum = 0;
        for (const auto & [h, current_size] : size_per_worker)
        {
            sum += current_size;
        }
        size_t avg = sum / plan_segment_ptr->getParallelSize() + 1;
        if (sum < plan_segment_ptr->getParallelSize())
            sum = 0;
        if (sum > 0)
        {
            // Assign parallelism accroding to regular average size.
            for (auto & [h, s] : size_per_worker)
            {
                size_t p = s / avg;
                if (p > 0)
                {
                    s = s % avg;
                }
                if (p == 0 && s > 0)
                {
                    p = 1;
                    s = 0;
                }
                for (size_t i = 0; i < p; i++)
                {
                    auto worker_address = getRemoteAddress(h, query_context);
                    result.worker_nodes.emplace_back(worker_address, NodeType::Remote, h.id);
                }
            }
        }
        // Assign parallelism according to major part(>0.5) of average size, if needed.
        if (result.worker_nodes.size() < plan_segment_ptr->getParallelSize())
        {
            for (auto & [h, s] : size_per_worker)
            {
                if (s * 2 > avg)
                {
                    auto worker_address = getRemoteAddress(h, query_context);
                    result.worker_nodes.emplace_back(worker_address, NodeType::Remote, h.id);
                    s = 0;
                }
                if (result.worker_nodes.size() == plan_segment_ptr->getParallelSize())
                    break;
            }
        }
        // Assign parallelism to each worker until no one left.
        while (result.worker_nodes.size() < plan_segment_ptr->getParallelSize())
        {
            for (const auto & [h, _] : size_per_worker)
            {
                auto worker_address = getRemoteAddress(h, query_context);
                result.worker_nodes.emplace_back(worker_address, NodeType::Remote, h.id);
                if (result.worker_nodes.size() == plan_segment_ptr->getParallelSize())
                    break;
            }
        }
    }
    else
    {
        auto local_address = getLocalAddress(*query_context);
        if (dag_graph_ptr->source_pruner
            && dag_graph_ptr->source_pruner->plan_segment_workers_map.contains(plan_segment_ptr->getPlanSegmentId()))
        {
            selectPrunedWorkers(dag_graph_ptr, plan_segment_ptr, result, local_address);
        }
        else
        {
            if (need_stable_schedule)
            {
                LOG_TRACE(log, "use stable schedule for segment:{} with {} nodes", plan_segment_ptr->getPlanSegmentId(), worker_number);
                if (plan_segment_ptr->getParallelSize() != worker_number)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        " Source plan segment parallel size {} is not equal to worker number {}.",
                        plan_segment_ptr->getParallelSize(),
                        worker_number);
                for (size_t parallel_index = 0; parallel_index < worker_number; parallel_index++)
                {
                    result.worker_nodes.emplace_back(cluster_nodes.all_workers[parallel_index]);
                }
            }
            else
            {
                for (size_t parallel_index = 0; parallel_index < plan_segment_ptr->getParallelSize(); parallel_index++)
                {
                    if (parallel_index > plan_segment_ptr->getParallelSize())
                        break;
                    result.worker_nodes.emplace_back(cluster_nodes.all_workers[cluster_nodes.rank_worker_ids[parallel_index]]);
                }
            }
        }
    }
    return result;
}

NodeSelectorResult ComputeNodeSelector::select(PlanSegment * plan_segment_ptr, ContextPtr query_context, DAGGraph * dag_graph_ptr)
{
    checkClusterInfo(plan_segment_ptr);
    NodeSelectorResult result;

    auto local_address = getLocalAddress(*query_context);
    if (dag_graph_ptr->source_pruner && query_context->getSettingsRef().enable_prune_compute_plan_segment
        && dag_graph_ptr->source_pruner->plan_segment_workers_map.contains(plan_segment_ptr->getPlanSegmentId()))
    {
        selectPrunedWorkers(dag_graph_ptr, plan_segment_ptr, result, local_address);
    }
    else
    {
        bool need_stable_schedule = needStableSchedule(plan_segment_ptr);
        if (need_stable_schedule)
        {
            const auto worker_number = cluster_nodes.all_workers.size() - 1;
            LOG_TRACE(log, "use stable schedule for segment:{} with {} nodes", plan_segment_ptr->getPlanSegmentId(), worker_number);
            if (plan_segment_ptr->getParallelSize() != worker_number)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Plan segment {} need stable schedule, but parallel size {} is not equal to worker number {}.",
                    plan_segment_ptr->getPlanSegmentId(),
                    plan_segment_ptr->getParallelSize(),
                    worker_number);
            for (size_t parallel_index = 0; parallel_index < worker_number; parallel_index++)
            {
                result.worker_nodes.emplace_back(cluster_nodes.all_workers[parallel_index]);
            }
        }
        else
        {
            for (size_t parallel_index = 0; parallel_index < plan_segment_ptr->getParallelSize(); parallel_index++)
            {
                if (parallel_index > plan_segment_ptr->getParallelSize())
                    break;
                result.worker_nodes.emplace_back(cluster_nodes.all_workers[cluster_nodes.rank_worker_ids[parallel_index]]);
            }
        }
    }
    return result;
}

NodeSelectorResult LocalityNodeSelector::select(PlanSegment * plan_segment_ptr, ContextPtr query_context, DAGGraph * dag_graph_ptr)
{
    checkClusterInfo(plan_segment_ptr);
    NodeSelectorResult result;
    auto local_input = NodeSelector::tryGetLocalInput(plan_segment_ptr);
    if (local_input)
    {
        std::unique_lock<std::mutex> lock(dag_graph_ptr->finished_address_mutex);
        auto address_it = dag_graph_ptr->finished_address.find(local_input->getPlanSegmentId());
        if (address_it == dag_graph_ptr->finished_address.end())
            throw Exception(
                "Logical error: address of segment " + std::to_string(local_input->getPlanSegmentId()) + " not found",
                ErrorCodes::LOGICAL_ERROR);
        for (const auto & [parallel_index, addr] : address_it->second)
        {
            result.worker_nodes.emplace_back(addr);
        }
        return result;
    }

    if (!query_context->getSettingsRef().bsp_shuffle_reduce_locality_enabled)
    {
        for (size_t i = 0; i < plan_segment_ptr->getParallelSize(); i++)
        {
            result.worker_nodes.emplace_back(WorkerNode{});
        }
        return result;
    }

    auto local_address = getLocalAddress(*query_context);
    for (const AddressInfo & addr : query_context->getExchangeDataTracker()->getExchangeDataAddrs(
             plan_segment_ptr,
             0,
             plan_segment_ptr->getParallelSize(),
             query_context->getSettingsRef().bsp_shuffle_reduce_locality_fraction))
    {
        if (addr == local_address)
            return result;
        // TODO(WangTao): fill worker :id.
        result.worker_nodes.emplace_back(addr);
    }

    return result;
}

NodeSelectorResult NodeSelector::select(PlanSegment * plan_segment_ptr, bool has_table_scan)
{
    NodeSelectorResult result;
    auto segment_id = plan_segment_ptr->getPlanSegmentId();
    LOG_TRACE(log, "Begin to select nodes for segment, id: {}, has table scan: {}", segment_id, has_table_scan);
    if (isLocal(plan_segment_ptr))
    {
        result = local_node_selector.select(plan_segment_ptr, query_context);
    }
    else if (has_table_scan)
    {
        result = source_node_selector.select(plan_segment_ptr, query_context, dag_graph_ptr);
    }
    else
    {
        if (query_context->getSettingsRef().bsp_mode)
        {
            result = locality_node_selector.select(plan_segment_ptr, query_context, dag_graph_ptr);
            if (result.worker_nodes.empty() || result.worker_nodes.size() != plan_segment_ptr->getParallelSize())
            {
                if (query_context->getSettingsRef().enable_bsp_selector_fallback)
                {
                    LOG_WARNING(
                        log,
                        "Select result size {} of plansegment {} doesn't equal to parallel size {}, fallback to compute selector",
                        result.worker_nodes.size(),
                        segment_id,
                        plan_segment_ptr->getParallelSize());
                    result = compute_node_selector.select(plan_segment_ptr, query_context, dag_graph_ptr);
                }
                else
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Select result size {} of plansegment {} doesn't equal to parallel size {}, fallback to compute selector",
                        result.worker_nodes.size(),
                        segment_id,
                        plan_segment_ptr->getParallelSize());
                }
            }
        }
        else
        {
            result = compute_node_selector.select(plan_segment_ptr, query_context, dag_graph_ptr);
        }
    }

    setParallelIndexAndSourceAddrs(plan_segment_ptr, &result);
    auto it = dag_graph_ptr->id_to_segment.find(segment_id);
    if (it == dag_graph_ptr->id_to_segment.end())
        throw Exception("Logical error: plan segment segment can not be found", ErrorCodes::LOGICAL_ERROR);

    LOG_TRACE(log, "Select node for plansegment {} result {}", segment_id, result.toString());
    return result;
}

PlanSegmentInputPtr NodeSelector::tryGetLocalInput(PlanSegment * plan_segment_ptr)
{
    const auto & inputs = plan_segment_ptr->getPlanSegmentInputs();
    auto it = std::find_if(inputs.begin(), inputs.end(), [](PlanSegmentInputPtr input) {
        return input->getExchangeMode() == ExchangeMode::LOCAL_NO_NEED_REPARTITION
            || input->getExchangeMode() == ExchangeMode::LOCAL_MAY_NEED_REPARTITION;
    });
    return it != inputs.end() ? *it : nullptr;
}


} // namespace DB
