#include <unordered_map>
#include <Interpreters/NodeSelector.h>
#include <Interpreters/sendPlanSegment.h>
#include "common/logger_useful.h"
#include "common/types.h"
#include "Interpreters/DistributedStages/AddressInfo.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void setPlansegmentParallelIndex(
    PlanSegment * plan_segment_ptr,
    DAGGraph * dag_graph_ptr,
    NodeSelectorResult * result,
    Poco::Logger * log,
    NodeSelectorPolicy /* node_select_policy */)
{
    LOG_TRACE(log, "Set parallel index for segment id {}", plan_segment_ptr->getPlanSegmentId());
    //set parallel index
    for (size_t parallel_index_id_index = 0; parallel_index_id_index < result->worker_nodes.size(); parallel_index_id_index++)
        result->indexes.emplace_back(parallel_index_id_index);

    //set input source addresses
    for (auto & plan_segment_input : plan_segment_ptr->getPlanSegmentInputs())
    {
        auto plan_segment_input_id = plan_segment_input->getPlanSegmentId();
        if (plan_segment_input->getPlanSegmentType() != PlanSegmentType::EXCHANGE)
        {
            continue;
        }

        auto source_iter = result->source_addresses.end();

        // if input mode is local, set parallel index to 1
        if (auto it = dag_graph_ptr->id_to_segment.find(plan_segment_input_id); it != dag_graph_ptr->id_to_segment.end())
        {
            for (auto & plan_segment_output : it->second->getPlanSegmentOutputs())
            {
                if (plan_segment_output->getExchangeId() != plan_segment_input->getExchangeId())
                    continue;
                // if data is write to local, so no need to shuffle data
                if (plan_segment_output->getExchangeMode() == ExchangeMode::LOCAL_NO_NEED_REPARTITION
                    || plan_segment_output->getExchangeMode() == ExchangeMode::LOCAL_MAY_NEED_REPARTITION)
                {
                    LOG_TRACE(log, "Local plan segment plan_segment_input_id:{}", plan_segment_input_id);
                    source_iter
                        = result->source_addresses.emplace(plan_segment_input_id, AddressInfos{AddressInfo("localhost", 0, "", "")}).first;
                    source_iter->second.parallel_index = 0;
                }
                else
                {
                    if (dag_graph_ptr->id_to_address.find(plan_segment_input_id) == dag_graph_ptr->id_to_address.end())
                    {
                        throw Exception(
                            "Logical error: address of segment " + std::to_string(plan_segment_input_id)
                                + " can not be found",
                            ErrorCodes::LOGICAL_ERROR);
                    }
                    LOG_TRACE(log, "Not local plan segment plan_segment_input_id:{}", plan_segment_input_id);
                    source_iter = result->source_addresses.emplace(
                        plan_segment_input_id, dag_graph_ptr->id_to_address[plan_segment_input_id]).first;
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
}

NodeSelectorResult LocalNodeSelector::select(PlanSegment * plan_segment_ptr, ContextPtr query_context)
{
    NodeSelectorResult result;
    auto local_address = getLocalAddress(query_context);
    result.worker_nodes.emplace_back(local_address, NodeType::Local);
    result.indexes.emplace_back(0);
    plan_segment_ptr->setParallelIndex(0);
    return result;
}

NodeSelectorResult SourceNodeSelector::select(PlanSegment * plan_segment_ptr, ContextPtr query_context)
{
    checkClusterInfo(plan_segment_ptr);
    NodeSelectorResult result;
    size_t parallel_index = 0;
    const auto & worker_group = query_context->getCurrentWorkerGroup();
    for (auto i : cluster_nodes.rank_worker_ids)
    {
        parallel_index++;
        if (parallel_index > plan_segment_ptr->getParallelSize())
            break;
        const auto & worker_endpoint = worker_group->getHostWithPortsVec()[i];
        auto worker_address = getRemoteAddress(worker_endpoint, query_context);
        result.worker_nodes.emplace_back(worker_address, NodeType::Remote, worker_endpoint.id);
    }
    return result;
}

NodeSelectorResult ComputeNodeSelector::select(PlanSegment * plan_segment_ptr, ContextPtr query_context)
{
    checkClusterInfo(plan_segment_ptr);
    NodeSelectorResult result;
    const auto & worker_group = query_context->getCurrentWorkerGroup();
    size_t parallel_index = 0;
    for (auto i : cluster_nodes.rank_worker_ids)
    {
        parallel_index++;
        if (parallel_index > plan_segment_ptr->getParallelSize())
            break;
        const auto & worker_endpoint = worker_group->getHostWithPortsVec()[i];
        auto worker_address = getRemoteAddress(worker_endpoint, query_context);
        result.worker_nodes.emplace_back(worker_address, NodeType::Remote, worker_endpoint.id);
    }
    return result;
}

NodeSelectorResult LocalityNodeSelector::select(PlanSegment * plan_segment_ptr, ContextPtr query_context)
{
    checkClusterInfo(plan_segment_ptr);
    NodeSelectorResult result;
    auto local_address = getLocalAddress(query_context);
    for (const AddressInfo & addr :
         query_context->getExchangeDataTracker()->getExchangeDataAddrs(plan_segment_ptr, 0, plan_segment_ptr->getParallelSize()))
    {
        if (addr == local_address)
            return result;
        // TODO(WangTao): fill worker id.
        result.worker_nodes.emplace_back(addr);
    }

    return result;
}

NodeSelectorResult NodeSelector::select(PlanSegment * plan_segment_ptr, bool is_source)
{
    NodeSelectorResult result;
    auto segment_id = plan_segment_ptr->getPlanSegmentId();
    LOG_TRACE(log, "Begin to select nodes for segment, id: {}, is_source: {}", segment_id, is_source);
    auto node_select_policy = NodeSelectorPolicy::InvalidPolicy;
    if (isLocal(plan_segment_ptr))
    {
        node_select_policy = NodeSelectorPolicy::LocalPolicy;
        result = local_node_selector.select(plan_segment_ptr, query_context);
    }
    else if (is_source)
    {
        node_select_policy = NodeSelectorPolicy::SourcePolicy;
        result = source_node_selector.select(plan_segment_ptr, query_context);
    }
    else
    {
        node_select_policy = NodeSelectorPolicy::ComputePolicy;
        if (query_context->getSettingsRef().bsp_mode)
        {
            result = locality_node_selector.select(plan_segment_ptr, query_context);
            if (result.worker_nodes.empty() || result.worker_nodes.size() != plan_segment_ptr->getParallelSize())
            {
                // TODO(WangTao): change it to warning and fix it.
                LOG_INFO(
                    log,
                    "Select result size {} of plansegment {} doesn't equal to parallel size {}, fallback to compute selector",
                    result.worker_nodes.size(),
                    segment_id,
                    plan_segment_ptr->getParallelSize());
                result = compute_node_selector.select(plan_segment_ptr, query_context);
            }
        }
        else
        {
            result = compute_node_selector.select(plan_segment_ptr, query_context);
        }
    }

    setPlansegmentParallelIndex(plan_segment_ptr, dag_graph_ptr, &result, log, node_select_policy);
    auto it = dag_graph_ptr->id_to_segment.find(segment_id);
    if (it == dag_graph_ptr->id_to_segment.end())
        throw Exception("Logical error: plan segment segment can not be found", ErrorCodes::LOGICAL_ERROR);

    dag_graph_ptr->id_to_address.emplace(std::make_pair(segment_id, result.getAddress()));
    LOG_TRACE(log, "Select node for plansegment {} result {}", segment_id, result.toString());
    return result;
}
} // namespace DB
