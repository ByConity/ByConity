#include <algorithm>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/ExchangeMode.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/DistributedStages/SourceTask.h>
#include <Interpreters/NodeSelector.h>
#include <QueryPlan/TableScanStep.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <bthread/mutex.h>
#include <Common/Exception.h>
#include <Common/HostWithPorts.h>
#include <common/defines.h>
#include <common/logger_useful.h>
#include <common/types.h>
#include "QueryPlan/ExchangeStep.h"

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

void divideSourceTaskByBucket(
    const std::unordered_map<AddressInfo, SourceTaskPayloadOnWorker, AddressInfo::Hash> & payloads,
    size_t weight_sum,
    size_t parallel_size,
    NodeSelectorResult & result)
{
    if (payloads.empty() || parallel_size == 0)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            fmt::format("Invalid argument for divideSourceTaskByBucket payloads.size:{} parallel_size:{}", payloads.size(), parallel_size));

    size_t assigned_instances = 0;
    while (assigned_instances < parallel_size)
    {
        for (const auto & [addr, payload_on_worker] : payloads)
        {
            size_t weight = payload_on_worker.rows;
            size_t to_be_assigned = weight_sum == 0 ? 0 : weight * parallel_size / weight_sum;
            /// to_be_assigned <= bucket_groups.size, as to avoid empty plan segment instance.
            to_be_assigned = std::min(to_be_assigned, payload_on_worker.bucket_groups.size());
            size_t already_assigned = std::min(to_be_assigned, result.buckets_on_workers[addr].size());
            to_be_assigned = to_be_assigned - already_assigned;
            ///  make sure there is no infinte loop
            to_be_assigned = std::max(1UL, to_be_assigned);
            for (size_t p = 0; p < to_be_assigned && assigned_instances < parallel_size; p++)
            {
                result.buckets_on_workers[addr].emplace_back(std::set<Int64>{});
                result.worker_nodes.emplace_back(WorkerNode(addr, NodeType::Remote, payload_on_worker.worker_id));
                assigned_instances++;
            }
        }
    }

    for (const auto & [addr, payload_on_worker] : payloads)
    {
        auto & buckets_on_worker = result.buckets_on_workers[addr];
        const auto & bucket_groups = payload_on_worker.bucket_groups;
        auto step = buckets_on_worker.empty() ? 1 : std::max(1UL, bucket_groups.size() / buckets_on_worker.size());
        size_t p_id = 0;
        auto start_iter = bucket_groups.begin();
        auto end_iter = bucket_groups.begin();
        size_t last_start = 0, last_end = 0;
        for (auto & buckets : buckets_on_worker)
        {
            auto start = std::min(p_id * step, bucket_groups.size());
            auto end = std::min((p_id + 1) * step, bucket_groups.size());
            if (p_id + 1 == buckets_on_worker.size())
                end = bucket_groups.size();
            std::advance(start_iter, start - last_start);
            std::advance(end_iter, end - last_end);
            last_start = start;
            last_end = end;
            for (auto ii = start_iter; ii != end_iter; std::advance(ii, 1))
            {
                const auto & bucket_group = ii->second;
                buckets.insert(bucket_group.begin(), bucket_group.end());
            }
            if (start == end)
                buckets.insert(kInvalidBucketNumber); // insert one invalid bucket number to mark empty set. used by filterParts
            p_id++;
        }
    }
}

void divideSourceTaskByPart(
    const std::unordered_map<AddressInfo, SourceTaskPayloadOnWorker, AddressInfo::Hash> & payloads,
    size_t weight_sum,
    size_t parallel_size,
    NodeSelectorResult & result)
{
    if (payloads.empty() || parallel_size == 0)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            fmt::format("Invalid argument for divideSourceTaskByPart payloads.size:{} parallel_size:{}", payloads.size(), parallel_size));

    size_t assigned_instances = 0;
    while (assigned_instances < parallel_size)
    {
        for (auto iter = payloads.begin(); iter != payloads.end() && assigned_instances < parallel_size; iter++)
        {
            const auto & addr = iter->first;
            const auto & payload_on_worker = iter->second;
            size_t weight = payload_on_worker.rows;
            size_t to_be_assigned = weight_sum == 0 ? 0 : weight * parallel_size / weight_sum;
            /// to_be_assigned <= part num, as to avoid empty plan segment instance.
            to_be_assigned = std::min(to_be_assigned, payload_on_worker.part_num);
            size_t already_assigned = std::min(to_be_assigned, result.source_task_count_on_workers[addr]);
            to_be_assigned = to_be_assigned - already_assigned;
            ///  make sure there is no infinte loop
            to_be_assigned = std::max(1UL, to_be_assigned);
            for (size_t p = 0; p < to_be_assigned && assigned_instances < parallel_size; p++)
            {
                result.source_task_count_on_workers[addr]++;
                result.worker_nodes.emplace_back(WorkerNode(addr, NodeType::Remote, iter->second.worker_id));
                assigned_instances++;
            }
        }
    }
}

/// get min buckets from storage input if possible, or else return kInvalidBucketNumber
Int64 getMinNumOfBuckets(const PlanSegmentInputs & inputs)
{
    if (inputs.empty())
        return kInvalidBucketNumber;
    Int64 min_bucket_number = inputs[0]->getNumOfBuckets();
    std::vector<Int64> nums_of_buckets;
    nums_of_buckets.reserve(inputs.size());
    for (const auto & input : inputs)
    {
        Int64 bucket_number = input->getNumOfBuckets();
        min_bucket_number = std::min(min_bucket_number, bucket_number);
        if (bucket_number != kInvalidBucketNumber)
            nums_of_buckets.emplace_back(bucket_number);
    }

    if (min_bucket_number != kInvalidBucketNumber)
    {
        for (auto iter : nums_of_buckets)
        {
            if (iter % min_bucket_number != 0)
                return kInvalidBucketNumber;
        }
    }

    return min_bucket_number;
}

bool hasBucketScan(const PlanSegment & plan_segment)
{
    bool has_bucket_scan = false;
    for (const auto & node : plan_segment.getQueryPlan().getNodes())
    {
        if (auto table_scan_step = std::dynamic_pointer_cast<TableScanStep>(node.step))
        {
            if (!table_scan_step->isBucketScan())
                return false;
            else
                has_bucket_scan = true;
        }
    }
    return has_bucket_scan;
}

NodeSelectorResult SourceNodeSelector::select(PlanSegment * plan_segment_ptr, ContextPtr query_context, DAGGraph * dag_graph_ptr)
{
    checkClusterInfo(plan_segment_ptr);
    bool need_stable_schedule = needStableSchedule(plan_segment_ptr);
    NodeSelectorResult result;
    // The one worker excluded is server itself.
    const auto worker_number = cluster_nodes.all_workers.empty() ? 0 : cluster_nodes.all_workers.size() - 1;
    if (plan_segment_ptr->getParallelSize() > worker_number && (!query_context->getSettingsRef().bsp_mode || need_stable_schedule))
    {
        throw Exception(
            ErrorCodes::BAD_QUERY_PARAMETER,
            "Logical error: distributed_max_parallel_size({}) of table scan can not be greater than worker number({})",
            plan_segment_ptr->getParallelSize(),
            worker_number);
    }

    /// will be obsolete in the future
    auto old_func = [&](std::unordered_map<AddressInfo, SourceTaskPayloadOnWorker, AddressInfo::Hash> & payload_on_workers,
                        size_t rows_count,
                        size_t parallel_size) {
        size_t avg = rows_count / parallel_size + 1;
        if (rows_count < parallel_size)
            rows_count = 0;
        if (rows_count > 0)
        {
            // Assign parallelism accroding to regular average size.
            for (auto & [addr, payload] : payload_on_workers)
            {
                size_t s = payload.rows;
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
                    result.worker_nodes.emplace_back(addr, NodeType::Remote, payload.worker_id);
                    result.source_task_count_on_workers[addr]++;
                }
            }
        }
        // Assign parallelism according to major part(>0.5) of average size, if needed.
        if (result.worker_nodes.size() < parallel_size)
        {
            for (auto & [addr, payload] : payload_on_workers)
            {
                size_t s = payload.rows;
                if (s * 2 > avg)
                {
                    result.worker_nodes.emplace_back(addr, NodeType::Remote, payload.worker_id);
                    result.source_task_count_on_workers[addr]++;
                    s = 0;
                }
                if (result.worker_nodes.size() == parallel_size)
                    break;
            }
        }
        // Assign parallelism to each worker until no one left.
        while (result.worker_nodes.size() < parallel_size)
        {
            for (const auto & [addr, payload] : payload_on_workers)
            {
                result.worker_nodes.emplace_back(addr, NodeType::Remote, payload.worker_id);
                result.source_task_count_on_workers[addr]++;
                if (result.worker_nodes.size() == parallel_size)
                    break;
            }
        }
    };

    // If parallelism is greater than the worker number, we split the parts according to the input size.
    if (plan_segment_ptr->getParallelSize() > worker_number)
    {
        // initialize payload_per_worker with empty payload, so that empty table will select nodes correcly
        std::unordered_map<AddressInfo, SourceTaskPayloadOnWorker, AddressInfo::Hash> payload_on_workers;
        for (const auto & worker : cluster_nodes.all_workers)
        {
            if (worker.type == NodeType::Remote)
                payload_on_workers[worker.address] = {.worker_id = worker.id};
        }
        size_t rows_count = 0;
        bool is_bucket_valid = true;
        Int64 min_num_of_buckets = getMinNumOfBuckets(plan_segment_ptr->getPlanSegmentInputs());
        if (min_num_of_buckets == kInvalidBucketNumber)
            is_bucket_valid = false;

        is_bucket_valid = is_bucket_valid && hasBucketScan(*plan_segment_ptr);

        const auto & source_task_payload_map = query_context->getCnchServerResource()->getSourceTaskPayload();
        for (const auto & plan_segment_input : plan_segment_ptr->getPlanSegmentInputs())
        {
            auto storage_id = plan_segment_input->getStorageID();
            if (storage_id && storage_id->hasUUID())
            {
                auto iter = source_task_payload_map.find(storage_id->uuid);
                if (iter != source_task_payload_map.end())
                {
                    for (const auto & [addr, p] : iter->second)
                    {
                        rows_count += p.rows;
                        auto & worker_payload = payload_on_workers[addr];
                        worker_payload.rows += p.rows;
                        worker_payload.part_num += 1;
                        if (is_bucket_valid)
                        {
                            for (auto bucket : p.buckets)
                            {
                                auto key = bucket % min_num_of_buckets;
                                worker_payload.bucket_groups[key].insert(bucket);
                            }
                        }
                    }
                }
            }
        }

        if (is_bucket_valid)
            divideSourceTaskByBucket(payload_on_workers, rows_count, plan_segment_ptr->getParallelSize(), result);
        else
            old_func(payload_on_workers, rows_count, plan_segment_ptr->getParallelSize());
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
    if (dag_graph_ptr->source_pruner && query_context->getSettingsRef().enable_prune_source_plan_segment
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

String NodeSelectorResult::toString() const
{
    fmt::memory_buffer buf;
    fmt::format_to(buf, "Target Address:");
    for (const auto & node_info : worker_nodes)
    {
        fmt::format_to(buf, "{},", node_info.address.toShortString());
    }
    for (const auto & [addr, source_task_count] : source_task_count_on_workers)
        fmt::format_to(buf, "\tSourceTaskCount(addr:{} count:{})", addr.toShortString(), source_task_count);
    for (const auto & [addr, buckets_on_worker] : buckets_on_workers)
    {
        for (const auto & buckets : buckets_on_worker)
        {
            fmt::format_to(
                buf,
                "\tSourceBuckets(addr:{} buckets:{})",
                addr.toShortString(),
                boost::join(buckets | boost::adaptors::transformed([](auto b) { return std::to_string(b); }), ","));
        }
    }
    return fmt::to_string(buf);
}

} // namespace DB
