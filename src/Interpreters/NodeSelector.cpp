#include <algorithm>
#include <functional>
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
#include <Interpreters/DistributedStages/PlanSegmentInstance.h>
#include <Interpreters/DistributedStages/SourceTask.h>
#include <Interpreters/NodeSelector.h>
#include <QueryPlan/ExchangeStep.h>
#include <QueryPlan/TableScanStep.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <bthread/mutex.h>
#include <Common/Exception.h>
#include <Common/HostWithPorts.h>
#include <common/defines.h>
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

void NodeSelector::setSources(
    PlanSegment * plan_segment_ptr, NodeSelectorResult * result, std::map<PlanSegmentInstanceId, std::vector<UInt32>> & read_partitions)
{
    LOG_TRACE(log, "Set sources for segment id {}", plan_segment_ptr->getPlanSegmentId());
    UInt32 segment_id = plan_segment_ptr->getPlanSegmentId();
    bool contains_table_scan_or_value = dag_graph_ptr->table_scan_or_value_segments.contains(segment_id);

    /// for bsp_mode, as two local input might be dispatched to different nodes,
    /// we only enforce the first
    bool enable_local_input = true;
    size_t current_parallel_size = result->worker_nodes.size();
    bool bsp_mode = query_context->getSettingsRef().bsp_mode;
    /// 1. Under bsp_mode, for cases where a source plan segment contains LOCAL_NO_NEED_REPARTITION input,
    /// as its inputs might not be scheduled according to order specified by cluster nodes(bsp mode will schedule tasks to nodes available currently)
    /// we will convert it into non-local input
    /// 2. For segment 0, under cases where it has a local input, as segment 0 must be scheduled to server
    /// LOCAL_NO_NEED_REPARTITION is disabled
    if ((bsp_mode && contains_table_scan_or_value) || segment_id == 0)
        enable_local_input = false;
    for (auto & plan_segment_input : plan_segment_ptr->getPlanSegmentInputs())
    {
        auto input_plan_segment_id = plan_segment_input->getPlanSegmentId();
        auto exchange_id = plan_segment_input->getExchangeId();
        if (plan_segment_input->getPlanSegmentType() != PlanSegmentType::EXCHANGE)
        {
            continue;
        }

        if (auto it = dag_graph_ptr->id_to_segment.find(input_plan_segment_id); it != dag_graph_ptr->id_to_segment.end())
        {
            auto mode = plan_segment_input->getExchangeMode();
            // if data is write to local, so no need to shuffle data
            if (isLocalExchange(mode))
            {
                if (enable_local_input)
                {
                    LOG_TRACE(log, "Local plan segment input, id:{}", input_plan_segment_id);
                    std::shared_ptr<AddressInfo> local_addr = std::make_shared<AddressInfo>("localhost", 0, "", "");
                    result->source_addresses[exchange_id].emplace_back(local_addr);
                    for (UInt32 parallel_id = 0; parallel_id < result->worker_nodes.size(); parallel_id++)
                    {
                        PlanSegmentMultiPartitionSource source = {.exchange_id = exchange_id, .address = local_addr, .partition_ids = {0}};
                        result->sources[PlanSegmentInstanceId{segment_id, parallel_id}].emplace_back(source);
                    }
                }
                else
                {
                    /// local is converted to non-local
                    LOG_TRACE(log, "Local plan segment input is converted to non-local, id:{}", input_plan_segment_id);
                    const auto & addresses = dag_graph_ptr->getAddressInfos(input_plan_segment_id);
                    std::vector<PlanSegmentMultiPartitionSource> sources;
                    UInt32 parallel_id = 0;
                    for (const auto & addr : addresses)
                    {
                        PlanSegmentMultiPartitionSource source
                            = {.exchange_id = exchange_id,
                               .address = std::make_shared<AddressInfo>(addr.getHostName(), addr.getPort(), "", "", addr.getExchangePort()),
                               .partition_ids = {0}};
                        result->sources[PlanSegmentInstanceId{segment_id, parallel_id++}].emplace_back(source);
                        result->source_addresses[exchange_id].emplace_back(source.address);
                    }
                }
                if (bsp_mode)
                    enable_local_input = false;
            }
            else if (mode == ExchangeMode::BROADCAST)
            {
                LOG_TRACE(log, "Broadcast plan segment input, id:{}", input_plan_segment_id);
                const auto & addresses = dag_graph_ptr->getAddressInfos(input_plan_segment_id);
                for (const auto & addr : addresses)
                {
                    result->source_addresses[exchange_id].emplace_back(
                        std::make_shared<AddressInfo>(addr.getHostName(), addr.getPort(), "", "", addr.getExchangePort()));
                }
                /// broadcast will read same partition for each source addresses
                for (UInt32 parallel_id = 0; parallel_id < current_parallel_size; parallel_id += 1)
                {
                    for (auto & addr : result->source_addresses[exchange_id])
                    {
                        auto source = PlanSegmentMultiPartitionSource{.exchange_id = exchange_id, .address = addr, .partition_ids = {}};
                        source.partition_ids.clear();
                        source.partition_ids = {parallel_id};
                        result->sources[PlanSegmentInstanceId{segment_id, parallel_id}].emplace_back(source);
                    }
                }
            }
            else
            {
                LOG_TRACE(log, "Non-local plan segment input, id:{}", input_plan_segment_id);
                const auto & addresses = dag_graph_ptr->getAddressInfos(input_plan_segment_id);
                for (const auto & addr : addresses)
                {
                    result->source_addresses[exchange_id].emplace_back(
                        std::make_shared<AddressInfo>(addr.getHostName(), addr.getPort(), "", "", addr.getExchangePort()));
                }
                for (UInt32 parallel_id = 0; parallel_id < current_parallel_size; parallel_id++)
                {
                    for (const auto & addr : result->source_addresses[exchange_id])
                    {
                        PlanSegmentMultiPartitionSource source
                            = {.exchange_id = exchange_id,
                               .address = addr,
                               .partition_ids = read_partitions[PlanSegmentInstanceId{segment_id, parallel_id}]};
                        result->sources[PlanSegmentInstanceId{segment_id, parallel_id}].emplace_back(std::move(source));
                    }
                }
            }
        }
    }
}

/// This method will try to coalesce small partitions (with size less than disk_shuffle_advisory_partition_size)
/// into one new partition. For example, if we have 5 partitions like [2, 2, 1, 2, 4] and disk_shuffle_advisory_partition_size=3,
/// then after coalescing, we will get 3 new partitions [{2,2}, {1, 2}, {4}]. For broadcast partition, it will only be read once in newly-coalesced partition,
/// thus, when calculating the new partition, it is only added once.
std::map<PlanSegmentInstanceId, std::vector<UInt32>> NodeSelector::coalescePartitions(
    UInt32 segment_id, size_t disk_shuffle_advisory_partition_size, size_t broadcast_partition, std::vector<size_t> & normal_partitions)
{
    std::map<PlanSegmentInstanceId, std::vector<UInt32>> result;
    UInt32 parallel_id = 0;
    size_t partition_id = 0;
    size_t current_size = 0;
    while (partition_id < normal_partitions.size())
    {
        if (!result.contains({segment_id, parallel_id}))
            current_size += broadcast_partition;
        result[{segment_id, parallel_id}].emplace_back(partition_id);
        current_size += normal_partitions[partition_id];
        if (current_size >= disk_shuffle_advisory_partition_size)
        {
            current_size = 0;
            parallel_id++;
        }
        partition_id++;
    }
    return result;
}

std::map<PlanSegmentInstanceId, std::vector<UInt32>> NodeSelector::splitReadPartitions(PlanSegment * plan_segment_ptr)
{
    std::map<PlanSegmentInstanceId, std::vector<UInt32>> result;
    UInt32 segment_id = plan_segment_ptr->getPlanSegmentId();
    const auto & query_id = plan_segment_ptr->getQueryId();
    /// Partition coalescing will be disabled for:
    /// 1. CTE's input and output segment(i.e. segment with intput/output exchange mode LOCAL_NO_NEED_REPARTITION)
    /// 2. gather segment
    size_t disk_shuffle_advisory_partition_size = query_context->getSettingsRef().disk_shuffle_advisory_partition_size;
    auto & plan_segment = *dag_graph_ptr->id_to_segment[segment_id];
    if (query_context->getSettingsRef().enable_disk_shuffle_partition_coalescing && !dag_graph_ptr->leaf_segments.contains(segment_id)
        && !dag_graph_ptr->table_scan_or_value_segments.contains(segment_id) && !plan_segment.hasLocalInput()
        && !plan_segment.hasLocalOutput())
    {
        /// each partition's size
        std::vector<size_t> normal_partitions;
        size_t broadcast_partition = 0;
        for (const auto & plan_segment_input : plan_segment.getPlanSegmentInputs())
        {
            auto exchange_id = plan_segment_input->getExchangeId();
            const auto & exchange_data_statuses = query_context->getExchangeDataTracker()->getExchangeStatusesRef(query_id, exchange_id);
            auto exchange_mode = plan_segment_input->getExchangeMode();
            for (const auto & sink_status : exchange_data_statuses.sink_statuses)
            {
                if (normal_partitions.empty())
                {
                    normal_partitions.resize(sink_status.status.size());
                    std::fill_n(normal_partitions.begin(), sink_status.status.size(), 0);
                }
                for (UInt32 partition_id = 0; partition_id < sink_status.status.size(); partition_id++)
                {
                    /// broadcase exchange should not be repeatedly added
                    if (exchange_mode == ExchangeMode::BROADCAST)
                    {
                        broadcast_partition += sink_status.status[partition_id];
                        break;
                    }
                    else
                    {
                        if (normal_partitions.size() != sink_status.status.size())
                            throw Exception(
                                ErrorCodes::LOGICAL_ERROR,
                                "Exchange(id:{}) sink statuses' partition size do not match, actual:{} expected:{}",
                                exchange_id,
                                normal_partitions.size(),
                                sink_status.status.size());
                        normal_partitions[partition_id] += sink_status.status[partition_id];
                    }
                }
            }
        }

        result = coalescePartitions(segment_id, disk_shuffle_advisory_partition_size, broadcast_partition, normal_partitions);

        LOG_DEBUG(
            log,
            "Plan segment {} is coalesced from {} to {}",
            plan_segment.getPlanSegmentId(),
            plan_segment.getParallelSize(),
            result.size());
    }
    else
    {
        size_t current_parallel_size = plan_segment_ptr->getParallelSize();
        for (UInt32 parallel_id = 0; parallel_id < current_parallel_size; parallel_id++)
            result[PlanSegmentInstanceId{segment_id, parallel_id}] = {};
        for (auto & plan_segment_input : plan_segment_ptr->getPlanSegmentInputs())
        {
            auto exchange_id = plan_segment_input->getExchangeId();
            auto input_plan_segment_id = plan_segment_input->getPlanSegmentId();
            if (plan_segment_input->getPlanSegmentType() != PlanSegmentType::EXCHANGE)
            {
                continue;
            }

            if (auto it = dag_graph_ptr->id_to_segment.find(input_plan_segment_id); it != dag_graph_ptr->id_to_segment.end())
            {
                auto mode = plan_segment_input->getExchangeMode();
                size_t exchange_parallel_size = query_context->getSettingsRef().exchange_parallel_size;
                if (mode == ExchangeMode::BROADCAST)
                    exchange_parallel_size = 1;
                auto input_parallel_size = exchange_parallel_size == 0
                    ? dag_graph_ptr->exchanges[exchange_id].second->getParallelSize()
                    : dag_graph_ptr->exchanges[exchange_id].second->getParallelSize() * exchange_parallel_size;
                // if data is write to local, so no need to shuffle data
                if (!isLocalExchange(mode) && mode != ExchangeMode::BROADCAST)
                {
                    auto step = std::max(1UL, input_parallel_size / current_parallel_size);
                    /// If we have exchange_parallel_size=1, input_parallel_size=4, current_parallel_size=2, two physical nodes(i.e. 2 different addresses) then we have:
                    ///     plan_segment_instance_0: source{addr1, partition_ids:[0,1]}, source{addr1, partition_ids:[0,1]}, source{addr2, partition_ids:[0,1]}, source{addr2, partition_ids:[0,1]}
                    ///     plan_segment_instance_1: source{addr1, partition_ids:[2,3]}, source{addr1, partition_ids:[2,3]}, source{addr2, partition_ids:[2,3]}, source{addr2, partition_ids:[2,3]}
                    for (UInt32 partition_id = 0; partition_id < input_parallel_size; partition_id += step)
                    {
                        std::vector<UInt32> partition_ids;
                        for (size_t diff = 0; diff < step && partition_id + diff < input_parallel_size; diff++)
                        {
                            partition_ids.emplace_back(partition_id + diff);
                        }
                        UInt32 parallel_id = partition_id / exchange_parallel_size;
                        result[PlanSegmentInstanceId{segment_id, parallel_id}] = partition_ids;
                    }
                    break;
                }
            }
        }
    }

    return result;
}

NodeSelectorResult LocalNodeSelector::select(PlanSegment *, ContextPtr query_context)
{
    NodeSelectorResult result;
    auto local_address = getLocalAddress(*query_context);
    result.worker_nodes.emplace_back(local_address, NodeType::Local);
    return result;
}

/// workers will be assigning tasks by the order of its data size
std::vector<const AddressInfo *> orderAddrByRows(std::unordered_map<AddressInfo, SourceTaskPayloadOnWorker, AddressInfo::Hash> & payloads)
{
    std::vector<const AddressInfo *> ordered_addrs;
    ordered_addrs.reserve(payloads.size());
    for (const auto & p : payloads)
        ordered_addrs.emplace_back(&p.first);
    /// order workers by weight
    std::sort(ordered_addrs.begin(), ordered_addrs.end(), [&](auto * l, auto * r) { return payloads[*l].rows > payloads[*r].rows; });
    return ordered_addrs;
}

/// assign one task for each non-empty worker
size_t initNodeSelectorResult(
    const std::vector<const AddressInfo *> & ordered_addrs,
    std::unordered_map<AddressInfo, SourceTaskPayloadOnWorker, AddressInfo::Hash> & payloads,
    size_t parallel_size,
    NodeSelectorResult & result,
    std::function<void(const AddressInfo &)> init_source_func)
{
    size_t assigned_instances = 0;
    for (const auto * addr_p : ordered_addrs)
    {
        const auto & addr = *addr_p;
        const auto & payload_on_worker = payloads[addr];
        if (payload_on_worker.rows > 0 && parallel_size > 0)
        {
            init_source_func(addr);
            result.worker_nodes.emplace_back(WorkerNode(addr, NodeType::Remote, payload_on_worker.worker_id));
            assigned_instances++;
        }
    }
    return assigned_instances;
}

void divideSourceTaskByBucket(
    std::unordered_map<AddressInfo, SourceTaskPayloadOnWorker, AddressInfo::Hash> & payloads,
    size_t weight_sum,
    size_t parallel_size,
    NodeSelectorResult & result)
{
    if (payloads.empty() || parallel_size == 0)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            fmt::format("Invalid argument for divideSourceTaskByBucket payloads.size:{} parallel_size:{}", payloads.size(), parallel_size));

    std::vector<const AddressInfo *> ordered_addrs = orderAddrByRows(payloads);
    size_t assigned_instances = initNodeSelectorResult(ordered_addrs, payloads, parallel_size, result, [&](const AddressInfo & addr) {
        result.buckets_on_workers[addr].emplace_back(std::set<Int64>{});
    });

    while (assigned_instances < parallel_size)
    {
        for (const auto & addr_p : ordered_addrs)
        {
            const auto & addr = *addr_p;
            const auto & payload_on_worker = payloads.find(addr)->second;

            size_t to_be_assigned = weight_sum == 0 ? 0 : payload_on_worker.rows * parallel_size / weight_sum;
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
    std::unordered_map<AddressInfo, SourceTaskPayloadOnWorker, AddressInfo::Hash> & payloads,
    size_t weight_sum,
    size_t parallel_size,
    NodeSelectorResult & result)
{
    if (payloads.empty() || parallel_size == 0)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            fmt::format("Invalid argument for divideSourceTaskByPart payloads.size:{} parallel_size:{}", payloads.size(), parallel_size));

    std::vector<const AddressInfo *> ordered_addrs = orderAddrByRows(payloads);
    size_t assigned_instances = initNodeSelectorResult(
        ordered_addrs, payloads, parallel_size, result, [&](const AddressInfo & addr) { result.source_task_count_on_workers[addr]++; });

    while (assigned_instances < parallel_size)
    {
        for (const auto & addr_p : ordered_addrs)
        {
            const auto & addr = *addr_p;
            const auto & payload_on_worker = payloads.find(addr)->second;

            size_t to_be_assigned = weight_sum == 0 ? 0 : payload_on_worker.rows * parallel_size / weight_sum;
            /// to_be_assigned <= part num, as to avoid empty plan segment instance.
            to_be_assigned = std::min(to_be_assigned, payload_on_worker.part_num);
            size_t already_assigned = std::min(to_be_assigned, result.source_task_count_on_workers[addr]);
            to_be_assigned = to_be_assigned - already_assigned;
            ///  make sure there is no infinte loop
            to_be_assigned = std::max(1UL, to_be_assigned);

            for (size_t p = 0; p < to_be_assigned && assigned_instances < parallel_size; p++)
            {
                result.source_task_count_on_workers[addr]++;
                result.worker_nodes.emplace_back(WorkerNode(addr, NodeType::Remote, payload_on_worker.worker_id));
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
            divideSourceTaskByPart(payload_on_workers, rows_count, plan_segment_ptr->getParallelSize(), result);
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

NodeSelectorResult NodeSelector::select(PlanSegment * plan_segment_ptr, bool has_table_scan_or_value)
{
    NodeSelectorResult result;
    auto segment_id = plan_segment_ptr->getPlanSegmentId();
    LOG_TRACE(log, "Begin to select nodes for segment, id: {}, has table scan/value: {}", segment_id, has_table_scan_or_value);

    if (!query_context->getSettingsRef().bsp_mode)
    {
        if (isLocal(plan_segment_ptr))
            result = local_node_selector.select(plan_segment_ptr, query_context);
        else if (has_table_scan_or_value)
            result = source_node_selector.select(plan_segment_ptr, query_context, dag_graph_ptr);
        else
            result = compute_node_selector.select(plan_segment_ptr, query_context, dag_graph_ptr);
        std::map<PlanSegmentInstanceId, std::vector<UInt32>> read_partitions;
        setSources(plan_segment_ptr, &result, read_partitions);
    }
    else
    {
        auto read_partitions = splitReadPartitions(plan_segment_ptr);
        plan_segment_ptr->setParallelSize(read_partitions.size());

        if (isLocal(plan_segment_ptr))
        {
            result = local_node_selector.select(plan_segment_ptr, query_context);
        }
        else if (has_table_scan_or_value)
        {
            result = source_node_selector.select(plan_segment_ptr, query_context, dag_graph_ptr);
        }
        else
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
        auto it = dag_graph_ptr->id_to_segment.find(segment_id);
        if (it == dag_graph_ptr->id_to_segment.end())
            throw Exception("Logical error: plan segment segment can not be found", ErrorCodes::LOGICAL_ERROR);
        setSources(plan_segment_ptr, &result, read_partitions);
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
    fmt::format_to(buf, "\tSourceAddr:");
    for (const auto & [exchange_id, addrs] : source_addresses)
    {
        fmt::format_to(buf, "[exg_id {}, addrs:", exchange_id);
        for (const auto & addr : addrs)
            fmt::format_to(buf, "{},", addr->toShortString());
        fmt::format_to(buf, "],");
    }
    fmt::format_to(buf, "\tReadSource:");
    for (const auto & [instance_id, read_sources] : sources)
    {
        fmt::format_to(buf, "[{}:", instance_id.toString());
        for (const auto & read_source : read_sources)
            fmt::format_to(buf, "{},", read_source.toString());
        fmt::format_to(buf, "],");
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
