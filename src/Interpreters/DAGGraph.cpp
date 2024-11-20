#include "DAGGraph.h"
#include <iterator>
#include <memory>
#include <unordered_set>

#include <CloudServices/CnchServerResource.h>
#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/StorageID.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BRPC_EXCEPTION;
}

void DAGGraph::joinAsyncRpcPerStage()
{
    if (query_context->getSettingsRef().send_plan_segment_by_brpc_join_at_last)
        return;
    if (query_context->getSettingsRef().send_plan_segment_by_brpc_join_per_stage)
        joinAsyncRpcWithThrow();
}

void DAGGraph::joinAsyncRpcWithThrow()
{
    auto async_ret = async_context->wait();
    if (async_ret.status == AsyncContext::AsyncStats::FAILED)
        throw Exception(
            "send plan segment async failed error code : " + toString(async_ret.error_code) + " error worker : " + async_ret.failed_worker
                + " error text : " + async_ret.error_text,
            ErrorCodes::BRPC_EXCEPTION);
}

void DAGGraph::joinAsyncRpcAtLast()
{
    if (query_context->getSettingsRef().send_plan_segment_by_brpc_join_at_last)
        joinAsyncRpcWithThrow();
}

/// return addresses order by parallel id
AddressInfos DAGGraph::getAddressInfos(size_t segment_id)
{
    /// for bsp_mode we need get worker addresses from finished_address, because retry might happen
    if (query_context->getSettingsRef().bsp_mode)
    {
        std::unique_lock<std::mutex> lock(finished_address_mutex);
        if (!finished_address.contains(segment_id))
        {
            throw Exception(
                "Logical error: address of segment " + std::to_string(segment_id) + " can not be found in finished_address",
                ErrorCodes::LOGICAL_ERROR);
        }
        AddressInfos addresses;
        addresses.reserve(finished_address[segment_id].size());
        for (const auto & p : finished_address[segment_id])
        {
            addresses.push_back(p.second);
        }
        return addresses;
    }
    else
    {
        if (!id_to_address.contains(segment_id))
        {
            throw Exception(
                "Logical error: address of segment " + std::to_string(segment_id) + " can not be found in id_to_address",
                ErrorCodes::LOGICAL_ERROR);
        }
        return id_to_address[segment_id];
    }
}

void SourcePruner::generateSegmentStorageMap()
{
    for (auto & node : plan_segments_ptr->getNodes())
    {
        for (auto & segment_input : node.getPlanSegment()->getPlanSegmentInputs())
        {
            if (segment_input->getStorageID())
            {
                auto uuid = segment_input->getStorageID()->uuid;
                LOG_TRACE(
                    log,
                    "SourcePrune plan segment {} storage id : {}",
                    node.getPlanSegment()->getPlanSegmentId(),
                    segment_input->getStorageID()->getNameForLogs());
                if (segment_input->getStorageID()->hasUUID())
                {
                    plan_segment_storages_map[node.getPlanSegment()->getPlanSegmentId()].insert(uuid);
                }
            }
        }
    }
}

void SourcePruner::generateUnprunableSegments()
{
    for (auto & node : plan_segments_ptr->getNodes())
    {
        for (auto & segment_output : node.getPlanSegment()->getPlanSegmentOutputs())
        {
            if (segment_output->getExchangeMode() == ExchangeMode::LOCAL_MAY_NEED_REPARTITION
                || segment_output->getExchangeMode() == ExchangeMode::LOCAL_NO_NEED_REPARTITION)
            {
                unprunable_plan_segments.insert(node.getPlanSegment()->getPlanSegmentId());
                unprunable_plan_segments.insert(segment_output->getPlanSegmentId());
            }
        }
        for (const auto & segment_input : node.getPlanSegment()->getPlanSegmentInputs())
        {
            if (segment_input->isStable())
            {
                unprunable_plan_segments.insert(node.getPlanSegment()->getPlanSegmentId());
            }
        }
    }
}

void SourcePruner::prepare()
{
    generateSegmentStorageMap();
    generateUnprunableSegments();
}

void SourcePruner::pruneSource(CnchServerResource * server_resource, std::unordered_map<size_t, PlanSegment *> & id_to_segment)
{
    prepare();
    for (auto & node : plan_segments_ptr->getNodes())
    {
        auto plan_segment_id = node.getPlanSegment()->getPlanSegmentId();
        if (unprunable_plan_segments.contains(plan_segment_id))
            continue;

        if (!plan_segment_storages_map[plan_segment_id].empty())
        {
            for (const auto & uuid : plan_segment_storages_map[plan_segment_id])
            {
                const auto & target_workers = server_resource->getAssignedWorkers(uuid);
                plan_segment_workers_map[plan_segment_id].insert(target_workers.begin(), target_workers.end());
            }
            plan_segment_workers_map[plan_segment_id];
            LOG_TRACE(
                log, "SourcePrune plan segment : {} worker size {}", plan_segment_id, plan_segment_workers_map[plan_segment_id].size());
        }
    }
    for (auto & node : plan_segments_ptr->getNodes())
    {
        auto plan_segment_id = node.getPlanSegment()->getPlanSegmentId();
        auto iter = plan_segment_workers_map.find(plan_segment_id);
        if (iter != plan_segment_workers_map.end())
        {
            auto parallel_size = iter->second.empty() ? 1 : iter->second.size();
            // Adjust the parallel size of pruned plan segment.
            node.getPlanSegment()->setParallelSize(parallel_size);
            auto inputs = node.plan_segment->getPlanSegmentInputs();
            for (auto & input : inputs)
            {
                auto child_iter = id_to_segment.find(input->getPlanSegmentId());
                if (child_iter != id_to_segment.end())
                {
                    for (auto & output : child_iter->second->getPlanSegmentOutputs())
                    {
                        if (output->getExchangeId() == input->getExchangeId())
                        {
                            // Adjust the parallel size of the input plan segment of the pruned segment.
                            output->setParallelSize(parallel_size);
                        }
                    }
                }
            }
        }
    }
}

PlanSegment * DAGGraph::getPlanSegmentPtr(size_t id)
{
    auto it = id_to_segment.find(id);
    if (it == id_to_segment.end())
    {
        throw Exception("Logical error: segment " + std::to_string(id) + " not found", ErrorCodes::LOGICAL_ERROR);
    }
    return it->second;
}

std::vector<size_t> AdaptiveScheduler::getRandomWorkerRank()
{
    std::vector<size_t> rank_worker_ids;
    auto worker_group = query_context->tryGetCurrentWorkerGroup();
    if (worker_group)
    {
        const auto & worker_hosts = worker_group->getHostWithPortsVec();
        rank_worker_ids.resize(worker_hosts.size(), 0);
        std::iota(rank_worker_ids.begin(), rank_worker_ids.end(), 0);
        thread_local std::random_device rd;
        std::shuffle(rank_worker_ids.begin(), rank_worker_ids.end(), rd);
    }
    return rank_worker_ids;
}

std::vector<size_t> AdaptiveScheduler::getHealthyWorkerRank()
{
    std::vector<size_t> rank_worker_ids;
    auto worker_group = query_context->tryGetCurrentWorkerGroup();
    auto worker_group_status = query_context->getWorkerGroupStatusPtr();
    if (!worker_group_status || !worker_group
        || worker_group_status->getWorkersResourceStatus().size() != worker_group->getHostWithPortsVec().size())
        return getRandomWorkerRank();
    const auto & hostports = worker_group->getHostWithPortsVec();
    size_t num_workers = hostports.size();
    rank_worker_ids.reserve(num_workers);
    for (size_t i = 0; i < num_workers; i++)
        rank_worker_ids.push_back(i);

    const auto & resources = worker_group_status->getWorkersResourceStatus();
    std::stable_sort(rank_worker_ids.begin(), rank_worker_ids.end(), [&](size_t lidx, size_t ridx) {
        if (!resources[lidx])
            return false;
        if (!resources[ridx])
            return true;
        return resources[lidx]->compare(*resources[ridx]);
    });

    if (log->trace())
    {
        for (auto & idx : rank_worker_ids)
        {
            if (resources[idx])
                LOG_TRACE(log, resources[idx]->toDebugString());
        }
    }

    thread_local std::random_device rd;
    //only shuffle first half healthy worker
    std::shuffle(rank_worker_ids.begin(), rank_worker_ids.begin() + worker_group_status->getHealthWorkerSize() / 2, rd);
    return rank_worker_ids;
}


} // namespace DB
