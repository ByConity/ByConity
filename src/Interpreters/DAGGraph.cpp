#include "DAGGraph.h"

#include <Interpreters/Context.h>

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
    if (!worker_group_status || !worker_group)
        return getRandomWorkerRank();
    const auto & hostports = worker_group->getHostWithPortsVec();
    size_t num_workers = hostports.size();
    rank_worker_ids.reserve(num_workers);
    for (size_t i = 0; i < num_workers; i++)
        rank_worker_ids.push_back(i);

    auto & workers_status = worker_group_status->getWorkersStatus();
    std::stable_sort(rank_worker_ids.begin(), rank_worker_ids.end(), [&](size_t lidx, size_t ridx) {
        auto lid = WorkerStatusManager::getWorkerId(worker_group->getVWName(), worker_group->getID(), hostports[lidx].id);
        auto rid = WorkerStatusManager::getWorkerId(worker_group->getVWName(), worker_group->getID(), hostports[ridx].id);
        auto liter = workers_status.find(lid);
        auto riter = workers_status.find(rid);
        if (liter == workers_status.end())
            return true;
        if (riter == workers_status.end())
            return false;
        return liter->second->compare(*riter->second);
    });

    if (log->trace())
    {
        for (auto & idx : rank_worker_ids)
        {
            auto worker_id = WorkerStatusManager::getWorkerId(worker_group->getVWName(), worker_group->getID(), hostports[idx].id);
            if (auto iter = workers_status.find(worker_id); iter != workers_status.end())
                LOG_TRACE(log, iter->second->toDebugString());
        }
    }

    thread_local std::random_device rd;
    //only shuffle healthy worker
    std::shuffle(rank_worker_ids.begin(), rank_worker_ids.begin() + worker_group_status->getHealthWorkerSize(), rd);
    return rank_worker_ids;
}

} // namespace DB
