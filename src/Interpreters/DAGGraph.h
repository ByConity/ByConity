#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/DistributedStages/executePlanSegment.h>
#include <Interpreters/WorkerGroupHandle.h>
#include <bthread/mutex.h>

#ifndef NDEBUG
#define TASK_ASSIGN_DEBUG
#endif

namespace DB
{

struct PlanSegmentsStatus
{
    //TODO dongyifeng add when PlanSegmentInfo is merged
    std::atomic<bool> is_final_stage_start{false};
    std::atomic<bool> is_cancel{false};
    Int32 error_code;
    String exception;
};

using PlanSegmentsStatusPtr = std::shared_ptr<PlanSegmentsStatus>;
using Source = std::vector<size_t>;

struct DAGGraph
{
    DAGGraph()
    {
        async_context = std::make_shared<AsyncContext>();
    }
    DAGGraph(const DAGGraph & other)
    {
        std::unique_lock lock(other.status_mutex);
        sources = other.sources;
        final = other.final;
        scheduled_segments = std::move(other.scheduled_segments);
        id_to_segment = std::move(other.id_to_segment);
        id_to_address = std::move(other.id_to_address);
        plan_segment_status_ptr = std::move(other.plan_segment_status_ptr);
        query_context = other.query_context;
        async_context = std::move(other.async_context);
        segment_paralle_size_map = std::move(other.segment_paralle_size_map);
    }
    void joinAsyncRpcWithThrow();
    void joinAsyncRpcPerStage();
    void joinAsyncRpcAtLast();

    Source sources;
    size_t final = std::numeric_limits<size_t>::max();
    std::set<size_t> scheduled_segments;
    std::unordered_map<size_t, PlanSegment *> id_to_segment;
    std::unordered_map<size_t, AddressInfos> id_to_address;
    std::set<AddressInfo> plan_send_addresses;
    PlanSegmentsStatusPtr plan_segment_status_ptr;
    ContextPtr query_context = nullptr;
#if defined(TASK_ASSIGN_DEBUG)
    std::unordered_map<size_t, std::vector<std::pair<size_t, AddressInfo>>> exchange_data_assign_node_mappings;
#endif
    mutable bthread::Mutex status_mutex;
    AsyncContextPtr async_context;
    std::unordered_map<size_t, UInt64> segment_paralle_size_map;
};

using DAGGraphPtr = std::shared_ptr<DAGGraph>;

class AdaptiveScheduler
{
public:
    explicit AdaptiveScheduler(const ContextPtr & context) : query_context(context), log(&Poco::Logger::get("AdaptiveScheduler"))
    {
    }
    std::vector<size_t> getRandomWorkerRank();
    std::vector<size_t> getHealthyWorkerRank();

private:
    const ContextPtr query_context;
    Poco::Logger * log;
};

} // namespace DB
