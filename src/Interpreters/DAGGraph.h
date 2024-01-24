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
    butil::IOBuf query_common_buf;
    butil::IOBuf query_settings_buf;
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
