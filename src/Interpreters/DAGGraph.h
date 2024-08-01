#pragma once

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/DistributedStages/executePlanSegment.h>
#include <Interpreters/WorkerGroupHandle.h>
#include <bthread/mutex.h>
#include "Interpreters/DistributedStages/PlanSegmentInstance.h"

#ifndef NDEBUG
#define TASK_ASSIGN_DEBUG
#endif

namespace DB
{
class CnchServerResource;

struct PlanSegmentsStatus
{
    //TODO dongyifeng add when PlanSegmentInfo is merged
    std::atomic<bool> is_final_stage_start{false};
    std::atomic<bool> is_cancel{false};
    Int32 error_code;
    String exception;
};

using PlanSegmentsStatusPtr = std::shared_ptr<PlanSegmentsStatus>;
using SegmentIds = std::unordered_set<size_t>;
using WorkerInfoSet = std::unordered_set<HostWithPorts, std::hash<HostWithPorts>, HostWithPorts::IsSameEndpoint>;
using PlanSegmentId = size_t;
using StorageUnions = std::vector<std::unordered_set<UUID>>;
using StorageUnionsPtr = std::shared_ptr<StorageUnions>;
struct SourcePruner
{
    SourcePruner(PlanSegmentTree * plan_segments_ptr_, Poco::Logger * log_)
        : plan_segments_ptr(plan_segments_ptr_), log(log_)
    {
    }
    
    void pruneSource(
        CnchServerResource * server_resource,
        std::unordered_map<size_t, PlanSegment *> & id_to_segment);

    void prepare();
    
    std::unordered_set<PlanSegmentId> unprunable_plan_segments;
    std::unordered_map<PlanSegmentId, std::unordered_set<UUID>> plan_segment_storages_map;
    std::unordered_map<PlanSegmentId, WorkerInfoSet> plan_segment_workers_map;

private:
    void generateUnprunableSegments();
    void generateSegmentStorageMap();
    PlanSegmentTree * plan_segments_ptr;
    Poco::Logger * log;
};

using SourcePrunerPtr = std::shared_ptr<SourcePruner>;

struct DAGGraph
{
    DAGGraph() : log(&Poco::Logger::get("DAGGraph")) { async_context = std::make_shared<AsyncContext>(); }
    void joinAsyncRpcWithThrow();
    void joinAsyncRpcPerStage();
    void joinAsyncRpcAtLast();
    AddressInfos getAddressInfos(size_t segment_id);

    PlanSegment * getPlanSegmentPtr(size_t id);

    SourcePrunerPtr makeSourcePruner(PlanSegmentTree * plan_segments_ptr)
    {
        source_pruner = std::make_shared<SourcePruner>(plan_segments_ptr, log);
        return source_pruner;
    }
    
    SegmentIds leaf_segments;
    /// all segments contain at least table scan
    SegmentIds segments_has_table_scan;
    size_t final = std::numeric_limits<size_t>::max();
    std::set<size_t> scheduled_segments;
    std::unordered_map<size_t, PlanSegment *> id_to_segment;
    std::unordered_map<size_t, AddressInfos> id_to_address;
    /// final worker address where task is successfully executed
    mutable std::mutex finished_address_mutex;
    std::unordered_map<size_t, std::map<size_t, AddressInfo>> finished_address;
    std::set<AddressInfo> plan_send_addresses;
    PlanSegmentsStatusPtr plan_segment_status_ptr;
    ContextPtr query_context = nullptr;
#if defined(TASK_ASSIGN_DEBUG)
    std::unordered_map<size_t, std::vector<std::pair<size_t, AddressInfo>>> exchange_data_assign_node_mappings;
#endif
    mutable bthread::Mutex status_mutex;
    AsyncContextPtr async_context;
    std::unordered_map<size_t, UInt64> segment_parallel_size_map;
    butil::IOBuf query_common_buf;
    butil::IOBuf query_settings_buf;
    SourcePrunerPtr source_pruner;

    Poco::Logger * log;
};

using DAGGraphPtr = std::shared_ptr<DAGGraph>;

class AdaptiveScheduler
{
public:
    explicit AdaptiveScheduler(const ContextPtr & context) : query_context(context), log(&Poco::Logger::get("AdaptiveScheduler")) { }
    std::vector<size_t> getRandomWorkerRank();
    std::vector<size_t> getHealthyWorkerRank();

private:
    const ContextPtr query_context;
    Poco::Logger * log;
};

} // namespace DB
