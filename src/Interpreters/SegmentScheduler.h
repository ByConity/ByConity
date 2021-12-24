#pragma once

#include <Core/Types.h>
#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/CancellationCode.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Parsers/IAST_fwd.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Common/Stopwatch.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <unordered_set>

#define TASK_ASSIGN_DEBUG

namespace DB
{

struct PlanSegmentsStatus
{
    /// <segment_id, <host_name:port, status>>
    //TODO dongyifeng add when PlanSegmentInfo is merged
//    std::map<size_t, std::map<String, PlanSegmentInfoPtr>> segment_status_map;
    volatile bool is_final_stage_start = false;
    std::atomic<bool> is_cancel{false};
    String exception;
};

using PlanSegmentsStatusPtr = std::shared_ptr<PlanSegmentsStatus>;
using PlanSegmentsPtr = std::vector<PlanSegmentPtr>;
using Source = std::vector<size_t>;

struct DAGGraph {
    DAGGraph(){}
    DAGGraph(const DAGGraph & other)
    {
        std::unique_lock lock(other.status_mutex);
        sources = other.sources;
        final = other.final;
        scheduler_segments = std::move(other.scheduler_segments);
        id_to_segment = std::move(other.id_to_segment);
        id_to_address = std::move(other.id_to_address);
        plan_segment_status_ptr = std::move(other.plan_segment_status_ptr);
        local_exchange_ids = other.local_exchange_ids;
    }
    Source sources;
    size_t final = std::numeric_limits<size_t>::max();
    std::set<size_t> scheduler_segments;
    std::unordered_map<size_t, PlanSegment *> id_to_segment;
    std::unordered_map<size_t, AddressInfos> id_to_address;
    std::set<AddressInfo> plan_send_addresses;
    PlanSegmentsStatusPtr plan_segment_status_ptr;
    bool has_set_local_exchange = false;
    size_t local_exchange_parallel_size=0;
    std::set<size_t> local_exchange_ids;
    AddressInfos first_local_exchange_address;
#if defined(TASK_ASSIGN_DEBUG)
    std::unordered_map<size_t, std::vector<std::pair<size_t, AddressInfo>>> exchange_data_assign_node_mappings;
#endif
    mutable bthread::Mutex status_mutex;
};

class SegmentScheduler
{
public:
    SegmentScheduler(): log(&Poco::Logger::get("SegmentScheduler")) {}
    virtual ~SegmentScheduler() {}
    PlanSegmentsStatusPtr insertPlanSegments(const String & query_id,
                                             PlanSegmentTree * plan_segments_ptr,
                                             ContextPtr query_context);

    CancellationCode cancelPlanSegmentsFromCoordinator(const String query_id, const String & exception, ContextPtr query_context);
    CancellationCode cancelPlanSegments(const String & query_id, const String & exception, const String & origin_host_name, std::shared_ptr<DAGGraph> dag_graph_ptr = nullptr);

//    void receivePlanSegmentStatus(const String & query_id);
//    void updatePlanSegmentStatus(std::shared_ptr<DAGGraph> dag_graph_ptr, const PlanSegmentInfo & segment_status);

    bool finishPlanSegments(const String & query_id);

//    void logPlanSegmentStatus(Context & context, const String & query_id);

    AddressInfos getWorkerAddress(const String & query_id, size_t segment_id);

    String getCurrentDispatchStatus(const String & query_id);

private:
    std::unordered_map<String, std::shared_ptr<DAGGraph>> query_map;
    mutable bthread::Mutex mutex;
    Poco::Logger * log;

    void buildDAGGraph(PlanSegmentTree * plan_segments_ptr, std::shared_ptr<DAGGraph> graph);
    bool scheduler(const String & query_id, ContextPtr query_context, std::shared_ptr<DAGGraph> dag_graph);

protected:
    virtual AddressInfos sendPlanSegment(PlanSegment * plan_segment_ptr, bool is_source, ContextPtr query_context, std::shared_ptr<DAGGraph> dag_graph);
};

using SegmentSchedulerPtr = std::shared_ptr<SegmentScheduler>;

}
