#pragma once

#include <condition_variable>
#include <memory>
#include <optional>
#include <set>
#include <unordered_map>
#include <time.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/DAGGraph.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/NodeSelector.h>
#include <Parsers/queryToString.h>
#include <butil/endpoint.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/Macros.h>
#include <Common/ProfileEvents.h>
#include <Common/thread_local_rng.h>

namespace DB
{

enum class TaskStatus : uint8_t
{
    Unknown = 1,
    Success = 2,
    Fail = 3,
    Wait = 4
};

/// Indicates a plan segment.
struct SegmentTask
{
    SegmentTask(size_t task_id_, bool is_source_ = false) : task_id(task_id_), is_source(is_source_) { }
    // plan segment id.
    size_t task_id;
    bool is_source;
};

using SegmentTaskPtr = std::shared_ptr<SegmentTask>;

// Tasks would be scheduled in same round.
using BatchTask = std::vector<SegmentTask>;
using BatchTaskPtr = std::shared_ptr<BatchTask>;

using BatchTasks = std::vector<BatchTaskPtr>;
using BatchTasksPtr = std::shared_ptr<BatchTasks>;
struct TaskResult
{
    TaskStatus status;
};

using BatchResult = std::vector<TaskResult>;
struct ScheduleResult
{
    BatchResult result;
};

/*
 * Scheduler
 * 1. Generates topology for given dag.
 * 2. Generates tasks per topology.
 * 3. Dispatches tasks to clients. (after sending resources for them)
 * 4. Receives task result and moving forward.
 *
 * Normally it will first schedule source tasks, then intermidiate(compute) ones, and final task at last.
*/
class IScheduler
{
    using PlanSegmentTopology = std::unordered_map<size_t, std::unordered_set<size_t>>;
    using Queue = ConcurrentBoundedQueue<BatchTaskPtr>;

public:
    IScheduler(const String & query_id_, ContextPtr query_context_, std::shared_ptr<DAGGraph> dag_graph_ptr_)
        : query_id(query_id_)
        , query_context(query_context_)
        , dag_graph_ptr(dag_graph_ptr_)
        , cluster_nodes(query_context_)
        , node_selector(cluster_nodes, query_context, dag_graph_ptr)
        , log(&Poco::Logger::get("Scheduler"))
    {
    }
    virtual ~IScheduler() = default;
    // Pop tasks from queue and schedule them.
    void schedule();
    /// TODO(WangTao): add staus for result
    virtual void onSegmentScheduled(const SegmentTask & task) = 0;
    virtual void onSegmentFinished(const size_t & segment_id, bool is_succeed, bool is_canceled) = 0;
    virtual void onQueryFinished()
    {
    }

protected:
    // Remove dependencies for all tasks who depends on given `task`, and enqueue those whose dependencies become emtpy.
    void removeDepsAndEnqueueTask(const SegmentTask & task);
    bool addBatchTask(BatchTaskPtr batch_task);

    const String query_id;
    ContextPtr query_context;
    std::shared_ptr<DAGGraph> dag_graph_ptr;
    // Generated per dag graph. The tasks whose number of dependencies will be enqueued.
    PlanSegmentTopology plansegment_topology;
    std::mutex plansegment_topology_mutex;
    // All batch task will be enqueue first. The schedule logic will pop queue and schedule the poped tasks.
    Queue queue{10000};
    ClusterNodes cluster_nodes;
    // Select nodes for tasks.
    NodeSelector node_selector;
    Poco::Logger * log;
    bool time_to_handle_finish_task = false;

    std::mutex nodes_alloc_mutex;
    std::condition_variable nodes_alloc_cv;
    bool has_available_worker = true;
    std::unordered_map<size_t, std::unordered_set<AddressInfo, AddressInfo::Hash>> busy_nodes;
    std::unordered_map<size_t, std::unordered_map<UInt64, AddressInfo>> segment_parallel_nodes;

    String error_msg;
    bool stopped = false;

    void genTopology();
    void genSourceTasks();
    bool getBatchTaskToSchedule(BatchTaskPtr & task);
    void sendResource(SegmentTask task, PlanSegment * plan_segment_ptr);
    TaskResult scheduleTask(const SegmentTask & task, PlanSegment * plan_segment_ptr);
    void prepareFinalTask();

    NodeSelector::SelectorResultMap node_selector_result;
};

// Once the dependencies scheduled, the segment would be scheduled.
class MPPScheduler : public IScheduler
{
public:
    MPPScheduler(const String & query_id_, ContextPtr query_context_, std::shared_ptr<DAGGraph> dag_graph_ptr_)
        : IScheduler(query_id_, query_context_, dag_graph_ptr_)
    {
    }

private:
    void onSegmentScheduled(const SegmentTask & task) override;
    // We do nothing on task finished.
    void onSegmentFinished(const size_t & segment_id, bool is_succeed, bool is_canceled) override
    {
        (void)segment_id;
        (void)is_succeed;
        (void)is_canceled;
    }
};

using MPPSchedulerPtr = std::unique_ptr<MPPScheduler>;

// Only if the dependencies were executed done, the segment would be scheduled.
class BSPScheduler : public IScheduler
{
public:
    BSPScheduler(const String & query_id_, ContextPtr query_context_, std::shared_ptr<DAGGraph> dag_graph_ptr_)
        : IScheduler(query_id_, query_context_, dag_graph_ptr_)
    {
    }

    // We do nothing on task scheduled.
    void onSegmentScheduled(const SegmentTask & task) override
    {
        (void)task;
    }
    void onSegmentFinished(const size_t & segment_id, bool is_succeed, bool is_canceled) override;
    void onQueryFinished() override;

    void updateSegmentStatusCounter(const size_t & segment_id, const UInt64 & parallel_index);

private:
    std::mutex segment_status_counter_mutex;
    std::unordered_map<size_t, std::unordered_set<UInt64>> segment_status_counter;
};
}
