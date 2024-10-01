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
#include <Interpreters/DistributedStages/PlanSegmentInstance.h>
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
    explicit SegmentTask(size_t task_id_, bool has_table_scan_ = false) : task_id(task_id_), has_table_scan(has_table_scan_)
    {
    }
    // plan segment id.
    size_t task_id;
    bool has_table_scan;
};

/// Indicates a plan segment instance.
struct SegmentTaskInstance
{
    SegmentTaskInstance(size_t task_id_, size_t parallel_index_) : task_id(task_id_), parallel_index(parallel_index_)
    {
    }
    // plan segment id.
    size_t task_id;
    size_t parallel_index;

    inline bool operator==(SegmentTaskInstance const & rhs) const
    {
        return (this->task_id == rhs.task_id && this->parallel_index == rhs.parallel_index);
    }

    class Hash
    {
    public:
        size_t operator()(const SegmentTaskInstance & key) const
        {
            return (key.task_id << 13) + key.parallel_index;
        }
    };
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
class Scheduler
{
    using PlanSegmentTopology = std::unordered_map<size_t, std::unordered_set<size_t>>;
    using Queue = ConcurrentBoundedQueue<BatchTaskPtr>;

public:
    Scheduler(const String & query_id_, ContextPtr query_context_, std::shared_ptr<DAGGraph> dag_graph_ptr_, bool batch_schedule_ = false)
        : query_id(query_id_)
        , query_context(query_context_)
        , query_unique_id(query_context->getCurrentTransactionID().toUInt64())
        , dag_graph_ptr(dag_graph_ptr_)
        , cluster_nodes(query_context_)
        , node_selector(cluster_nodes, query_context, dag_graph_ptr)
        , local_address(getLocalAddress(*query_context))
        , batch_schedule(batch_schedule_)
        , log(&Poco::Logger::get("Scheduler"))
    {
        cluster_nodes.all_workers.emplace_back(local_address, NodeType::Local, "");
        timespec query_expiration_ts = query_context->getQueryExpirationTimeStamp();
        query_expiration_ms = query_expiration_ts.tv_sec * 1000 + query_expiration_ts.tv_nsec / 1000000;
    }

    virtual ~Scheduler() = default;
    // Pop tasks from queue and schedule them.
    void schedule();
    virtual void submitTasks(PlanSegment * plan_segment_ptr, const SegmentTask & task) = 0;
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
    size_t query_unique_id;
    std::shared_ptr<DAGGraph> dag_graph_ptr;
    std::mutex segment_bufs_mutex;
    std::unordered_map<size_t, std::shared_ptr<butil::IOBuf>> segment_bufs;
    // Generated per dag graph. The tasks whose number of dependencies will be enqueued.
    PlanSegmentTopology plansegment_topology;
    std::mutex plansegment_topology_mutex;
    // All batch task will be enqueue first. The schedule logic will pop queue and schedule the poped tasks.
    Queue queue{10000};
    ClusterNodes cluster_nodes;
    // Select nodes for tasks.
    NodeSelector node_selector;
    AddressInfo local_address;
    bool time_to_handle_finish_task = false;

    String error_msg;
    std::atomic<bool> stopped{false};

    bool batch_schedule = false;
    BatchPlanSegmentHeaders batch_segment_headers;

    Poco::Logger * log;

    void genTopology();
    virtual void genTasks() = 0;
    bool getBatchTaskToSchedule(BatchTaskPtr & task);
    virtual void sendResources(PlanSegment * plan_segment_ptr)
    {
        (void)plan_segment_ptr;
    }
    virtual void prepareTask(PlanSegment * plan_segment_ptr, NodeSelectorResult & selector_info, const SegmentTask & task)
    {
        (void)plan_segment_ptr;
        (void)selector_info;
        (void)task;
    }
    virtual PlanSegmentExecutionInfo generateExecutionInfo(size_t task_id, size_t index)
    {
        (void)task_id;
        PlanSegmentExecutionInfo execution_info;
        execution_info.parallel_id = index;
        return execution_info;
    }
    void dispatchOrSaveTask(PlanSegment * plan_segment_ptr, const SegmentTask & task, const size_t idx);
    TaskResult scheduleTask(PlanSegment * plan_segment_ptr, const SegmentTask & task);
    void batchScheduleTasks();
    void prepareFinalTask();
    NodeSelectorResult selectNodes(PlanSegment * plan_segment_ptr, const SegmentTask & task)
    {
        std::unique_lock<std::mutex> lock(node_selector_result_mutex);
        auto selector_result = node_selector_result.emplace(task.task_id, node_selector.select(plan_segment_ptr, task.has_table_scan));
        return selector_result.first->second;
    }

    std::mutex node_selector_result_mutex;
    NodeSelector::SelectorResultMap node_selector_result;

    size_t query_expiration_ms;
};

}
