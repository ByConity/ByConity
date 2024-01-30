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
    explicit SegmentTask(size_t task_id_, bool is_source_ = false) : task_id(task_id_), is_source(is_source_)
    {
    }
    // plan segment id.
    size_t task_id;
    bool is_source;
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
        , local_address(getLocalAddress(*query_context))
        , log(&Poco::Logger::get("Scheduler"))
    {
        cluster_nodes.rank_workers.emplace_back(local_address, NodeType::Local, "");
    }
    virtual ~IScheduler() = default;
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
    Poco::Logger * log;
    bool time_to_handle_finish_task = false;

    String error_msg;
    std::atomic<bool> stopped{false};

    void genTopology();
    void genSourceTasks();
    bool getBatchTaskToSchedule(BatchTaskPtr & task);
    void sendResource(PlanSegment * plan_segment_ptr);
    void dispatchTask(PlanSegment * plan_segment_ptr, const SegmentTask & task, const size_t idx);
    TaskResult scheduleTask(PlanSegment * plan_segment_ptr, const SegmentTask & task);
    void prepareFinalTask();

    NodeSelector::SelectorResultMap node_selector_result;
};

// Once the dependencies scheduled, the segment would be scheduled.
//
// IScheduler::genTopology -> IScheduler::scheduleTask -> MPPScheduler::submitTasks -> IScheduler::dispatchTask -rpc-> Worker node
class MPPScheduler : public IScheduler
{
public:
    MPPScheduler(const String & query_id_, ContextPtr query_context_, std::shared_ptr<DAGGraph> dag_graph_ptr_)
        : IScheduler(query_id_, query_context_, dag_graph_ptr_)
    {
    }

private:
    void submitTasks(PlanSegment * plan_segment_ptr, const SegmentTask & task) override;
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
//
// IScheduler::genTopology -> IScheduler::scheduleTask -> BSPScheduler::submitTasks
//                                                                  |
//                                                        IScheduler::dispatchTask -rpc-> Worker node
//                                                                  |                       | rpc
//                                                                  <--------- BSPScheduler::onSegmentFinished
class BSPScheduler : public IScheduler
{
    struct PendingTaskIntances
    {
        // nodes -> [segment instance who prefer to be scheduled to it]
        std::unordered_map<AddressInfo, std::unordered_set<SegmentTaskInstance, SegmentTaskInstance::Hash>, AddressInfo::Hash> for_nodes;
        // segment isntances who have no preference.
        std::unordered_set<SegmentTaskInstance, SegmentTaskInstance::Hash> no_prefs;
    };

public:
    BSPScheduler(const String & query_id_, ContextPtr query_context_, std::shared_ptr<DAGGraph> dag_graph_ptr_)
        : IScheduler(query_id_, query_context_, dag_graph_ptr_)
    {
    }

    void submitTasks(PlanSegment * plan_segment_ptr, const SegmentTask & task) override;
    // We do nothing on task scheduled.
    void onSegmentScheduled(const SegmentTask & task) override
    {
        (void)task;
    }
    void onSegmentFinished(const size_t & segment_id, bool is_succeed, bool is_canceled) override;
    void onQueryFinished() override;

    void updateSegmentStatusCounter(const size_t & segment_id, const UInt64 & parallel_index);

private:
    std::pair<bool, SegmentTaskInstance> getInstanceToSchedule(const AddressInfo & worker);
    void triggerDispatch(const std::vector<WorkerNode> & available_workers);

    std::mutex segment_status_counter_mutex;
    std::unordered_map<size_t, std::unordered_set<UInt64>> segment_status_counter;

    std::mutex nodes_alloc_mutex;
    // segment id -> nodes running its instance
    std::unordered_map<size_t, std::unordered_set<AddressInfo, AddressInfo::Hash>> occupied_workers;
    // segment id -> [segment instance, node]
    std::unordered_map<size_t, std::unordered_map<UInt64, AddressInfo>> segment_parallel_locations;
    PendingTaskIntances pending_task_instances;
};
}
