#include <atomic>
#include <cstddef>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <Interpreters/DistributedStages/PlanSegmentInstance.h>
#include <Interpreters/DistributedStages/RuntimeSegmentsStatus.h>
#include <Interpreters/DistributedStages/Scheduler.h>
#include <Interpreters/DistributedStages/SourceTask.h>
#include <Common/HostWithPorts.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED;
}

// Smaller value means dirtier.
enum TaintLevel
{
    FailedOrRunning = 0,
    Running = 1,
    Last
};

// Only if the dependencies were executed done, the segment would be scheduled.
//
// Scheduler::genTopology -> Scheduler::scheduleTask -> BSPScheduler::submitTasks
//                                                                  |
//                                                        Scheduler::dispatchOrSaveTask -rpc-> Worker node
//                                                                  |                       | rpc
//                                                                  <--------- BSPScheduler::onSegmentFinished
class BSPScheduler : public Scheduler
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
        : Scheduler(query_id_, query_context_, dag_graph_ptr_)
    {
        ResourceOption option;
        if (!option.table_ids.empty())
        {
            query_context->getCnchServerResource()->setSendMutations(true);
            query_context->getCnchServerResource()->sendResources(query_context, option);
        }
    }

    // We do nothing on task scheduled.
    void onSegmentScheduled(const SegmentTask & task) override
    {
        (void)task;
    }
    void onSegmentFinished(const size_t & segment_id, bool is_succeed, bool is_canceled) override;
    void onQueryFinished() override;

    void updateSegmentStatusCounter(size_t segment_id, UInt64 parallel_index, const RuntimeSegmentStatus & status);
    /// retry task if possible, returns whether retry is successful or not
    bool retryTaskIfPossible(size_t segment_id, UInt64 parallel_index, const RuntimeSegmentStatus & status);
    void onWorkerRestarted(const WorkerId & id, const HostWithPorts & host_ports);

    const AddressInfo & getSegmentParallelLocation(PlanSegmentInstanceId instance_id);

    std::pair<String, String> tryGetWorkerGroupName()
    {
        if (auto wg = query_context->tryGetCurrentWorkerGroup(); wg)
        {
            return std::make_pair(wg->getVWName(), wg->getID());
        }
        return std::make_pair("", "");
    }

private:
    std::pair<bool, SegmentTaskInstance> getInstanceToSchedule(const AddressInfo & worker);
    void triggerDispatch(const std::vector<WorkerNode> & available_workers);
    void sendResources(PlanSegment * plan_segment_ptr) override;
    void prepareTask(PlanSegment * plan_segment_ptr, NodeSelectorResult & selector_info, const SegmentTask & task) override;
    void genTasks() override;
    void genLeafTasks();
    PlanSegmentExecutionInfo generateExecutionInfo(size_t task_id, size_t index) override;
    void submitTasks(PlanSegment * plan_segment_ptr, const SegmentTask & task) override;

    bool isUnrecoverableStatus(const RuntimeSegmentStatus & status);
    bool isOutdated(const RuntimeSegmentStatus & status);
    bool isTaintNode(size_t task_id, const AddressInfo & worker, TaintLevel & taint_level);

    std::mutex segment_status_counter_mutex;
    std::unordered_map<size_t, std::unordered_set<UInt64>> segment_status_counter;

    std::mutex nodes_alloc_mutex;
    // segment id -> nodes running its instance
    std::unordered_map<size_t, std::unordered_set<AddressInfo, AddressInfo::Hash>> running_segment_to_workers;
    std::unordered_map<AddressInfo, std::unordered_set<PlanSegmentInstanceId>, AddressInfo::Hash> worker_to_running_instances;
    // segment id -> nodes failed its instance
    std::unordered_map<size_t, std::unordered_set<AddressInfo, AddressInfo::Hash>> failed_segment_to_workers;
    // segment id -> [segment instance, node]
    std::unordered_map<size_t, std::unordered_map<UInt64, AddressInfo>> segment_parallel_locations;
    std::unordered_map<PlanSegmentInstanceId, size_t> segment_instance_retry_cnt;

    PendingTaskIntances pending_task_instances;
    // segment task instance -> <index, total> count in this worker
    std::unordered_map<SegmentTaskInstance, std::pair<size_t, size_t>, SegmentTaskInstance::Hash> source_task_idx;
    std::unordered_map<SegmentTaskInstance, std::set<Int64>, SegmentTaskInstance::Hash> source_task_buckets;

    /// Error reasons which can not be recovered by retry. We need quit right now.
    std::unordered_set<int> unrecoverable_reasons{ErrorCodes::LOGICAL_ERROR, ErrorCodes::QUERY_WAS_CANCELLED};
};

}
