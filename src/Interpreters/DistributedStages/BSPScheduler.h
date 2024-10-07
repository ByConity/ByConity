#include <atomic>
#include <cstddef>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <Interpreters/DistributedStages/PlanSegmentInstance.h>
#include <Interpreters/DistributedStages/RuntimeSegmentsStatus.h>
#include <Interpreters/DistributedStages/ScheduleEvent.h>
#include <Interpreters/DistributedStages/Scheduler.h>
#include <Interpreters/DistributedStages/SourceTask.h>
#include "common/types.h"
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

enum SegmentInstanceStatusUpdateResult
{
    UpdateSuccess = 0,
    UpdateFailed = 1,
    RetrySuccess = 2,
    RetryFailed = 3
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
    using EventQueue = ConcurrentBoundedQueue<std::shared_ptr<ScheduleEvent>>;

    struct PendingTaskIntances
    {
        // nodes -> [segment instance who prefer to be scheduled to it]
        std::unordered_map<AddressInfo, std::unordered_set<SegmentTaskInstance, SegmentTaskInstance::Hash>, AddressInfo::Hash> for_nodes;
        // segment isntances who have no preference.
        std::unordered_set<SegmentTaskInstance, SegmentTaskInstance::Hash> no_prefs;
    };

    struct RunningInstances
    {
        explicit RunningInstances(std::shared_ptr<WorkerStatusManager> worker_status_manager_)
            : worker_status_manager(worker_status_manager_)
        {
        }
        void insert(const AddressInfo & address, const WorkerId & worker_id, const PlanSegmentInstanceId & instance_id)
        {
            worker_to_running_instances[address].insert(instance_id);
            std::optional<WorkerStatusExtra> status = worker_status_manager->getWorkerStatus(worker_id);
            running_instance_epoch[instance_id] = status.has_value() ? status->worker_status->register_time : 0;
        }
        void erase(const AddressInfo & address, const PlanSegmentInstanceId & instance_id)
        {
            worker_to_running_instances[address].erase(instance_id);
            running_instance_epoch.erase(instance_id);
        }
        std::unordered_set<PlanSegmentInstanceId> getInstances(const AddressInfo & address, UInt32 register_time)
        {
            std::unordered_set<PlanSegmentInstanceId> ret;
            for (const auto & id : worker_to_running_instances[address])
            {
                if (running_instance_epoch[id] == register_time)
                    ret.insert(id);
            }
            return ret;
        }
        UInt32 getEpoch(const PlanSegmentInstanceId & instance_id)
        {
            if (const auto & iter = running_instance_epoch.find(instance_id); iter != running_instance_epoch.end())
            {
                return iter->second;
            }
            return static_cast<UInt32>(0);
        }
        // nodes -> instances running on it, used to retry them when worker restarted.
        std::unordered_map<AddressInfo, std::unordered_set<PlanSegmentInstanceId>, AddressInfo::Hash> worker_to_running_instances;
        std::unordered_map<PlanSegmentInstanceId, UInt32> running_instance_epoch;
        std::shared_ptr<WorkerStatusManager> worker_status_manager;
    };

public:
    BSPScheduler(const String & query_id_, ContextPtr query_context_, std::shared_ptr<DAGGraph> dag_graph_ptr_)
        : Scheduler(query_id_, query_context_, dag_graph_ptr_), running_instances(query_context_->getWorkerStatusManager())
    {
        ResourceOption option;
        if (!option.table_ids.empty())
        {
            query_context->getCnchServerResource()->setSendMutations(true);
            query_context->getCnchServerResource()->sendResources(query_context, option);
        }
    }

    PlanSegmentExecutionInfo schedule() override;

    // We do nothing on task scheduled.
    void onSegmentScheduled(const SegmentTask & task) override
    {
        (void)task;
    }
    void onSegmentFinished(const size_t & segment_id, bool is_succeed, bool is_canceled) override;
    void onQueryFinished() override;

    void onEventDispatchTrigger(std::shared_ptr<ScheduleEvent> event);
    void onWorkerRestarted(const WorkerId & id, UInt32 register_time);
    SegmentInstanceStatusUpdateResult
    segmentInstanceFinished(size_t segment_id, UInt64 parallel_index, const RuntimeSegmentStatus & status);
    void workerRestarted(const WorkerId & id, const HostWithPorts & host_ports, UInt32 register_time);

    std::pair<String, String> tryGetWorkerGroupName()
    {
        if (auto wg = query_context->tryGetCurrentWorkerGroup(); wg)
        {
            return std::make_pair(wg->getVWName(), wg->getID());
        }
        return std::make_pair("", "");
    }

protected:
    bool addBatchTask(BatchTaskPtr batch_task) override;

private:
    bool postEvent(std::shared_ptr<ScheduleEvent> event);
    bool getEventToProcess(std::shared_ptr<ScheduleEvent> & event);

    std::pair<bool, SegmentTaskInstance> getInstanceToSchedule(const AddressInfo & worker);
    void triggerDispatch(const std::vector<WorkerNode> & available_workers);
    void sendResources(PlanSegment * plan_segment_ptr) override;
    void prepareTask(PlanSegment * plan_segment_ptr, NodeSelectorResult & selector_info, const SegmentTask & task) override;
    void genLeafTasks();
    PlanSegmentExecutionInfo generateExecutionInfo(size_t task_id, size_t index) override;
    void submitTasks(PlanSegment * plan_segment_ptr, const SegmentTask & task) override;
    void prepareFinalTaskImpl(PlanSegment * final_plan_segment, const AddressInfo & addr) override;

    bool isUnrecoverableStatus(const RuntimeSegmentStatus & status);
    bool isOutdated(const RuntimeSegmentStatus & status);
    bool isTaintNode(size_t task_id, const AddressInfo & worker, TaintLevel & taint_level);

    SegmentInstanceStatusUpdateResult
    onSegmentInstanceFinished(size_t segment_id, UInt64 parallel_index, const RuntimeSegmentStatus & status);
    /// retry task if possible, returns whether retry is successful or not
    bool retryTaskIfPossible(size_t segment_id, UInt64 parallel_index, const RuntimeSegmentStatus & status);
    void resendResource(const HostWithPorts & host_ports);

    // All batch task will be enqueue first. The schedule logic will pop queue and schedule the poped tasks.
    EventQueue queue{10000};
    // Special queue for resend event, which is of highest priority.
    EventQueue resend_queue{10000};

    std::mutex finish_segment_instance_mutex;
    std::condition_variable finish_segment_instance_cv;
    std::unordered_map<PlanSegmentInstanceAttempt, SegmentInstanceStatusUpdateResult, PlanSegmentInstanceAttempt::Hash>
        segment_status_update_results;

    std::mutex segment_status_counter_mutex;
    std::unordered_map<size_t, std::unordered_set<UInt64>> segment_status_counter;

    std::mutex nodes_alloc_mutex;
    // segment id -> nodes running its instance, to avoid instances of same segment to be running on one same worker.
    std::unordered_map<size_t, std::unordered_set<AddressInfo, AddressInfo::Hash>> running_segment_to_workers;
    // nodes -> instances running on it, used to retry them when worker restarted.
    RunningInstances running_instances;
    // segment id -> nodes failed its instance, used to check if the node was tainted.
    std::unordered_map<size_t, std::unordered_set<AddressInfo, AddressInfo::Hash>> failed_segment_to_workers;
    // segment id -> [segment instance, node]
    std::unordered_map<size_t, std::unordered_map<UInt64, WorkerNode>> segment_parallel_locations;
    std::unordered_map<PlanSegmentInstanceId, size_t> segment_instance_retry_cnt;

    PendingTaskIntances pending_task_instances;
    // segment task instance -> <index, total> count in this worker
    std::unordered_map<SegmentTaskInstance, std::pair<size_t, size_t>, SegmentTaskInstance::Hash> source_task_idx;
    std::unordered_map<SegmentTaskInstance, std::set<Int64>, SegmentTaskInstance::Hash> source_task_buckets;

    /// Error reasons which can not be recovered by retry. We need quit right now.
    std::unordered_set<int> unrecoverable_reasons{ErrorCodes::LOGICAL_ERROR, ErrorCodes::QUERY_WAS_CANCELLED};
};
}
