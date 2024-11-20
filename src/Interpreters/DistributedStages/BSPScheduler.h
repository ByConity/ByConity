#pragma once

#include <atomic>
#include <cstddef>
#include <list>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <Catalog/Catalog.h>
#include <Interpreters/DistributedStages/ExchangeDataTracker.h>
#include <Interpreters/DistributedStages/PlanSegmentInstance.h>
#include <Interpreters/DistributedStages/ResourceRequest.h>
#include <Interpreters/DistributedStages/RuntimeSegmentsStatus.h>
#include <Interpreters/DistributedStages/ScheduleEvent.h>
#include <Interpreters/DistributedStages/Scheduler.h>
#include <Interpreters/DistributedStages/SourceTask.h>
#include <Interpreters/NodeSelector.h>
#include <Interpreters/WorkerStatusManager.h>
#include <Protos/resource_manager_rpc.pb.h>
#include <Common/HostWithPorts.h>
#include <common/types.h>

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

    // ATTENTION: NOT thread safe. Make it used in main event processing.
    struct PendingResourceRequests
    {
        std::unordered_map<SegmentTaskInstance, ResourceRequest, SegmentTaskInstance::Hash> pending_requests;
        std::unordered_map<SegmentTaskInstance, UInt32, SegmentTaskInstance::Hash> request_time;

        void insert(const SegmentTaskInstance & instance, const ResourceRequest & req)
        {
            pending_requests[instance] = req;
            request_time[instance] = time(nullptr);
        }
        void erase(const SegmentTaskInstance & instance)
        {
            pending_requests.erase(instance);
            request_time.erase(instance);
        }
        bool illegal(const SegmentTaskInstance & instance, UInt32 epoch)
        {
            return pending_requests.contains(instance) && pending_requests[instance].epoch == epoch;
        }
        std::list<ResourceRequest> getOutdatedRequests()
        {
            std::list<ResourceRequest> ret;
            UInt32 now = time(nullptr);
            for (auto iter = request_time.begin(); iter != request_time.end();)
            {
                if (now - iter->second > timeout_ms)
                {
                    pending_requests[iter->first].epoch += 1;
                    ret.push_back(std::move(pending_requests[iter->first]));
                    iter = request_time.erase(iter);
                }
                else
                    iter++;
            }
            return ret;
        }

        UInt32 timeout_ms; // todo (wangtao.vip) init this
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
            if (worker_status_manager)
            {
                std::optional<WorkerStatus> status = worker_status_manager->getWorkerStatus(worker_id);
                running_instance_epoch[instance_id] = status.has_value() ? status->resource_status.register_time : 0;
            }
            else
            {
                running_instance_epoch[instance_id] = 0;
            }
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
        : BSPScheduler(
            query_id_,
            query_context_->getCurrentTransactionID(),
            query_context_,
            ClusterNodes(query_context_),
            dag_graph_ptr_,
            query_context_->getCnchCatalog(),
            query_context_->getWorkerStatusManager())
    {
    }
    BSPScheduler(
        const String & query_id_,
        size_t query_unique_id_,
        ContextPtr query_context_,
        ClusterNodes cluster_nodes_,
        std::shared_ptr<DAGGraph> dag_graph_ptr_,
        std::shared_ptr<Catalog::Catalog> catalog_,
        std::shared_ptr<WorkerStatusManager> worker_status_manager)
        : Scheduler(query_id_, query_context_, std::move(cluster_nodes_), dag_graph_ptr_)
        , running_instances(std::make_shared<RunningInstances>(worker_status_manager))
        , query_unique_id(query_unique_id_)
        , catalog(catalog_)
    {
        ResourceOption option;

        if (!option.table_ids.empty())
        {
            query_context->getCnchServerResource()->setSendMutations(true);
            query_context->getCnchServerResource()->sendResources(this->query_context, option);
        }
    }
    ~BSPScheduler() override;

    PlanSegmentExecutionInfo schedule() override;

    // We do nothing on task scheduled.
    void onSegmentScheduled(const SegmentTask & task) override
    {
        (void)task;
    }
    void onSegmentFinished(const size_t & segment_id, bool is_succeed, bool is_canceled) override;
    void onQueryFinished() override;

    SegmentInstanceStatusUpdateResult
    segmentInstanceFinished(size_t segment_id, UInt64 parallel_index, const RuntimeSegmentStatus & status);
    void workerRestarted(const WorkerId & id, const HostWithPorts & host_ports, UInt32 register_time);
    void resourceRequestGranted(const UInt32 segment_id, const UInt32 parallel_index, const UInt32 epoch, bool ok);
    bool getEventToProcess(std::shared_ptr<ScheduleEvent> & event);
    bool processEvent(const ScheduleEvent & event);
    bool hasEvent() const;
    const ClusterNodes & getClusterNodes() const
    {
        return cluster_nodes;
    }

    size_t getAttemptId(const PlanSegmentInstanceId & instance_id) const
    {
        return segment_instance_attempts[instance_id];
    }
    const std::unordered_map<size_t, std::unordered_set<AddressInfo, AddressInfo::Hash>> & getFailedSegmentToWorkers()
    {
        return failed_segment_to_workers;
    }

protected:
    bool addBatchTask(BatchTaskPtr batch_task) override;

private:

    bool postEvent(std::shared_ptr<ScheduleEvent> event);
    bool postHighPriorityEvent(std::shared_ptr<ScheduleEvent> event);
    std::pair<bool, SegmentTaskInstance> getInstanceToSchedule(const AddressInfo & worker);
    void triggerDispatch(const std::vector<WorkerNode> & available_workers);
    void sendResources(PlanSegment * plan_segment_ptr) override;
    void prepareTask(PlanSegment * plan_segment_ptr, NodeSelectorResult & selector_info, const SegmentTask & task) override;
    void genLeafTasks();
    PlanSegmentExecutionInfo generateExecutionInfo(size_t task_id, size_t index) override;
    void submitTasks(PlanSegment * plan_segment_ptr, const SegmentTask & task) override;
    void prepareFinalTaskImpl(PlanSegment * final_plan_segment, const AddressInfo & addr) override;
    void sendResourceRequest(const SegmentTaskInstance & instance, const WorkerId & worker_id);

    bool isUnrecoverableStatus(const RuntimeSegmentStatus & status);
    bool isOutdated(const RuntimeSegmentStatus & status);
    bool isTaintNode(size_t task_id, const AddressInfo & worker, TaintLevel & taint_level);

    void handleScheduleBatchTaskEvent(const ScheduleEvent& event);
    void handleTriggerDispatchEvent(const ScheduleEvent& event);
    void handleWorkerRestartedEvent(const ScheduleEvent& event);
    void handleSegmentInstanceFinishedEvent(const ScheduleEvent& event);
    void handleSendResourceRequestEvent(const ScheduleEvent & event);
    void handleResourceRequestGrantedEvent(const ScheduleEvent & event);

    SegmentInstanceStatusUpdateResult
    onSegmentInstanceFinished(size_t segment_id, UInt64 parallel_index, const RuntimeSegmentStatus & status);
    /// retry task if possible, returns whether retry is successful or not
    bool retryTaskIfPossible(size_t segment_id, UInt64 parallel_index, const RuntimeSegmentStatus & status);
    void resendResource(const HostWithPorts & host_ports);

    Protos::SendResourceRequestReq fillResourceRequestToProto(const ResourceRequest & req);

    // All batch task will be enqueue first. The schedule logic will pop queue and schedule the poped tasks.
    EventQueue queue{10000};
    // Special queue for events of highest priority, like abort/resend resource.
    EventQueue high_priority_queue{10000};

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
    std::shared_ptr<RunningInstances> running_instances;
    // segment id -> nodes failed its instance, used to check if the node was tainted.
    std::unordered_map<size_t, std::unordered_set<AddressInfo, AddressInfo::Hash>> failed_segment_to_workers;
    // segment id -> [segment instance, node]
    std::unordered_map<size_t, std::unordered_map<UInt64, std::optional<WorkerNode>>> segment_parallel_locations;
    mutable std::unordered_map<PlanSegmentInstanceId, size_t> segment_instance_attempts;

    PendingTaskIntances pending_task_instances;
    // segment task instance -> <index, total> count in this worker
    std::unordered_map<SegmentTaskInstance, std::pair<size_t, size_t>, SegmentTaskInstance::Hash> source_task_idx;
    std::unordered_map<SegmentTaskInstance, std::set<Int64>, SegmentTaskInstance::Hash> source_task_buckets;

    // ATTENTION: NOT thread safe. Make it used in main event processing.
    PendingResourceRequests pending_resource_requests;

    /// Error reasons which can not be recovered by retry. We need quit right now.
    static std::unordered_set<int> unrecoverable_reasons;
    size_t query_unique_id = {};
    std::shared_ptr<Catalog::Catalog> catalog;
};
}
