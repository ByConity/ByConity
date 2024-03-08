#include <cstddef>
#include <unordered_map>
#include <atomic>
#include <Interpreters/DistributedStages/RuntimeSegmentsStatus.h>
#include <Interpreters/DistributedStages/Scheduler.h>
#include "Interpreters/DistributedStages/PlanSegmentInstance.h"

namespace DB
{

// Only if the dependencies were executed done, the segment would be scheduled.
//
// Scheduler::genTopology -> Scheduler::scheduleTask -> BSPScheduler::submitTasks
//                                                                  |
//                                                        Scheduler::dispatchTask -rpc-> Worker node
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
    }

    void submitTasks(PlanSegment * plan_segment_ptr, const SegmentTask & task) override;
    // We do nothing on task scheduled.
    void onSegmentScheduled(const SegmentTask & task) override
    {
        (void)task;
    }
    void onSegmentFinished(const size_t & segment_id, bool is_succeed, bool is_canceled) override;
    void onQueryFinished() override;

    void updateSegmentStatusCounter(size_t segment_id, UInt64 parallel_index, const RuntimeSegmentsStatus & status);
    /// retry task if possible, returns whether retry is successful or not
    bool retryTaskIfPossible(size_t segment_id, UInt64 parallel_index);

private:
    std::pair<bool, SegmentTaskInstance> getInstanceToSchedule(const AddressInfo & worker);
    void triggerDispatch(const std::vector<WorkerNode> & available_workers);
    void sendResources(PlanSegment * plan_segment_ptr) override;
    void prepareTask(PlanSegment * plan_segment_ptr, size_t parallel_size) override;
    PlanSegmentExecutionInfo generateExecutionInfo(size_t task_id, size_t index) override;

    std::mutex segment_status_counter_mutex;
    std::unordered_map<size_t, std::unordered_set<UInt64>> segment_status_counter;

    std::mutex nodes_alloc_mutex;
    // segment id -> nodes running/runned(failed) its instance
    std::unordered_map<size_t, std::unordered_set<AddressInfo, AddressInfo::Hash>> occupied_workers;
    // segment id -> nodes failed its instance
    std::unordered_map<size_t, std::unordered_set<AddressInfo, AddressInfo::Hash>> failed_workers;
    // segment id -> [segment instance, node]
    std::unordered_map<size_t, std::unordered_map<UInt64, AddressInfo>> segment_parallel_locations;
    PendingTaskIntances pending_task_instances;
    // segment task instance -> <index, total> count in this worker
    std::unordered_map<SegmentTaskInstance, std::pair<size_t, size_t>, SegmentTaskInstance::Hash> source_task_idx;
    std::atomic_size_t retry_count = {0};
};

}
