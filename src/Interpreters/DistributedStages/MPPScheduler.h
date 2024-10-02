#include <Interpreters/DistributedStages/Scheduler.h>

namespace DB
{

// Once the dependencies scheduled, the segment would be scheduled.
//
// Scheduler::genTopology -> Scheduler::scheduleTask -> MPPScheduler::submitTasks -> Scheduler::dispatchOrSaveTask -rpc-> Worker node
class MPPScheduler : public Scheduler
{
public:
    MPPScheduler(const String & query_id_, ContextPtr query_context_, std::shared_ptr<DAGGraph> dag_graph_ptr_, bool batch_schedule_)
        : Scheduler(query_id_, query_context_, dag_graph_ptr_, batch_schedule_)
    {
    }

protected:
    PlanSegmentExecutionInfo generateExecutionInfo(size_t task_id, size_t index) override;

private:
    void genTasks() override;
    void genBatchTasks();
    void submitTasks(PlanSegment * plan_segment_ptr, const SegmentTask & task) override;
    // We do nothing.
    void onSegmentScheduled(const SegmentTask &) override {}
    void onSegmentFinished(const size_t &, bool, bool) override {}
    void prepareTask(PlanSegment * plan_segment_ptr, NodeSelectorResult & selector_info, const SegmentTask & task) override;
    void prepareFinalTaskImpl(PlanSegment * final_plan_segment, const AddressInfo & addr) override;
};

using MPPSchedulerPtr = std::unique_ptr<MPPScheduler>;

}
