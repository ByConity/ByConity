#include <Interpreters/DistributedStages/Scheduler.h>

namespace DB
{

// Once the dependencies scheduled, the segment would be scheduled.
//
// Scheduler::genTopology -> Scheduler::scheduleTask -> MPPScheduler::submitTasks -> Scheduler::dispatchTask -rpc-> Worker node
class MPPScheduler : public Scheduler
{
public:
    MPPScheduler(const String & query_id_, ContextPtr query_context_, std::shared_ptr<DAGGraph> dag_graph_ptr_)
        : Scheduler(query_id_, query_context_, dag_graph_ptr_)
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

}
