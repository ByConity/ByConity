#include <chrono>
#include <Core/Types.h>
#include <Interpreters/DAGGraph.h>
#include <Interpreters/DistributedStages/BSPScheduler.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/DistributedStages/RuntimeSegmentsStatus.h>
#include <Interpreters/DistributedStages/ScheduleEvent.h>
#include <Interpreters/DistributedStages/SourceTask.h>
#include <Interpreters/WorkerStatusManager.h>
#include <Interpreters/sendPlanSegment.h>
#include <Processors/tests/gtest_exchange_helper.h>
#include <Processors/tests/gtest_processers_utils.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <Common/HostWithPorts.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_utils.h>


struct SendPlanSegmentHistory
{
    std::unordered_map<size_t, std::unordered_map<DB::AddressInfo, std::deque<DB::PlanSegmentInstanceId>, DB::AddressInfo::Hash>> data;
};

/// wont really send
struct MockPlanSegmentSender
{
    SendPlanSegmentHistory history;
    void sendPlanSegmentToAddress(
        const DB::AddressInfo & addr,
        DB::PlanSegment * plan_segment_ptr,
        DB::PlanSegmentExecutionInfo & execution_info,
        DB::ContextPtr /*query_context*/,
        std::shared_ptr<DB::DAGGraph> /*dag_graph_ptr*/,
        std::shared_ptr<butil::IOBuf> /*plan_segment_buf_ptr*/,
        const DB::WorkerId & /*worker_id*/)
    {
        auto instance_id = DB::PlanSegmentInstanceId{static_cast<UInt32>(plan_segment_ptr->getPlanSegmentId()), execution_info.parallel_id};
        history.data[plan_segment_ptr->getPlanSegmentId()][addr].emplace_back(instance_id);
    }
};

struct MockWorkerStatusManager : public DB::WorkerStatusManager
{
    ~MockWorkerStatusManager() override = default;
    DB::ThreadSafeMap<DB::WorkerId, DB::WorkerStatusExtra, DB::WorkerIdHash> data;
    std::optional<DB::WorkerStatusExtra> getWorkerStatus(const DB::WorkerId & worker_id) override
    {
        return data.get(worker_id);
    }
};

/// Source task split
/// Retry at source node Task, and there are no free workers when retry happended
/// INPUT: 4 tasks with 2 physical nodes, task 0, 1 will be scheduled to node1, task 2,3 should be scheduled to node2
/// EVENTS: Schedule, task 0 and task 1 dispatched, task 0 fail, task 1 or task 0 dispatched, task 2 succ, task 3 dispatched, task 3 succ, task 0 or 1 dispatched
/// EXPECTED: Failed source task should always restart at the same worker node
TEST(GtestScheduler, SourceTaskSplitRetryTestCase1)
{
    std::unordered_map<std::string, DB::Field> settings{{"bsp_mode", 1}, {"distributed_max_parallel_size", 4}};
    auto query_context = getContext().createQueryContext("q1", settings);
    DB::TxnTimestamp t1 = 1;
    query_context->initCnchServerResource(t1);
    query_context->setQueryExpirationTimeStamp();

    /// prepare plan segment
    std::vector<std::shared_ptr<DB::PlanSegment>> segments
        = {std::make_shared<DB::PlanSegment>(2, "q1", "c1"), std::make_shared<DB::PlanSegment>(1, "q1", "c2")};
    segments[0]->setParallelSize(4);
    segments[1]->setParallelSize(4);
    DB::Block header;
    std::vector<DB::PlanSegmentInputPtr> segment_inputs
        = {std::make_shared<DB::PlanSegmentInput>(header, DB::PlanSegmentType::SOURCE),
           std::make_shared<DB::PlanSegmentInput>(header, DB::PlanSegmentType::EXCHANGE)};
    segment_inputs[1]->setPlanSegmentId(1);
    segments[0]->appendPlanSegmentInput(segment_inputs[0]);
    segments[1]->appendPlanSegmentInput(segment_inputs[1]);

    /// initialize dag_graph_ptr
    DB::DAGGraphPtr dag_graph_ptr = std::make_shared<DB::DAGGraph>();
    dag_graph_ptr->query_context = query_context;
    dag_graph_ptr->id_to_segment[1] = segments[1].get();
    dag_graph_ptr->id_to_segment[2] = segments[0].get();
    dag_graph_ptr->leaf_segments = {2};
    dag_graph_ptr->table_scan_or_value_segments = {2};

    DB::ClusterNodes cluster_nodes;
    cluster_nodes.all_workers
        = {DB::WorkerNode(DB::AddressInfo("10.10.10.10", 9010, "", ""), DB::NodeType::Remote),
           DB::WorkerNode(DB::AddressInfo("10.10.10.11", 9010, "", ""), DB::NodeType::Remote)};
    cluster_nodes.rank_worker_ids = {1, 2};
    cluster_nodes.all_hosts = {DB::HostWithPorts{"10.10.10.10", 9010}, DB::HostWithPorts{"10.10.10.11", 9010}};
    cluster_nodes.vw_name = "vw1";
    cluster_nodes.worker_group_id = "wg1";

    std::shared_ptr<MockWorkerStatusManager> mock_worker_status_manager = std::make_shared<MockWorkerStatusManager>();
    auto tp = std::chrono::system_clock::now();
    DB::WorkerStatusExtra wrk_status(std::make_shared<DB::WorkerStatus>(), tp);
    wrk_status.worker_status->register_time = 100;
    mock_worker_status_manager->data.set(
        DB::WorkerId(cluster_nodes.vw_name, cluster_nodes.worker_group_id, cluster_nodes.all_workers[0].id), wrk_status);
    mock_worker_status_manager->data.set(
        DB::WorkerId(cluster_nodes.vw_name, cluster_nodes.worker_group_id, cluster_nodes.all_workers[1].id), wrk_status);

    std::shared_ptr<DB::BSPScheduler> bsp_scheduler = std::make_shared<DB::BSPScheduler>(
        "q1", t1.toUInt64(), query_context, cluster_nodes, dag_graph_ptr, nullptr, mock_worker_status_manager);
    MockPlanSegmentSender mock_sender;
    bsp_scheduler->setSendPlanSegmentToAddress([&](const DB::AddressInfo & addr,
                                                   DB::PlanSegment * plan_segment_ptr,
                                                   DB::PlanSegmentExecutionInfo & execution_info,
                                                   DB::ContextPtr query_context_,
                                                   std::shared_ptr<DB::DAGGraph> dag_graph_ptr_,
                                                   std::shared_ptr<butil::IOBuf> plan_segment_buf_ptr,
                                                   const DB::WorkerId & worker_id) {
        mock_sender.sendPlanSegmentToAddress(
            addr, plan_segment_ptr, execution_info, query_context_, dag_graph_ptr_, plan_segment_buf_ptr, worker_id);
    });

    /// initialize source task payload
    auto & source_task_payload = query_context->getCnchServerResource()->getSourceTaskPayloadRef();
    DB::UUID uuid(1);
    for (const auto & worker : cluster_nodes.all_workers)
        source_task_payload[uuid][worker.address] = DB::SourceTaskPayload{.rows = 10, .part_num = 1};

    /// First Event, ScheduleTask
    DB::ScheduleBatchTaskEvent event1(std::make_shared<DB::BatchTask>(std::vector<DB::SegmentTask>{DB::SegmentTask(2, true, true)}));
    bsp_scheduler->processEvent(event1);
    /// check queue
    std::shared_ptr<DB::ScheduleEvent> event2;
    bool has_event = bsp_scheduler->getEventToProcess(event2);
    ASSERT_TRUE(has_event);
    ASSERT_EQ(event2->getType(), DB::ScheduleEventType::TriggerDispatch);
    ASSERT_TRUE(dynamic_cast<DB::TriggerDispatchEvent &>(*event2).all_workers);

    // Second Event, trigger dispatch
    bsp_scheduler->processEvent(*event2);

    ASSERT_FALSE(bsp_scheduler->hasEvent());

    auto & wrk_1_history = mock_sender.history.data[2][cluster_nodes.all_workers[0].address];
    auto & wrk_2_history = mock_sender.history.data[2][cluster_nodes.all_workers[1].address];
    ASSERT_EQ(wrk_1_history.size(), 1);
    ASSERT_EQ(wrk_2_history.size(), 1);
    auto failed_instance = wrk_1_history[0];
    wrk_1_history.clear();

    // return succ and dispatch all the rest tasks
    DB::RuntimeSegmentStatus status;
    status.is_succeed = false;
    DB::SegmentInstanceFinishedEvent event3(failed_instance.segment_id, failed_instance.parallel_index, status);
    bsp_scheduler->processEvent(event3);

    while (!wrk_2_history.empty())
    {
        std::shared_ptr<DB::ScheduleEvent> event;
        if (bsp_scheduler->hasEvent())
        {
            has_event = bsp_scheduler->getEventToProcess(event);
            ASSERT_TRUE(has_event);
            bsp_scheduler->processEvent(*event);
        }
        auto instance = wrk_2_history.front();
        status.is_succeed = true;
        status.segment_id = instance.segment_id;
        status.parallel_index = instance.parallel_index;
        status.attempt_id = bsp_scheduler->getAttemptId(instance);
        event = std::make_shared<DB::SegmentInstanceFinishedEvent>(instance.segment_id, instance.parallel_index, status);
        bsp_scheduler->processEvent(*event);
        wrk_2_history.pop_front();
    }

    while (!wrk_1_history.empty())
    {
        std::shared_ptr<DB::ScheduleEvent> event;
        if (bsp_scheduler->hasEvent())
        {
            has_event = bsp_scheduler->getEventToProcess(event);
            ASSERT_TRUE(has_event);
            bsp_scheduler->processEvent(*event);
        }
        auto instance = wrk_1_history.front();
        status.is_succeed = true;
        status.segment_id = instance.segment_id;
        status.parallel_index = instance.parallel_index;
        status.attempt_id = bsp_scheduler->getAttemptId(instance);
        event = std::make_shared<DB::SegmentInstanceFinishedEvent>(instance.segment_id, instance.parallel_index, status);
        bsp_scheduler->processEvent(*event);
        wrk_1_history.pop_front();
    }

    /// failed task must be dispatched to original location
    ASSERT_EQ(
        dag_graph_ptr->finished_address[failed_instance.segment_id][failed_instance.parallel_index].toShortString(),
        cluster_nodes.all_workers[0].address.toShortString());
}
