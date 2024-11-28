#include <chrono>
#include <Core/Types.h>
#include <Interpreters/DAGGraph.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/BSPScheduler.h>
#include <Interpreters/DistributedStages/ExchangeDataTracker.h>
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
    bool empty(size_t segment_id)
    {
        for (const auto & [addr, instances] : data[segment_id])
        {
            if (!instances.empty())
                return false;
        }
        return true;
    }
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
    DB::ThreadSafeMap<DB::WorkerId, DB::WorkerStatus, DB::WorkerIdHash> data;
    std::optional<DB::WorkerStatus> getWorkerStatus(const DB::WorkerId & worker_id) override
    {
        return data.get(worker_id);
    }
};

struct SchedulerTestContext
{
    std::vector<std::shared_ptr<DB::PlanSegment>> segments;
    DB::ContextMutablePtr query_context;
    DB::DAGGraphPtr dag_graph_ptr;
    DB::ClusterNodes cluster_nodes;
    std::shared_ptr<MockWorkerStatusManager> mock_worker_status_manager;
    std::shared_ptr<DB::BSPScheduler> bsp_scheduler;
    MockPlanSegmentSender mock_sender;
};

SchedulerTestContext createSchedulerTestContext(size_t parallel_size, const std::unordered_map<std::string, DB::Field> & settings)
{
    SchedulerTestContext result;
    result.query_context = getContext().createQueryContext("q1", settings);
    DB::TxnTimestamp t1 = 1;
    result.query_context->initCnchServerResource(t1);
    result.query_context->initQueryExpirationTimeStamp();

    /// prepare plan segment
    result.segments = {std::make_shared<DB::PlanSegment>(2, "q1", "c1"), std::make_shared<DB::PlanSegment>(1, "q1", "c2")};
    result.segments[0]->setParallelSize(parallel_size);
    result.segments[1]->setParallelSize(parallel_size);
    DB::Block header;
    std::vector<DB::PlanSegmentInputPtr> segment_inputs
        = {std::make_shared<DB::PlanSegmentInput>(header, DB::PlanSegmentType::SOURCE),
           std::make_shared<DB::PlanSegmentInput>(header, DB::PlanSegmentType::EXCHANGE)};
    segment_inputs[1]->setPlanSegmentId(2);
    segment_inputs[1]->setExchangeId(1);
    result.segments[0]->appendPlanSegmentInput(segment_inputs[0]);
    result.segments[1]->appendPlanSegmentInput(segment_inputs[1]);
    DB::PlanSegmentOutputPtr segment_output = std::make_shared<DB::PlanSegmentOutput>(header, DB::PlanSegmentType::OUTPUT);
    segment_output->setParallelSize(parallel_size);
    result.segments[0]->appendPlanSegmentOutput(segment_output);

    /// initialize dag_graph_ptr
    result.dag_graph_ptr = std::make_shared<DB::DAGGraph>();
    result.dag_graph_ptr->query_context = result.query_context;
    result.dag_graph_ptr->id_to_segment[1] = result.segments[1].get();
    result.dag_graph_ptr->id_to_segment[2] = result.segments[0].get();
    result.dag_graph_ptr->leaf_segments = {2};
    result.dag_graph_ptr->table_scan_or_value_segments = {2};
    result.dag_graph_ptr->exchanges[1] = {segment_inputs[1], segment_output};

    result.cluster_nodes.all_workers
        = {DB::WorkerNode(DB::AddressInfo("10.10.10.10", 9010, "", ""), DB::NodeType::Remote),
           DB::WorkerNode(DB::AddressInfo("10.10.10.11", 9010, "", ""), DB::NodeType::Remote)};
    result.cluster_nodes.rank_worker_ids = {1, 2};
    result.cluster_nodes.all_hosts = {DB::HostWithPorts{"10.10.10.10", 9010}, DB::HostWithPorts{"10.10.10.11", 9010}};
    result.cluster_nodes.vw_name = "vw1";
    result.cluster_nodes.worker_group_id = "wg1";

    result.mock_worker_status_manager = std::make_shared<MockWorkerStatusManager>();
    auto tp = std::chrono::system_clock::now();
    DB::WorkerStatus wrk_status(DB::ResourceStatus(), tp);
    wrk_status.resource_status.register_time = 100;
    result.mock_worker_status_manager->data.set(
        DB::WorkerId(result.cluster_nodes.vw_name, result.cluster_nodes.worker_group_id, result.cluster_nodes.all_workers[0].id),
        wrk_status);
    result.mock_worker_status_manager->data.set(
        DB::WorkerId(result.cluster_nodes.vw_name, result.cluster_nodes.worker_group_id, result.cluster_nodes.all_workers[1].id),
        wrk_status);

    DB::ContextWeakMutablePtr context_weak = std::weak_ptr<DB::Context>(result.query_context);
    result.query_context->setMockExchangeDataTracker(std::make_shared<DB::ExchangeStatusTracker>(context_weak));

    result.bsp_scheduler = std::make_shared<DB::BSPScheduler>(
        "q1", t1.toUInt64(), result.query_context, result.cluster_nodes, result.dag_graph_ptr, nullptr, result.mock_worker_status_manager);
    result.bsp_scheduler->setSendPlanSegmentToAddress([&](const DB::AddressInfo & addr,
                                                          DB::PlanSegment * plan_segment_ptr,
                                                          DB::PlanSegmentExecutionInfo & execution_info,
                                                          DB::ContextPtr query_context_,
                                                          std::shared_ptr<DB::DAGGraph> dag_graph_ptr_,
                                                          std::shared_ptr<butil::IOBuf> plan_segment_buf_ptr,
                                                          const DB::WorkerId & worker_id) {
        result.mock_sender.sendPlanSegmentToAddress(
            addr, plan_segment_ptr, execution_info, query_context_, dag_graph_ptr_, plan_segment_buf_ptr, worker_id);
    });

    /// initialize source task payload
    auto & source_task_payload = result.query_context->getCnchServerResource()->getSourceTaskPayloadRef();
    DB::UUID uuid(1);
    for (const auto & worker : result.cluster_nodes.all_workers)
        source_task_payload[uuid][worker.address] = DB::SourceTaskPayload{.rows = 10, .part_num = 1};

    return result;
}

/// Source task split
/// Retry at source node Task, and there are no free workers when retry happended
/// INPUT: 4 tasks with 2 physical nodes, task 0, 1 will be scheduled to node1, task 2,3 should be scheduled to node2
/// EVENTS: Schedule, task 0 and task 2 dispatched, task 0 fail, task 0 dispatched, All tasks in node 2 return succ and dispatch until no one left, All tasks in node 1 return succ and dispatch until no one left
/// EXPECTED: Failed source task should always restart at the same worker node
TEST(GtestScheduler, SourceTaskSplitRetryTestCase1)
{
    size_t parallel_size = 4;
    std::unordered_map<std::string, DB::Field> settings{{"bsp_mode", 1}, {"distributed_max_parallel_size", parallel_size}};
    auto scheduler_context = createSchedulerTestContext(parallel_size, settings);
    auto bsp_scheduler = scheduler_context.bsp_scheduler;
    auto dag_graph_ptr = scheduler_context.dag_graph_ptr;
    auto & cluster_nodes = scheduler_context.cluster_nodes;
    auto & mock_sender = scheduler_context.mock_sender;
    size_t segment_id = 2;

    /// First Event, ScheduleTask
    DB::ScheduleBatchTaskEvent event1(
        std::make_shared<DB::BatchTask>(std::vector<DB::SegmentTask>{DB::SegmentTask(segment_id, true, true)}));
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

    auto & wrk_1_history = mock_sender.history.data[segment_id][cluster_nodes.all_workers[0].address];
    auto & wrk_2_history = mock_sender.history.data[segment_id][cluster_nodes.all_workers[1].address];
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

/// Retry and Failure happened at the same time
/// INPUT: 2 tasks with 2 physical nodes, task 0 will be scheduled to node1, task 1 should be scheduled to node2. bsp_max_retry_num = 5
/// EVENTS: Schedule, task 0 and task 2 dispatched, task 0 fail and retried, task 2 fail and retried, task 0 fail and retried, task 2 fail and retried, task 0 fail and but retry is delayed, task 2 fail and query failed, task 0 retry dispatched
/// EXPECTED: Last Retry should be sent, but the whole query should fail
TEST(GtestScheduler, RetryTestCase1)
{
    size_t parallel_size = 2;
    std::unordered_map<std::string, DB::Field> settings{
        {"bsp_mode", 1},
        {"distributed_max_parallel_size", parallel_size},
        {"bsp_max_retry_num", 2},
        {"enable_disk_shuffle_partition_coalescing", 0}};
    auto scheduler_context = createSchedulerTestContext(parallel_size, settings);
    auto bsp_scheduler = scheduler_context.bsp_scheduler;
    auto & dag_graph_ptr = scheduler_context.dag_graph_ptr;
    auto & cluster_nodes = scheduler_context.cluster_nodes;
    auto & mock_sender = scheduler_context.mock_sender;
    size_t segment_id = 1;
    size_t exchange_id = 1;

    auto exchange_data_tracker = scheduler_context.query_context->getExchangeDataTracker();
    exchange_data_tracker->registerExchange("q1", exchange_id, parallel_size);
    DB::ExchangeStatus exg_status;
    exg_status.worker_addr = DB::AddressInfo();
    exg_status.status = {1, 1};
    exchange_data_tracker->registerExchangeStatus("q1", exchange_id, 0, exg_status);
    exchange_data_tracker->registerExchangeStatus("q1", exchange_id, 1, exg_status);

    dag_graph_ptr->finished_address[2][0] = cluster_nodes.all_workers[0].address;
    dag_graph_ptr->finished_address[2][1] = cluster_nodes.all_workers[1].address;

    std::shared_ptr<DB::ScheduleEvent> event;

    /// First Event, ScheduleTask
    event = std::make_shared<DB::ScheduleBatchTaskEvent>(
        std::make_shared<DB::BatchTask>(std::vector<DB::SegmentTask>{DB::SegmentTask(segment_id, false, false)}));
    bsp_scheduler->processEvent(*event);

    /// check queue
    bool has_event = bsp_scheduler->getEventToProcess(event);
    ASSERT_TRUE(has_event);
    ASSERT_EQ(event->getType(), DB::ScheduleEventType::TriggerDispatch);
    ASSERT_TRUE(dynamic_cast<DB::TriggerDispatchEvent &>(*event).all_workers);

    // Second Event, trigger dispatch
    bsp_scheduler->processEvent(*event);

    ASSERT_FALSE(bsp_scheduler->hasEvent());

    // retry worker 0's task, but delay its dispatch after query failed
    {
        auto & history = mock_sender.history.data[segment_id][cluster_nodes.all_workers[0].address];
        while (!history.empty())
        {
            auto instance = history.back();
            history.pop_back();
            DB::RuntimeSegmentStatus status;
            status.is_succeed = false;
            status.segment_id = instance.segment_id;
            status.parallel_index = instance.parallel_index;
            status.attempt_id = bsp_scheduler->getAttemptId(instance);
            event = std::make_shared<DB::SegmentInstanceFinishedEvent>(instance.segment_id, instance.parallel_index, status);
            bsp_scheduler->processEvent(*event);
            ASSERT_TRUE(bsp_scheduler->hasEvent());
            has_event = bsp_scheduler->getEventToProcess(event);
            ASSERT_TRUE(has_event);
            ASSERT_EQ(event->getType(), DB::ScheduleEventType::TriggerDispatch);
        }
    }

    // worker1's task keeps failing, until query failed
    size_t i = 0;
    while (i++ < 3)
    {
        auto & history = mock_sender.history.data[segment_id][cluster_nodes.all_workers[1].address];
        auto instance = history.back();
        history.pop_back();
        DB::RuntimeSegmentStatus status;
        status.is_succeed = false;
        status.segment_id = instance.segment_id;
        status.parallel_index = instance.parallel_index;
        status.attempt_id = bsp_scheduler->getAttemptId(instance);
        std::shared_ptr<DB::ScheduleEvent> event1
            = std::make_shared<DB::SegmentInstanceFinishedEvent>(instance.segment_id, instance.parallel_index, status);
        bsp_scheduler->processEvent(*event1);
        if (i != 3)
        {
            ASSERT_TRUE(bsp_scheduler->hasEvent());
            has_event = bsp_scheduler->getEventToProcess(event1);
            ASSERT_TRUE(has_event);
            bsp_scheduler->processEvent(*event1);
        }
    }
    ASSERT_FALSE(bsp_scheduler->hasEvent());
    /// even we retry after query failed(which wont happen in real cases, as scheduler is already stopped) and schedule context is released,
    /// we wont meet any coredump.
    bsp_scheduler->processEvent(*event);
}
