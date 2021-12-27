#include <memory>
#include <string>
#include <thread>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/ExchangeMode.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/DistributedStages/PlanSegmentExecutor.h>
#include <Interpreters/DistributedStages/executePlanSegment.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/IBroadcastReceiver.h>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include <Processors/Exchange/DataTrans/Local/LocalBroadcastRegistry.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/RemoteExchangeSourceStep.h>
#include <Processors/tests/gtest_processers_utils.h>
#include <gtest/gtest.h>
#include <Poco/ConsoleChannel.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_utils.h>

using namespace DB;

namespace UnitTest
{
TEST(PlanSegmentExecutor, ExecuteTest)
{
    initLogger();
    const auto & context = getContext().context;
    const size_t rows = 100;
    Block block = createUInt64Block(rows, 10, 88);
    Block header = block.cloneEmpty();
    Chunk chunk(block.mutateColumns(), rows);
    ColumnsWithTypeAndName arguments;

    ExchangeOptions exchange_options{.exhcange_timeout_ms = 2000, .local_debug_mode = true};

    const String query_id = "PlanSegmentExecutor_test";
    AddressInfo coodinator_address("localhost", 8888, "test", "123456", 9999, 6666);
    auto coodinator_address_str = extractExchangeStatusHostPort(coodinator_address);
    LocalChannelOptions options{10, exchange_options.exhcange_timeout_ms, 1};
    ExchangeDataKey source_key{query_id, 1, 2, 1, coodinator_address_str};
    BroadcastSenderPtr source_sender = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsSender(source_key, options);

    ExchangeDataKey sink_key{query_id, 2, 3, 1, coodinator_address_str};
    BroadcastReceiverPtr sink_receiver = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsReceiver(sink_key, options);

    PlanSegmentInputs inputs;

    auto input = std::make_shared<PlanSegmentInput>(header, PlanSegmentType::EXCHANGE);
    input->setParallelIndex(1);
    input->setExchangeParallelSize(1);
    input->setPlanSegmentId(1);
    inputs.push_back(input);

    auto output = std::make_shared<PlanSegmentOutput>(header, PlanSegmentType::EXCHANGE);
    output->setParallelSize(1);
    output->setExchangeParallelSize(1);
    output->setPlanSegmentId(3);
    output->setExchangeMode(ExchangeMode::REPARTITION);

    PlanSegment plan_segment = PlanSegment();
    plan_segment.setQueryId(query_id);
    plan_segment.setPlanSegmentId(2);
    plan_segment.setContext(context);

    plan_segment.setCoordinatorAddress(coodinator_address);
    plan_segment.appendPlanSegmentInputs(inputs);
    plan_segment.setPlanSegmentOutput(output);

    DataStream datastream{.header = header};
    auto exchange_source_step = std::make_unique<RemoteExchangeSourceStep>(inputs, datastream);
    exchange_source_step->setPlanSegment(&plan_segment);
    exchange_source_step->setExchangeOptions(exchange_options);

    arguments.push_back(header.getByPosition(1));
    arguments.push_back(header.getByPosition(2));
    auto func = createRepartitionFunction(getContext().context, arguments);
    auto total_bytes = chunk.bytes();

    for (int i = 0; i < 5; i++)
    {
        BroadcastStatus status = source_sender->send(chunk.clone());
        ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
    }

    source_sender->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "sink test");

    QueryPlan query_plan;
    QueryPlan::Node remote_node{.step = std::move(exchange_source_step), .children = {}};
    query_plan.addRoot(std::move(remote_node));
    plan_segment.setQueryPlan(std::move(query_plan));
    PlanSegmentExecutor executor(std::make_unique<PlanSegment>(std::move(plan_segment)), context, exchange_options);
    executor.execute();

    for (int i = 0; i < 5; i++)
    {
        RecvDataPacket recv_res = sink_receiver->recv(2000);
        ASSERT_TRUE(std::holds_alternative<Chunk>(recv_res));
        Chunk & recv_chunk = std::get<Chunk>(recv_res);
        ASSERT_TRUE(recv_chunk.getNumRows() == rows);
        ASSERT_TRUE(recv_chunk.bytes() == total_bytes);
    }
}

TEST(PlanSegmentExecutor, ExecuteAsyncTest)
{
    initLogger();
    const auto context = Context::createCopy(getContext().context);
    const size_t rows = 100;
    Block block = createUInt64Block(rows, 10, 88);
    Block header = block.cloneEmpty();
    Chunk chunk(block.mutateColumns(), rows);
    ColumnsWithTypeAndName arguments;

    ExchangeOptions exchange_options{.exhcange_timeout_ms = 2000, .local_debug_mode = true};

    const String query_id = "PlanSegmentExecutor_test";
    AddressInfo coodinator_address("localhost", 8888, "test", "123456", 9999, 6666);
    auto coodinator_address_str = extractExchangeStatusHostPort(coodinator_address);
    LocalChannelOptions options{10, exchange_options.exhcange_timeout_ms, 1};
    ExchangeDataKey source_key{query_id, 1, 2, 1, coodinator_address_str};
    BroadcastSenderPtr source_sender = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsSender(source_key, options);

    ExchangeDataKey sink_key{query_id, 2, 3, 1, coodinator_address_str};
    BroadcastReceiverPtr sink_receiver = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsReceiver(sink_key, options);

    PlanSegmentInputs inputs;

    auto input = std::make_shared<PlanSegmentInput>(header, PlanSegmentType::EXCHANGE);
    input->setParallelIndex(1);
    input->setExchangeParallelSize(1);
    input->setPlanSegmentId(1);
    inputs.push_back(input);

    auto output = std::make_shared<PlanSegmentOutput>(header, PlanSegmentType::EXCHANGE);
    output->setParallelSize(1);
    output->setExchangeParallelSize(1);
    output->setPlanSegmentId(3);
    output->setExchangeMode(ExchangeMode::REPARTITION);

    PlanSegment plan_segment = PlanSegment();
    plan_segment.setQueryId(query_id);
    plan_segment.setPlanSegmentId(2);
    plan_segment.setContext(context);

    plan_segment.setCoordinatorAddress(coodinator_address);
    plan_segment.appendPlanSegmentInputs(inputs);
    plan_segment.setPlanSegmentOutput(output);

    DataStream datastream{.header = header};
    auto exchange_source_step = std::make_unique<RemoteExchangeSourceStep>(inputs, datastream);
    exchange_source_step->setPlanSegment(&plan_segment);
    exchange_source_step->setExchangeOptions(exchange_options);

    arguments.push_back(header.getByPosition(1));
    arguments.push_back(header.getByPosition(2));
    auto func = createRepartitionFunction(getContext().context, arguments);
    auto total_bytes = chunk.bytes();

    QueryPlan query_plan;
    QueryPlan::Node remote_node{.step = std::move(exchange_source_step), .children = {}};
    query_plan.addRoot(std::move(remote_node));
    plan_segment.setQueryPlan(std::move(query_plan));
    PlanSegmentExecutor executor(std::make_unique<PlanSegment>(std::move(plan_segment)), context, exchange_options);
    auto execute_func = [&]() { executor.execute(); };

    ThreadFromGlobalPool thread(std::move(execute_func));
    SCOPE_EXIT({
        if (thread.joinable())
            thread.join();
    });

    for (int i = 0; i < 5; i++)
    {
        BroadcastStatus status = source_sender->send(chunk.clone());
        ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
    }

    source_sender->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "sink test");

    for (int i = 0; i < 5; i++)
    {
        RecvDataPacket recv_res = sink_receiver->recv(2000);
        ASSERT_TRUE(std::holds_alternative<Chunk>(recv_res));
        Chunk & recv_chunk = std::get<Chunk>(recv_res);
        ASSERT_TRUE(recv_chunk.getNumRows() == rows);
        ASSERT_TRUE(recv_chunk.bytes() == total_bytes);
    }
}

TEST(PlanSegmentExecutor, ExecuteCancelTest)
{
    initLogger();
    const auto context = Context::createCopy(getContext().context);
    const size_t rows = 100;
    Block block = createUInt64Block(rows, 10, 88);
    Block header = block.cloneEmpty();
    Chunk chunk(block.mutateColumns(), rows);
    ColumnsWithTypeAndName arguments;

    ExchangeOptions exchange_options{.exhcange_timeout_ms = 10000, .local_debug_mode = true};

    const String query_id = "PlanSegmentExecutor_test";
    AddressInfo coodinator_address("localhost", 8888, "test", "123456", 9999, 6666);
    auto coodinator_address_str = extractExchangeStatusHostPort(coodinator_address);
    LocalChannelOptions options{10, exchange_options.exhcange_timeout_ms, 1};
    ExchangeDataKey source_key{query_id, 1, 2, 1, coodinator_address_str};
    BroadcastSenderPtr source_sender = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsSender(source_key, options);

    ExchangeDataKey sink_key{query_id, 2, 3, 1, coodinator_address_str};
    BroadcastReceiverPtr sink_receiver = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsReceiver(sink_key, options);

    PlanSegmentInputs inputs;

    auto input = std::make_shared<PlanSegmentInput>(header, PlanSegmentType::EXCHANGE);
    input->setParallelIndex(1);
    input->setExchangeParallelSize(1);
    input->setPlanSegmentId(1);
    inputs.push_back(input);

    auto output = std::make_shared<PlanSegmentOutput>(header, PlanSegmentType::EXCHANGE);
    output->setParallelSize(1);
    output->setExchangeParallelSize(1);
    output->setPlanSegmentId(3);
    output->setExchangeMode(ExchangeMode::REPARTITION);

    PlanSegment plan_segment = PlanSegment();
    plan_segment.setQueryId(query_id);
    plan_segment.setPlanSegmentId(2);
    plan_segment.setContext(context);

    plan_segment.setCoordinatorAddress(coodinator_address);
    plan_segment.appendPlanSegmentInputs(inputs);
    plan_segment.setPlanSegmentOutput(output);

    DataStream datastream{.header = header};
    auto exchange_source_step = std::make_unique<RemoteExchangeSourceStep>(inputs, datastream);
    exchange_source_step->setPlanSegment(&plan_segment);
    exchange_source_step->setExchangeOptions(exchange_options);

    arguments.push_back(header.getByPosition(1));
    arguments.push_back(header.getByPosition(2));
    auto func = createRepartitionFunction(getContext().context, arguments);
    auto total_bytes = chunk.bytes();

    for (int i = 0; i < 2; i++)
    {
        BroadcastStatus status = source_sender->send(chunk.clone());
        ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
    }

    QueryPlan query_plan;
    QueryPlan::Node remote_node{.step = std::move(exchange_source_step), .children = {}};
    query_plan.addRoot(std::move(remote_node));
    plan_segment.setQueryPlan(std::move(query_plan));
    // buffer will flush when row_num reached to send_threshold_in_row_num
    PlanSegmentExecutor executor(std::make_unique<PlanSegment>(std::move(plan_segment)), context, exchange_options);
    // PlanSegmentExecutor executor(std::make_unique<PlanSegment>(std::move(plan_segment)), context);

    auto execute_func = [&]() { executor.execute(); };

    ThreadFromGlobalPool thread(std::move(execute_func));
    SCOPE_EXIT({
        if (thread.joinable())
            thread.join();
    });

    for (int i = 0; i < 2; i++)
    {
        RecvDataPacket recv_res = sink_receiver->recv(5000);
        ASSERT_TRUE(std::holds_alternative<Chunk>(recv_res));
        Chunk & recv_chunk = std::get<Chunk>(recv_res);
        ASSERT_TRUE(recv_chunk.getNumRows() == rows);
        ASSERT_TRUE(recv_chunk.bytes() == total_bytes);
    }

    CancellationCode code = CancellationCode::NotFound;
    for (; code == CancellationCode::NotFound; code = context->getPlanSegmentProcessList().tryCancelPlanSegmentGroup(query_id))
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_TRUE(code == CancellationCode::CancelSent);

    RecvDataPacket recv_res = sink_receiver->recv(5000);
    ASSERT_TRUE(std::holds_alternative<BroadcastStatus>(recv_res));
    ASSERT_TRUE(std::get<BroadcastStatus>(recv_res).code == BroadcastStatusCode::SEND_CANCELLED);
}

}
