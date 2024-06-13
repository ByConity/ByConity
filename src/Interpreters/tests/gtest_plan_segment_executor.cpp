/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
#include <Processors/Exchange/DataTrans/BroadcastSenderProxy.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/IBroadcastReceiver.h>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include <Processors/Exchange/DataTrans/Local/LocalBroadcastChannel.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <Processors/tests/gtest_processers_utils.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/QueryPlan.h>
#include <QueryPlan/RemoteExchangeSourceStep.h>
#include <gtest/gtest.h>
#include <Poco/ConsoleChannel.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_utils.h>
#include <common/types.h>
#include <Interpreters/DistributedStages/PlanSegmentInstance.h>

using namespace DB;

namespace UnitTest
{
TEST(PlanSegmentExecutor, ExecuteTest)
{
    initLogger();
    auto context = Context::createCopy(getContext().context);
    context->setProcessListEntry(nullptr);
    const size_t rows = 100;
    Block block = createUInt64Block(rows, 10, 88);
    Block header = block.cloneEmpty();
    Chunk chunk(block.mutateColumns(), rows);
    ColumnsWithTypeAndName arguments;


    arguments.push_back(header.getByPosition(1));
    arguments.push_back(header.getByPosition(2));
    auto func = createRepartitionFunction(getContext().context, arguments);

    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_nsec += 2000 * 1000000;
    ExchangeOptions exchange_options{.exchange_timeout_ts = ts};

    const String query_id = "PlanSegmentExecutor_test";
    const UInt64 query_tx_id = 12345;
    context->setTemporaryTransaction(query_tx_id, query_tx_id, false);
    context->setPlanSegmentInstanceId({1,0});

    AddressInfo coordinator_address("localhost", 8888, "test", "123456");
    AddressInfo local_address("localhost", 0, "test", "123456");

    auto coordinator_address_str = extractExchangeHostPort(coordinator_address);
    LocalChannelOptions options{10, exchange_options.exchange_timeout_ts, false};

    auto source_key = std::make_shared<ExchangeDataKey>(query_tx_id, 1, 0);
    BroadcastSenderProxyPtr source_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(source_key);
    source_sender->accept(context, header);

    auto sink_key = std::make_shared<ExchangeDataKey>(query_tx_id, 2, 0);
    BroadcastSenderProxyPtr sink_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(sink_key);
    auto sink_channel = std::make_shared<LocalBroadcastChannel>(sink_key, options, LocalBroadcastChannel::generateNameForTest());
    sink_sender->becomeRealSender(sink_channel);
    BroadcastReceiverPtr sink_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(sink_channel);

    PlanSegmentInputs inputs;

    auto input = std::make_shared<PlanSegmentInput>(header, PlanSegmentType::EXCHANGE);
    input->setExchangeParallelSize(1);
    input->setExchangeId(1);
    input->setPlanSegmentId(1);
    input->insertSourceAddress(local_address);
    inputs.push_back(input);

    auto output = std::make_shared<PlanSegmentOutput>(header, PlanSegmentType::EXCHANGE);
    output->setParallelSize(1);
    output->setExchangeParallelSize(1);
    output->setExchangeId(2);
    output->setPlanSegmentId(3);
    output->setExchangeMode(ExchangeMode::REPARTITION);

    auto plan_segment = std::make_unique<PlanSegment>();
    plan_segment->setQueryId(query_id);
    plan_segment->setPlanSegmentId(2);
    plan_segment->setCoordinatorAddress(coordinator_address);
    plan_segment->appendPlanSegmentInputs(inputs);
    plan_segment->appendPlanSegmentOutput(output);

    context->getClientInfo().initial_query_id = plan_segment->getQueryId();
    context->getClientInfo().current_query_id = plan_segment->getQueryId() + std::to_string(plan_segment->getPlanSegmentId());

    DataStream datastream{.header = header};
    auto exchange_source_step = std::make_unique<RemoteExchangeSourceStep>(inputs, datastream, false, false);
    exchange_source_step->setPlanSegment(plan_segment.get(), context);
    exchange_source_step->setExchangeOptions(exchange_options);

    auto sender_func = [&]() {
        for (int i = 0; i < 5; i++)
        {
            BroadcastStatus status = source_sender->send(chunk.clone());
            ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
        }
        source_sender->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "sink test");
    };

    ThreadFromGlobalPool thread(std::move(sender_func));
    SCOPE_EXIT({
        if (thread.joinable())
            thread.join();
    });

    QueryPlan query_plan;
    QueryPlan::Node remote_node{.step = std::move(exchange_source_step), .children = {}};
    query_plan.addRoot(std::move(remote_node));
    plan_segment->setQueryPlan(std::move(query_plan));
    auto plan_segment_instance = std::make_unique<PlanSegmentInstance>();
    plan_segment_instance->info.parallel_id = 0;
    plan_segment_instance->plan_segment = std::move(plan_segment);
    PlanSegmentExecutor executor(std::move(plan_segment_instance), context, exchange_options);

    executor.execute();


    for (int i = 0; i < 5; i++)
    {
        RecvDataPacket recv_res = sink_receiver->recv(2000);
        ASSERT_TRUE(std::holds_alternative<Chunk>(recv_res));
        Chunk & recv_chunk = std::get<Chunk>(recv_res);
        ASSERT_TRUE(recv_chunk.getNumRows() == rows);
        ASSERT_TRUE(recv_chunk.bytes() == chunk.bytes());
    }
}

TEST(PlanSegmentExecutor, ExecuteAsyncTest)
{
    initLogger();
    const auto context = Context::createCopy(getContext().context);
    context->setProcessListEntry(nullptr);
    const size_t rows = 100;
    Block block = createUInt64Block(rows, 10, 88);
    Block header = block.cloneEmpty();
    Chunk chunk(block.mutateColumns(), rows);
    ColumnsWithTypeAndName arguments;

    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_nsec += 2000 * 1000000;
    ExchangeOptions exchange_options{.exchange_timeout_ts = ts};

    const String query_id = "PlanSegmentExecutor_test";
    const UInt64 query_tx_id = 11111;
    context->setTemporaryTransaction(query_tx_id, query_tx_id, false);
    context->setPlanSegmentInstanceId({1, 0});

    AddressInfo coordinator_address("localhost", 8888, "test", "123456");
    auto coordinator_address_str = extractExchangeHostPort(coordinator_address);
    AddressInfo local_address("localhost", 0, "test", "123456");

    LocalChannelOptions options{10, exchange_options.exchange_timeout_ts, false};

    auto source_key = std::make_shared<ExchangeDataKey>(query_tx_id, 1, 0);
    BroadcastSenderProxyPtr source_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(source_key);
    source_sender->accept(context, header);

    auto sink_key = std::make_shared<ExchangeDataKey>(query_tx_id, 2, 0);
    BroadcastSenderProxyPtr sink_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(sink_key);
    auto sink_channel = std::make_shared<LocalBroadcastChannel>(sink_key, options, LocalBroadcastChannel::generateNameForTest());
    sink_sender->becomeRealSender(sink_channel);
    BroadcastReceiverPtr sink_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(sink_channel);


    auto plan_segment_instance = std::make_unique<PlanSegmentInstance>();
    plan_segment_instance->info.parallel_id = 0;
    plan_segment_instance->info.execution_address = local_address;

    PlanSegmentInputs inputs;

    auto input = std::make_shared<PlanSegmentInput>(header, PlanSegmentType::EXCHANGE);
    input->setExchangeParallelSize(1);
    input->setExchangeId(1);
    input->setPlanSegmentId(1);
    input->insertSourceAddress(local_address);
    inputs.push_back(input);

    auto output = std::make_shared<PlanSegmentOutput>(header, PlanSegmentType::EXCHANGE);
    output->setParallelSize(1);
    output->setExchangeParallelSize(1);
    output->setExchangeId(2);
    output->setPlanSegmentId(3);
    output->setExchangeMode(ExchangeMode::REPARTITION);

    auto plan_segment = std::make_unique<PlanSegment>();
    plan_segment->setQueryId(query_id);
    plan_segment->setPlanSegmentId(2);
    plan_segment->setCoordinatorAddress(coordinator_address);
    plan_segment->appendPlanSegmentInputs(inputs);
    plan_segment->appendPlanSegmentOutput(output);

    context->getClientInfo().initial_query_id = plan_segment->getQueryId();
    context->getClientInfo().current_query_id = plan_segment->getQueryId() + std::to_string(plan_segment->getPlanSegmentId());

    DataStream datastream{.header = header};
    auto exchange_source_step = std::make_unique<RemoteExchangeSourceStep>(inputs, datastream, false, false);
    exchange_source_step->setPlanSegment(plan_segment.get(), context);
    exchange_source_step->setExchangeOptions(exchange_options);

    arguments.push_back(header.getByPosition(1));
    arguments.push_back(header.getByPosition(2));
    auto func = createRepartitionFunction(getContext().context, arguments);
    auto total_bytes = chunk.bytes();

    QueryPlan query_plan;
    QueryPlan::Node remote_node{.step = std::move(exchange_source_step), .children = {}};
    query_plan.addRoot(std::move(remote_node));
    plan_segment->setQueryPlan(std::move(query_plan));
    plan_segment_instance->plan_segment = std::move(plan_segment);
    PlanSegmentExecutor executor(std::move(plan_segment_instance), context, exchange_options);

    auto execute_func = [&]() { executor.execute(); };

    ThreadFromGlobalPool thread(std::move(execute_func));
    SCOPE_EXIT({
        if (thread.joinable())
            thread.join();
    });

    for (int i = 0; i < 5; i++)
    {
        BroadcastStatus status = source_sender->send(chunk.clone());
        ASSERT_EQ(status.code, BroadcastStatusCode::RUNNING) << status.message;
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
    context->setProcessListEntry(nullptr);
    const size_t rows = 100;
    Block block = createUInt64Block(rows, 10, 88);
    Block header = block.cloneEmpty();
    Chunk chunk(block.mutateColumns(), rows);
    ColumnsWithTypeAndName arguments;

    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_nsec += 1000 * 1000000;
    ExchangeOptions exchange_options{.exchange_timeout_ts = ts};

    const String query_id = "PlanSegmentExecutor_test";
    const UInt64 query_tx_id = 11111;
    context->setTemporaryTransaction(query_tx_id,query_tx_id,false);
    context->setPlanSegmentInstanceId({1, 0});

    AddressInfo coordinator_address("localhost", 8888, "test", "123456");
    AddressInfo local_address("localhost", 0, "test", "123456");

    auto coordinator_address_str = extractExchangeHostPort(coordinator_address);
    LocalChannelOptions options{10, exchange_options.exchange_timeout_ts, false};

    auto source_key = std::make_shared<ExchangeDataKey>(query_tx_id, 1, 0);
    BroadcastSenderProxyPtr source_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(source_key);
    source_sender->accept(context, header);

    auto sink_key = std::make_shared<ExchangeDataKey>(query_tx_id, 2, 0);
    BroadcastSenderProxyPtr sink_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(sink_key);
    auto sink_channel  = std::make_shared<LocalBroadcastChannel>(sink_key, options, LocalBroadcastChannel::generateNameForTest());
    sink_sender->becomeRealSender(sink_channel);
    BroadcastReceiverPtr sink_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(sink_channel);

    auto plan_segment_instance = std::make_unique<PlanSegmentInstance>();
    plan_segment_instance->info.parallel_id = 0;
    plan_segment_instance->info.execution_address = local_address;

    PlanSegmentInputs inputs;

    auto input = std::make_shared<PlanSegmentInput>(header, PlanSegmentType::EXCHANGE);
    input->setExchangeParallelSize(1);
    input->setExchangeId(1);
    input->setPlanSegmentId(1);
    input->insertSourceAddress(local_address);
    inputs.push_back(input);

    auto output = std::make_shared<PlanSegmentOutput>(header, PlanSegmentType::EXCHANGE);
    output->setParallelSize(1);
    output->setExchangeParallelSize(1);
    output->setExchangeId(2);
    output->setPlanSegmentId(3);
    output->setExchangeMode(ExchangeMode::REPARTITION);

    auto plan_segment = std::make_unique<PlanSegment>();
    plan_segment->setQueryId(query_id);
    plan_segment->setPlanSegmentId(2);
    plan_segment->setCoordinatorAddress(coordinator_address);
    plan_segment->appendPlanSegmentInputs(inputs);
    plan_segment->appendPlanSegmentOutput(output);

    context->getClientInfo().initial_query_id = plan_segment->getQueryId();
    context->getClientInfo().current_query_id = plan_segment->getQueryId() + std::to_string(plan_segment->getPlanSegmentId());
    DataStream datastream{.header = header};
    auto exchange_source_step = std::make_unique<RemoteExchangeSourceStep>(inputs, datastream, false, false);
    exchange_source_step->setPlanSegment(plan_segment.get(), context);
    exchange_source_step->setExchangeOptions(exchange_options);

    arguments.push_back(header.getByPosition(1));
    arguments.push_back(header.getByPosition(2));
    auto func = createRepartitionFunction(getContext().context, arguments);
    auto total_bytes = chunk.bytes();

    QueryPlan query_plan;
    QueryPlan::Node remote_node{.step = std::move(exchange_source_step), .children = {}};
    query_plan.addRoot(std::move(remote_node));
    plan_segment->setQueryPlan(std::move(query_plan));
    plan_segment_instance->plan_segment = std::move(plan_segment);
    // buffer will flush when row_num reached to send_threshold_in_row_num
    PlanSegmentExecutor executor(std::move(plan_segment_instance), context, exchange_options);

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

    for (int i = 0; i < 2; i++)
    {
        RecvDataPacket recv_res = sink_receiver->recv(5000);
        ASSERT_TRUE(std::holds_alternative<Chunk>(recv_res));
        Chunk & recv_chunk = std::get<Chunk>(recv_res);
        ASSERT_TRUE(recv_chunk.getNumRows() == rows);
        ASSERT_TRUE(recv_chunk.bytes() == total_bytes);
    }

    CancellationCode code = CancellationCode::NotFound;
    int max_time = 100;
    for (; code == CancellationCode::NotFound; code = context->getPlanSegmentProcessList().tryCancelPlanSegmentGroup(query_id))
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        max_time--;
        if(max_time < 0)
            break;
    }

    ASSERT_TRUE(code == CancellationCode::CancelSent);

    RecvDataPacket recv_res = sink_receiver->recv(5000);
    ASSERT_TRUE(std::holds_alternative<BroadcastStatus>(recv_res));
    ASSERT_TRUE(std::get<BroadcastStatus>(recv_res).code == BroadcastStatusCode::SEND_CANCELLED);
}

}
