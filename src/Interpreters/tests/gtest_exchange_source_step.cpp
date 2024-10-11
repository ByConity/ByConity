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
#include <stdlib.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/DistributedStages/PlanSegmentExecutor.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxy.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include <Processors/Exchange/DataTrans/Local/LocalBroadcastChannel.h>
#include <Processors/Exchange/DataTrans/Local/LocalChannelOptions.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/QueryPipeline.h>
#include <QueryPlan/QueryPlan.h>
#include <QueryPlan/RemoteExchangeSourceStep.h>
#include <Processors/tests/gtest_processers_utils.h>
#include <gtest/gtest.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/Util/MapConfiguration.h>
#include "common/types.h"
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_utils.h>
#include <common/scope_guard.h>


using namespace DB;

namespace UnitTest
{
TEST(ExchangeSourceStep, InitializePipelineTest)
{
    initLogger();
    UInt64 query_tx_id = 666;
    auto context = Context::createCopy(getContext().context);
    AddressInfo local_address("localhost", 0, "test", "123456");
    context->setTemporaryTransaction(query_tx_id, query_tx_id, false);
    context->setPlanSegmentInstanceId({1, 1});

    auto & client_info = context->getClientInfo();
    PlanSegment plan_segment = PlanSegment();
    plan_segment.setQueryId("RemoteExchangeSourceStep_test");
    plan_segment.setPlanSegmentId(2);

    client_info.current_query_id = plan_segment.getQueryId() + std::to_string(plan_segment.getPlanSegmentId());
    client_info.current_user = "test";
    client_info.initial_query_id = plan_segment.getQueryId();
    AddressInfo coordinator_address("localhost", 8888, "test", "123456");

    auto coordinator_address_str = extractExchangeHostPort(coordinator_address);
    plan_segment.setCoordinatorAddress(coordinator_address);
    context->setCoordinatorAddress(coordinator_address);
    setQueryDuration(context);
    Block header = {ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "local_exchange_test")};

    PlanSegmentInputs inputs;
    for (int i = 1; i <= 2; ++i)
    {
        auto input = std::make_shared<PlanSegmentInput>(header, PlanSegmentType::EXCHANGE);
        input->setExchangeParallelSize(1);
        input->setPlanSegmentId(1);
        input->setExchangeId(i);
        input->insertSourceAddress(local_address);
        inputs.push_back(input);
    }


    DataStream datastream{.header = header};
    RemoteExchangeSourceStep exchange_source_step(inputs, datastream, false, false);
    exchange_source_step.setPlanSegment(&plan_segment, context);

    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_nsec += 1000 * 1000000;
    ExchangeOptions exchange_options{.exchange_timeout_ts = ts, .send_threshold_in_bytes = 0};
    exchange_source_step.setExchangeOptions(exchange_options);

    auto data_key_1 = std::make_shared<ExchangeDataKey>(query_tx_id, 1, 1);
    BroadcastSenderProxyPtr local_sender_1 = BroadcastSenderProxyRegistry::instance().getOrCreate(data_key_1);
    local_sender_1->accept(context, header);

    auto data_key_2 = std::make_shared<ExchangeDataKey>(query_tx_id, 2, 1);
    BroadcastSenderProxyPtr local_sender_2 = BroadcastSenderProxyRegistry::instance().getOrCreate(data_key_2);
    local_sender_2->accept(context, header);

    QueryPipeline pipeline;
    exchange_source_step.initializePipeline(pipeline, BuildQueryPipelineSettings::fromContext(context));
    PlanSegmentExecutor::registerAllExchangeReceivers(getLogger("PlanSegmentExecutor"), pipeline, 200);

    Chunk chunk = createUInt8Chunk(10, 1, 8);
    auto total_bytes = chunk.bytes();

    auto sender_func = [&]() {
        local_sender_1->send(chunk.clone());
        local_sender_2->send(chunk.clone());
    };

    ThreadFromGlobalPool thread(std::move(sender_func));
    SCOPE_EXIT({
        if (thread.joinable())
            thread.join();
    });

    PullingAsyncPipelineExecutor executor(pipeline);
    Chunk pull_chunk;
    ASSERT_TRUE(executor.pull(pull_chunk));
    ASSERT_TRUE(pull_chunk.getNumRows() == 10);
    ASSERT_TRUE(pull_chunk.bytes() == total_bytes);
    ASSERT_TRUE(executor.pull(pull_chunk));
    ASSERT_TRUE(pull_chunk.getNumRows() == 10);
    ASSERT_TRUE(pull_chunk.bytes() == total_bytes);

    executor.cancel();
}

}
