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
#include <gtest/gtest.h>

#include <Common/tests/gtest_utils.h>
#include <Common/tests/gtest_global_context.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxy.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>
#include <Processors/Exchange/DataTrans/IBroadcastReceiver.h>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include <Processors/Exchange/DataTrans/Local/LocalBroadcastChannel.h>
#include <Processors/Exchange/DataTrans/Local/LocalChannelOptions.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <Processors/Exchange/ExchangeSource.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/QueryPipeline.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/tests/gtest_processers_utils.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <Processors/LimitTransform.h>

using namespace DB;
namespace UnitTest
{
TEST(ExchangeSource, LocalNormalTest)
{
    initLogger();
    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_nsec += 200 * 1000000;
    ExchangeOptions exchange_options {.exchange_timeout_ts = ts};

    auto data_key = std::make_shared<ExchangeDataKey>(1, 1, 1);
    auto context = getContext().context;
    LocalChannelOptions options{context->getSettingsRef().exchange_local_receiver_queue_size, exchange_options.exchange_timeout_ts, false};
    auto channel = std::make_shared<LocalBroadcastChannel>(data_key, options, LocalBroadcastChannel::generateNameForTest());
    BroadcastSenderProxyPtr local_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(data_key);
    local_sender->accept(getContext().context, Block());
    BroadcastReceiverPtr local_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(channel);
    local_receiver->registerToSenders(200);

    Chunk chunk = createUInt8Chunk(10, 1, 8);
    auto total_bytes = chunk.bytes();

    setQueryDuration();
    BroadcastStatus status = local_sender->send(std::move(chunk));
    ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);

    Block header = {ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "local_exchange_test")};

    auto exchange_source = std::make_shared<ExchangeSource>(std::move(header), local_receiver, exchange_options);
    QueryPipeline pipeline;

    pipeline.init(Pipe(exchange_source));

    PullingAsyncPipelineExecutor executor(pipeline);
    Chunk pull_chunk;
    ASSERT_TRUE(executor.pull(pull_chunk));
    ASSERT_TRUE(pull_chunk.getNumRows() == 10);
    ASSERT_TRUE(pull_chunk.bytes() == total_bytes);
    try
    {
        /// trigger timeout
        executor.pull(pull_chunk);
        /// rethrow exception
        executor.pull(pull_chunk);
        ASSERT_TRUE(false) << "Should have thrown.";
    }
    catch (DB::Exception & e)
    {
        ASSERT_TRUE(e.displayText().find("timeout") != std::string::npos) << "Expected 'timeout after ms', got: " << e.displayText();
    }

    executor.cancel();

}

TEST(ExchangeSource, LocalLimitTest)
{
    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_nsec += 200 * 1000000;
    ExchangeOptions exchange_options {.exchange_timeout_ts = ts};
    auto data_key = std::make_shared<ExchangeDataKey>(1, 1, 1);
    auto context = getContext().context;
    LocalChannelOptions options{context->getSettingsRef().exchange_local_receiver_queue_size, exchange_options.exchange_timeout_ts, false};
    auto channel = std::make_shared<LocalBroadcastChannel>(data_key, options, LocalBroadcastChannel::generateNameForTest());
    BroadcastSenderProxyPtr local_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(data_key);
    BroadcastReceiverPtr local_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(channel);
    local_sender->accept(getContext().context, Block());
    local_receiver->registerToSenders(200);
    Chunk chunk = createUInt8Chunk(10, 1, 8);

    setQueryDuration();
    for (int i = 0; i < 5; i++)
    {
        BroadcastStatus status = local_sender->send(chunk.clone());
        ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
    }

    Block header = {ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "local_exchange_test")};

    auto exchange_source = std::make_shared<ExchangeSource>(std::move(header), local_receiver, exchange_options);
    QueryPipeline pipeline;

    Pipe pipe;
    pipe.addSource(exchange_source);

    pipe.addTransform(std::make_shared<LimitTransform>(exchange_source->getPort().getHeader(), 1, 0));

    pipeline.init(std::move(pipe));

    PullingAsyncPipelineExecutor executor(pipeline);
    Chunk pull_chunk;
    ASSERT_TRUE(executor.pull(pull_chunk));
    ASSERT_TRUE(pull_chunk.getNumRows() == 1);
    ASSERT_FALSE(executor.pull(pull_chunk) && pull_chunk);
    executor.cancel();
}

}
