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
#include <vector>
#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Core/ColumnNumbers.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Processors/Chunk.h>
#include <Processors/Exchange/BroadcastExchangeSink.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxy.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/IBroadcastReceiver.h>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include <Processors/Exchange/DataTrans/Local/LocalBroadcastChannel.h>
#include <Processors/Exchange/DataTrans/Local/LocalChannelOptions.h>
#include <Processors/Exchange/ExchangeBufferedSender.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <Processors/Exchange/ExchangeSource.h>
#include <Processors/Exchange/LoadBalancedExchangeSink.h>
#include <Processors/Exchange/MultiPartitionExchangeSink.h>
#include <Processors/Exchange/RepartitionTransform.h>
#include <Processors/Exchange/SinglePartitionExchangeSink.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/QueryPipeline.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/Transforms/BufferedCopyTransform.h>
#include <Processors/tests/gtest_processers_utils.h>
#include <Common/tests/gtest_global_context.h>

using namespace DB;
namespace UnitTest
{
TEST(ExchangeSink, BroadcastExchangeSinkTest)
{
    auto context = getContext().context;
    Block header = {ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "local_exchange_test")};
    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_nsec += 1000 * 1000000;
    ExchangeOptions exchange_options {.exchange_timeout_ts = ts};
    LocalChannelOptions options{10, exchange_options.exchange_timeout_ts, false};
    auto source_key = std::make_shared<ExchangeDataKey>(1, 1, 1);
    auto source_channel = std::make_shared<LocalBroadcastChannel>(source_key, options, LocalBroadcastChannel::generateNameForTest());
    BroadcastSenderProxyPtr source_sender
        = BroadcastSenderProxyRegistry::instance().getOrCreate(source_key, SenderProxyOptions{.wait_timeout_ms = 2000});
    source_sender->accept(context, header);
    source_channel->registerToSenders(1000);
    BroadcastReceiverPtr source_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(source_channel);

    auto sink_key = std::make_shared<ExchangeDataKey>(1, 2, 2);
    auto sink_channel = std::make_shared<LocalBroadcastChannel>(sink_key, options, LocalBroadcastChannel::generateNameForTest());
    BroadcastSenderProxyPtr sink_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(sink_key);
    sink_sender->accept(context, header);
    sink_channel->registerToSenders(1000);
    BroadcastReceiverPtr sink_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(sink_channel);

    Chunk chunk = createUInt8Chunk(10, 1, 8);
    auto total_bytes = chunk.bytes();

    setQueryDuration();
    for (int i = 0; i < 5; i++)
    {
        BroadcastStatus status = source_sender->send(chunk.clone());
        ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
    }
    source_sender->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "sink test");

    auto exchange_source = std::make_shared<ExchangeSource>(header, source_receiver, exchange_options);
    auto exchange_sink = std::make_shared<BroadcastExchangeSink>(header, std::vector<BroadcastSenderPtr>{sink_sender}, exchange_options, BroadcastExchangeSink::generateNameForTest());
    connect(exchange_source->getPort(), exchange_sink->getPort());
    Processors processors;
    processors.emplace_back(std::move(exchange_source));
    processors.emplace_back(std::move(exchange_sink));
    PipelineExecutor executor(processors);
    executor.execute(2);
    for (int i = 0; i < 5; i++)
    {
        RecvDataPacket recv_res = sink_receiver->recv(2000);
        ASSERT_TRUE(std::holds_alternative<Chunk>(recv_res));
        Chunk & recv_chunk = std::get<Chunk>(recv_res);
        ASSERT_TRUE(recv_chunk.getNumRows() == 10);
        ASSERT_TRUE(recv_chunk.bytes() == total_bytes);
    }
}

TEST(ExchangeSink, BroadcastExchangeSinkBufferTest)
{
    auto context = getContext().context;
    Block header = {ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "local_exchange_test")};
    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_nsec += 1000 * 1000000;
    ExchangeOptions exchange_options {.exchange_timeout_ts = ts, .force_use_buffer = true};
    LocalChannelOptions options{10, exchange_options.exchange_timeout_ts, false};
    auto source_key = std::make_shared<ExchangeDataKey>(1, 1, 1);
    auto source_channel = std::make_shared<LocalBroadcastChannel>(source_key, options, LocalBroadcastChannel::generateNameForTest());
    BroadcastSenderProxyPtr source_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(source_key);
    source_sender->accept(context, header);
    source_channel->registerToSenders(1000);
    BroadcastReceiverPtr source_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(source_channel);

    auto sink_key = std::make_shared<ExchangeDataKey>(1, 2, 2);
    auto sink_channel = std::make_shared<LocalBroadcastChannel>(sink_key, options, LocalBroadcastChannel::generateNameForTest());
    BroadcastSenderProxyPtr sink_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(sink_key);
    sink_sender->accept(context, header);
    sink_channel->registerToSenders(1000);
    BroadcastReceiverPtr sink_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(sink_channel);

    Chunk chunk = createUInt8Chunk(10, 1, 8);
    auto total_bytes = chunk.bytes();

    setQueryDuration();
    for (int i = 0; i < 5; i++)
    {
        BroadcastStatus status = source_sender->send(chunk.clone());
        ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
    }
    source_sender->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "sink test");

    auto exchange_source = std::make_shared<ExchangeSource>(header, source_receiver, exchange_options);
    auto exchange_sink = std::make_shared<BroadcastExchangeSink>(header, std::vector<BroadcastSenderPtr>{sink_sender}, exchange_options, BroadcastExchangeSink::generateNameForTest());
    connect(exchange_source->getPort(), exchange_sink->getPort());
    Processors processors;
    processors.emplace_back(std::move(exchange_source));
    processors.emplace_back(std::move(exchange_sink));
    PipelineExecutor executor(processors);
    executor.execute(2);
    for (int i = 0; i < 5; i++)
    {
        RecvDataPacket recv_res = sink_receiver->recv(2000);
        ASSERT_TRUE(std::holds_alternative<Chunk>(recv_res));
        Chunk & recv_chunk = std::get<Chunk>(recv_res);
        ASSERT_TRUE(recv_chunk.getNumRows() == 10);
        ASSERT_TRUE(recv_chunk.bytes() == total_bytes);
    }
}

TEST(ExchangeSink, LoadBalancedExchangeSinkTest)
{
    auto context = getContext().context;
    Block header = {ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "local_exchange_test")};
    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_nsec += 1000 * 1000000;
    ExchangeOptions exchange_options {.exchange_timeout_ts = ts};
    LocalChannelOptions options{10, exchange_options.exchange_timeout_ts, false};
    auto source_key = std::make_shared<ExchangeDataKey>(1, 1, 1);
    auto source_channel = std::make_shared<LocalBroadcastChannel>(source_key, options, LocalBroadcastChannel::generateNameForTest());
    BroadcastSenderProxyPtr source_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(source_key);
    source_sender->accept(context, header);
    source_channel->registerToSenders(1000);
    BroadcastReceiverPtr source_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(source_channel);

    auto sink_key = std::make_shared<ExchangeDataKey>(1, 2, 2);
    auto sink_channel = std::make_shared<LocalBroadcastChannel>(sink_key, options, LocalBroadcastChannel::generateNameForTest());
    BroadcastSenderProxyPtr sink_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(sink_key);
    sink_sender->accept(context, header);
    sink_channel->registerToSenders(1000);
    BroadcastReceiverPtr sink_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(sink_channel);

    Chunk chunk = createUInt8Chunk(10, 1, 8);
    auto total_bytes = chunk.bytes();

    setQueryDuration();
    for (int i = 0; i < 5; i++)
    {
        BroadcastStatus status = source_sender->send(chunk.clone());
        ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
    }
    source_sender->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "sink test");

    auto exchange_source = std::make_shared<ExchangeSource>(header, source_receiver, exchange_options);
    auto exchange_sink = std::make_shared<LoadBalancedExchangeSink>(header, std::vector<BroadcastSenderPtr>{sink_sender}, LoadBalancedExchangeSink::generateNameForTest());
    connect(exchange_source->getPort(), exchange_sink->getPort());
    Processors processors;
    processors.emplace_back(std::move(exchange_source));
    processors.emplace_back(std::move(exchange_sink));
    PipelineExecutor executor(processors);
    executor.execute(2);

    for (int i = 0; i < 5; i++)
    {
        RecvDataPacket recv_res = sink_receiver->recv(2000);
        ASSERT_TRUE(std::holds_alternative<Chunk>(recv_res));
        Chunk & recv_chunk = std::get<Chunk>(recv_res);
        ASSERT_TRUE(recv_chunk.getNumRows() == 10);
        ASSERT_TRUE(recv_chunk.bytes() == total_bytes);
    }
}

TEST(ExchangeSink, MultiPartitionExchangeSinkTest)
{
    auto context = getContext().context;
    const size_t rows = 100;
    Block block = createUInt64Block(rows, 10, 88);
    Block header = block.cloneEmpty();
    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_nsec += 1000 * 1000000;
    ExchangeOptions exchange_options {.exchange_timeout_ts = ts};
    LocalChannelOptions options{10, exchange_options.exchange_timeout_ts, false};
    auto source_key = std::make_shared<ExchangeDataKey>(1, 1, 1);
    auto source_channel = std::make_shared<LocalBroadcastChannel>(source_key, options, LocalBroadcastChannel::generateNameForTest());
    BroadcastSenderProxyPtr source_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(source_key);
    source_sender->accept(context, header);
    source_channel->registerToSenders(1000);
    BroadcastReceiverPtr source_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(source_channel);

    auto sink_key = std::make_shared<ExchangeDataKey>(1, 2, 2);
    auto sink_channel = std::make_shared<LocalBroadcastChannel>(sink_key, options, LocalBroadcastChannel::generateNameForTest());
    BroadcastSenderProxyPtr sink_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(sink_key);
    sink_sender->accept(context, header);
    sink_channel->registerToSenders(1000);
    BroadcastReceiverPtr sink_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(sink_channel);

    Chunk chunk(block.mutateColumns(), rows);
    ColumnsWithTypeAndName arguments;
    arguments.push_back(header.getByPosition(1));
    arguments.push_back(header.getByPosition(2));
    auto func = createRepartitionFunction(getContext().context, arguments);
    auto total_bytes = chunk.bytes();

    setQueryDuration();
    for (int i = 0; i < 5; i++)
    {
        BroadcastStatus status = source_sender->send(chunk.clone());
        ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
    }
    source_sender->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "sink test");

    auto exchange_source = std::make_shared<ExchangeSource>(header, source_receiver, exchange_options);
    auto exchange_sink = std::make_shared<MultiPartitionExchangeSink>(
        header,
        std::vector<BroadcastSenderPtr>{sink_sender},
        func,
        ColumnNumbers{1, 2},
        ExchangeOptions{ts, 100000000, rows},
        MultiPartitionExchangeSink::generateNameForTest());
    connect(exchange_source->getPort(), exchange_sink->getPort());
    Processors processors;
    processors.emplace_back(std::move(exchange_source));
    processors.emplace_back(std::move(exchange_sink));
    PipelineExecutor executor(processors);
    executor.execute(2);

    for (int i = 0; i < 5; i++)
    {
        RecvDataPacket recv_res = sink_receiver->recv(2000);
        ASSERT_TRUE(std::holds_alternative<Chunk>(recv_res));
        Chunk & recv_chunk = std::get<Chunk>(recv_res);
        ASSERT_TRUE(recv_chunk.getNumRows() == rows);
        ASSERT_TRUE(recv_chunk.bytes() == total_bytes);
    }
}

TEST(ExchangeSink, SinglePartitionExchangeSinkNormalTest)
{
    auto context = getContext().context;
    const size_t rows = 100;
    Block block = createUInt64Block(rows, 10, 88);
    Block header = block.cloneEmpty();

    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_nsec += 1000 * 1000000;
    ExchangeOptions exchange_options {.exchange_timeout_ts = ts};
    LocalChannelOptions options{10, exchange_options.exchange_timeout_ts, false};
    auto source_key = std::make_shared<ExchangeDataKey>(1, 1, 1);
    auto source_channel = std::make_shared<LocalBroadcastChannel>(source_key, options, LocalBroadcastChannel::generateNameForTest());
    BroadcastSenderProxyPtr source_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(source_key);
    source_sender->accept(context, header);
    source_channel->registerToSenders(1000);
    BroadcastReceiverPtr source_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(source_channel);

    auto sink_key = std::make_shared<ExchangeDataKey>(1, 2, 2);
    auto sink_channel = std::make_shared<LocalBroadcastChannel>(sink_key, options, LocalBroadcastChannel::generateNameForTest());
    BroadcastSenderProxyPtr sink_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(sink_key);
    sink_sender->accept(context, header);
    sink_channel->registerToSenders(1000);
    BroadcastReceiverPtr sink_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(sink_channel);

    Chunk chunk(block.mutateColumns(), rows);
    ColumnsWithTypeAndName arguments;
    arguments.push_back(header.getByPosition(1));
    arguments.push_back(header.getByPosition(2));
    auto func = createRepartitionFunction(getContext().context, arguments);
    auto total_bytes = chunk.bytes();

    setQueryDuration();
    for (int i = 0; i < 5; i++)
    {
        BroadcastStatus status = source_sender->send(chunk.clone());
        ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
    }
    source_sender->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "sink test");

    auto exchange_source = std::make_shared<ExchangeSource>(header, source_receiver, exchange_options);
    auto repartition_transform = std::make_shared<RepartitionTransform>(header, 1, ColumnNumbers{1, 2}, func);
    auto exchange_sink = std::make_shared<SinglePartitionExchangeSink>(
        header, sink_sender, 0, ExchangeOptions{ts, 0, 0}, SinglePartitionExchangeSink::generateNameForTest());
    connect(exchange_source->getPort(), repartition_transform->getInputPort());
    connect(repartition_transform->getOutputPort(), exchange_sink->getPort());

    Processors processors;
    processors.emplace_back(std::move(exchange_source));
    processors.emplace_back(std::move(repartition_transform));
    processors.emplace_back(std::move(exchange_sink));
    PipelineExecutor executor(processors);
    executor.execute(2);

    for (int i = 0; i < 5; i++)
    {
        RecvDataPacket recv_res = sink_receiver->recv(2000);
        ASSERT_TRUE(std::holds_alternative<Chunk>(recv_res));
        Chunk & recv_chunk = std::get<Chunk>(recv_res);
        ASSERT_TRUE(recv_chunk.getNumRows() == rows);
        ASSERT_TRUE(recv_chunk.bytes() == total_bytes);
    }
}

TEST(ExchangeSink, SinglePartitionExchangeSinkPipelineTest)
{
    auto context = getContext().context;
    const size_t rows = 100;
    Block block = createUInt64Block(rows, 10, 88);
    Block header = block.cloneEmpty();

    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_nsec += 1000 * 1000000;
    ExchangeOptions exchange_options {.exchange_timeout_ts = ts};
    LocalChannelOptions options{10, exchange_options.exchange_timeout_ts, false};
    auto source_key = std::make_shared<ExchangeDataKey>(1, 1, 1);
    auto source_channel = std::make_shared<LocalBroadcastChannel>(source_key, options, LocalBroadcastChannel::generateNameForTest());
    BroadcastSenderProxyPtr source_sender = BroadcastSenderProxyRegistry::instance().getOrCreate(source_key);
    source_sender->accept(context, header);
    source_channel->registerToSenders(1000);
    BroadcastReceiverPtr source_receiver = std::dynamic_pointer_cast<IBroadcastReceiver>(source_channel);

    auto sink_key_1 = std::make_shared<ExchangeDataKey>(1, 2, 2);
    auto sink_channel_1 = std::make_shared<LocalBroadcastChannel>(sink_key_1, options, LocalBroadcastChannel::generateNameForTest());
    BroadcastSenderProxyPtr sink_sender_1 = BroadcastSenderProxyRegistry::instance().getOrCreate(sink_key_1);
    sink_sender_1->accept(context, header);
    sink_channel_1->registerToSenders(1000);
    BroadcastReceiverPtr sink_receiver_1 = std::dynamic_pointer_cast<IBroadcastReceiver>(sink_channel_1);

    auto sink_key_2 = std::make_shared<ExchangeDataKey>(1, 3, 3);
    auto sink_channel_2 = std::make_shared<LocalBroadcastChannel>(sink_key_2, options, LocalBroadcastChannel::generateNameForTest());
    BroadcastSenderProxyPtr sink_sender_2 = BroadcastSenderProxyRegistry::instance().getOrCreate(sink_key_2);
    sink_sender_2->accept(context, header);
    sink_channel_2->registerToSenders(1000);
    BroadcastReceiverPtr sink_receiver_2 = std::dynamic_pointer_cast<IBroadcastReceiver>(sink_channel_2);

    Chunk chunk(block.mutateColumns(), rows);
    ColumnsWithTypeAndName arguments;
    arguments.push_back(header.getByPosition(1));
    arguments.push_back(header.getByPosition(2));
    auto func = createRepartitionFunction(getContext().context, arguments);
    auto chunk_bytes = chunk.bytes();

    setQueryDuration();
    for (int i = 0; i < 5; i++)
    {
        BroadcastStatus status = source_sender->send(chunk.clone());
        ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
    }
    source_sender->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "sink test");

    auto exchange_source = std::make_shared<ExchangeSource>(header, source_receiver, exchange_options);
    auto repartition_transform = std::make_shared<RepartitionTransform>(header, 2, ColumnNumbers{1, 2}, func);
    auto buffer_copy_transform = std::make_shared<BufferedCopyTransform>(header, 2, 10);

    auto exchange_sink_1 = std::make_shared<SinglePartitionExchangeSink>(
        header, sink_sender_1, 0, ExchangeOptions{ts, 0, 0}, SinglePartitionExchangeSink::generateNameForTest());
    auto exchange_sink_2 = std::make_shared<SinglePartitionExchangeSink>(
        header, sink_sender_2, 1, ExchangeOptions{ts, 0, 0}, SinglePartitionExchangeSink::generateNameForTest());

    connect(exchange_source->getPort(), repartition_transform->getInputPort());
    connect(repartition_transform->getOutputPort(), buffer_copy_transform->getInputPort());
    connect(buffer_copy_transform->getOutputs().front(), exchange_sink_1->getPort());
    connect(buffer_copy_transform->getOutputs().back(), exchange_sink_2->getPort());

    Processors processors;
    processors.emplace_back(std::move(exchange_source));
    processors.emplace_back(std::move(repartition_transform));
    processors.emplace_back(std::move(buffer_copy_transform));
    processors.emplace_back(std::move(exchange_sink_1));
    processors.emplace_back(std::move(exchange_sink_2));

    PipelineExecutor executor(processors);
    executor.execute(2);

    sink_sender_1->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "sink1 finish");
    sink_sender_2->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "sink2 finish");

    size_t total_bytes = 0;

    for (int i = 0; i < 5; i++)
    {
        RecvDataPacket recv_res = sink_receiver_1->recv(2000);
        if (std::holds_alternative<Chunk>(recv_res))
        {
            Chunk & recv_chunk = std::get<Chunk>(recv_res);
            total_bytes += recv_chunk.bytes();
        }
    }

    ASSERT_TRUE(total_bytes == chunk_bytes * 5);
}

}
