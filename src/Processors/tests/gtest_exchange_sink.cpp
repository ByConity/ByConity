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
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/IBroadcastReceiver.h>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include <Processors/Exchange/DataTrans/Local/LocalBroadcastChannel.h>
#include <Processors/Exchange/DataTrans/Local/LocalBroadcastRegistry.h>
#include <Processors/Exchange/DataTrans/Local/LocalChannelOptions.h>
#include <Processors/Exchange/ExchangeBufferedSender.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <Processors/Exchange/ExchangeSource.h>
#include <Processors/Exchange/LoadBalancedExchangeSink.h>
#include <Processors/Exchange/MultiPartitionExchangeSink.h>
#include <Processors/Exchange/RepartitionTransform.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/QueryPipeline.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/tests/gtest_processers_utils.h>
#include <Common/tests/gtest_global_context.h>
#include <Processors/Exchange/SinglePartitionExchangeSink.h>
#include <Processors/Transforms/BufferedCopyTransform.h>

using namespace DB;
namespace UnitTest
{
TEST(BroadcastExchangeSink, LocalNormalTest)
{
    ExchangeOptions exchange_options {.exhcange_timeout_ms=2000};
    LocalChannelOptions options{10, exchange_options.exhcange_timeout_ms};
    ExchangeDataKey source_key{"", 1, 1, 1, ""};
    BroadcastSenderPtr source_sender = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsSender(source_key, options);
    BroadcastReceiverPtr source_receiver = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsReceiver(source_key, options);

    ExchangeDataKey sink_key{"", 2, 2, 2, ""};
    BroadcastSenderPtr sink_sender = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsSender(sink_key, options);
    BroadcastReceiverPtr sink_receiver = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsReceiver(sink_key, options);

    Chunk chunk = createUInt8Chunk(10, 1, 8);
    auto total_bytes = chunk.bytes();

    for (int i = 0; i < 5; i++)
    {
        BroadcastStatus status = source_sender->send(chunk.clone());
        ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
    }
    source_sender->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "sink test");

    Block header = {ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "local_exchange_test")};
    auto exchange_source = std::make_shared<ExchangeSource>(header, source_receiver, exchange_options);
    auto exchange_sink = std::make_shared<BroadcastExchangeSink>(header, std::vector<BroadcastSenderPtr>{sink_sender});
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

TEST(LoadBalancedExchangeSink, LocalNormalTest)
{
    ExchangeOptions exchange_options {.exhcange_timeout_ms=2000};
    LocalChannelOptions options{10, exchange_options.exhcange_timeout_ms};
    ExchangeDataKey source_key{"", 1, 1, 1, ""};
    BroadcastSenderPtr source_sender = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsSender(source_key, options);
    BroadcastReceiverPtr source_receiver = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsReceiver(source_key, options);

    ExchangeDataKey sink_key{"", 2, 2, 2, ""};
    BroadcastSenderPtr sink_sender = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsSender(sink_key, options);
    BroadcastReceiverPtr sink_receiver = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsReceiver(sink_key, options);

    Chunk chunk = createUInt8Chunk(10, 1, 8);
    auto total_bytes = chunk.bytes();

    for (int i = 0; i < 5; i++)
    {
        BroadcastStatus status = source_sender->send(chunk.clone());
        ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
    }
    source_sender->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "sink test");

    Block header = {ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "local_exchange_test")};
    auto exchange_source = std::make_shared<ExchangeSource>(header, source_receiver, exchange_options);
    auto exchange_sink = std::make_shared<LoadBalancedExchangeSink>(header, std::vector<BroadcastSenderPtr>{sink_sender});
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

TEST(MultiPartitionExchangeSink, LocalNormalTest)
{
    ExchangeOptions exchange_options {.exhcange_timeout_ms=2000};
    LocalChannelOptions options{10, exchange_options.exhcange_timeout_ms};
    ExchangeDataKey source_key{"", 1, 1, 1, ""};
    BroadcastSenderPtr source_sender = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsSender(source_key, options);
    BroadcastReceiverPtr source_receiver = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsReceiver(source_key, options);

    ExchangeDataKey sink_key{"", 2, 2, 2, ""};
    BroadcastSenderPtr sink_sender = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsSender(sink_key, options);
    BroadcastReceiverPtr sink_receiver = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsReceiver(sink_key, options);

    const size_t rows = 100;
    const size_t send_threshold_in_row_num = rows * 2;
    Block block = createUInt64Block(rows, 10, 88);
    Block header = block.cloneEmpty();
    Chunk chunk(block.mutateColumns(), rows);
    ColumnsWithTypeAndName arguments;
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

    auto exchange_source = std::make_shared<ExchangeSource>(header, source_receiver, exchange_options);
    auto exchange_sink = std::make_shared<MultiPartitionExchangeSink>(
        header,
        std::vector<BroadcastSenderPtr>{sink_sender},
        func,
        ColumnNumbers{1, 2},
        ExchangeOptions{1000, 100000000, send_threshold_in_row_num});
    connect(exchange_source->getPort(), exchange_sink->getPort());
    Processors processors;
    processors.emplace_back(std::move(exchange_source));
    processors.emplace_back(std::move(exchange_sink));
    PipelineExecutor executor(processors);
    executor.execute(2);

    // buffer will flush when row_num reached to send_threshold_in_row_num
    for (int i = 0; i < 2; i++)
    {
        RecvDataPacket recv_res = sink_receiver->recv(2000);
        ASSERT_TRUE(std::holds_alternative<Chunk>(recv_res));
        Chunk & recv_chunk = std::get<Chunk>(recv_res);
        ASSERT_TRUE(recv_chunk.getNumRows() == send_threshold_in_row_num);
        ASSERT_TRUE(recv_chunk.bytes() == total_bytes * 2);
    }

    // Sink flush last buffer when finishing.
    RecvDataPacket recv_res = sink_receiver->recv(2000);
    ASSERT_TRUE(std::holds_alternative<Chunk>(recv_res));
    Chunk & recv_chunk = std::get<Chunk>(recv_res);
    ASSERT_TRUE(recv_chunk.getNumRows() == 100);
    ASSERT_TRUE(recv_chunk.bytes() == total_bytes);
}

TEST(SinglePartitionExchangeSink, LocalNormalTest)
{
    ExchangeOptions exchange_options {.exhcange_timeout_ms=2000};
    LocalChannelOptions options{10, exchange_options.exhcange_timeout_ms};
    ExchangeDataKey source_key{"", 1, 1, 1, ""};
    BroadcastSenderPtr source_sender = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsSender(source_key, options);
    BroadcastReceiverPtr source_receiver = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsReceiver(source_key, options);

    ExchangeDataKey sink_key{"", 2, 2, 2, ""};
    BroadcastSenderPtr sink_sender = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsSender(sink_key, options);
    BroadcastReceiverPtr sink_receiver = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsReceiver(sink_key, options);

    const size_t rows = 100;
    Block block = createUInt64Block(rows, 10, 88);
    Block header = block.cloneEmpty();
    Chunk chunk(block.mutateColumns(), rows);
    ColumnsWithTypeAndName arguments;
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

    auto exchange_source = std::make_shared<ExchangeSource>(header, source_receiver, exchange_options);
    auto repartition_transform = std::make_shared<RepartitionTransform>(header, 1, ColumnNumbers{1, 2}, func);
    auto exchange_sink = std::make_shared<SinglePartitionExchangeSink>(header, sink_sender, 0, ExchangeOptions{1000, 0, 0});
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

TEST(SinglePartitionExchangeSink, PipelineTest)
{
    ExchangeOptions exchange_options {.exhcange_timeout_ms=2000};
    LocalChannelOptions options{10, exchange_options.exhcange_timeout_ms};
    ExchangeDataKey source_key{"", 1, 1, 1, ""};
    BroadcastSenderPtr source_sender = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsSender(source_key, options);
    BroadcastReceiverPtr source_receiver = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsReceiver(source_key, options);

    ExchangeDataKey sink_key_1{"", 2, 2, 2, ""};
    BroadcastSenderPtr sink_sender_1 = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsSender(sink_key_1, options);
    BroadcastReceiverPtr sink_receiver_1 = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsReceiver(sink_key_1, options);

    ExchangeDataKey sink_key_2{"", 3, 3, 3, ""};
    BroadcastSenderPtr sink_sender_2 = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsSender(sink_key_2, options);
    BroadcastReceiverPtr sink_receiver_2 = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsReceiver(sink_key_2, options);

    const size_t rows = 100;
    Block block = createUInt64Block(rows, 10, 88);
    Block header = block.cloneEmpty();
    Chunk chunk(block.mutateColumns(), rows);
    ColumnsWithTypeAndName arguments;
    arguments.push_back(header.getByPosition(1));
    arguments.push_back(header.getByPosition(2));
    auto func = createRepartitionFunction(getContext().context, arguments);
    auto chunk_bytes = chunk.bytes();

    for (int i = 0; i < 5; i++)
    {
        BroadcastStatus status = source_sender->send(chunk.clone());
        ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
    }
    source_sender->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "sink test");

    auto exchange_source = std::make_shared<ExchangeSource>(header, source_receiver, exchange_options);
    auto repartition_transform = std::make_shared<RepartitionTransform>(header, 2, ColumnNumbers{1, 2}, func);
    auto buffer_copy_transform = std::make_shared<BufferedCopyTransform>(header, 2, 10);

    auto exchange_sink_1 = std::make_shared<SinglePartitionExchangeSink>(header, sink_sender_1, 0, ExchangeOptions{1000, 0, 0});
    auto exchange_sink_2 = std::make_shared<SinglePartitionExchangeSink>(header, sink_sender_2, 1, ExchangeOptions{1000, 0, 0});

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
