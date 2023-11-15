#include <filesystem>
#include <memory>
#include <thread>
#include <Core/BackgroundSchedulePool.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/ExchangeDataTracker.h>
#include <Parsers/IParser.h>
#include <Processors/Exchange/BroadcastExchangeSink.h>
#include <Processors/Exchange/DataTrans/Batch/DiskExchangeDataManager.h>
#include <Processors/Exchange/DataTrans/Batch/Writer/DiskPartitionWriter.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxy.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcExchangeReceiverRegistryService.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcRemoteBroadcastReceiver.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Mock/CyclicProductionSource.h>
#include <Processors/tests/gtest_exchange_helper.h>
#include <Processors/tests/gtest_processers_utils.h>
#include <gtest/gtest.h>
#include <Common/tests/gtest_global_context.h>

using namespace DB;
using namespace UnitTest;

static Block getHeader(size_t column_num)
{
    ColumnsWithTypeAndName columns;
    for (size_t i = 0; i < column_num; i++)
    {
        columns.push_back(ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "column" + std::to_string(i)));
    }
    Block header = {columns};
    return header;
}

void write(ContextMutablePtr & context, Block header, DiskExchangeDataManagerPtr mgr, ExchangeDataKeyPtr key)
{
    mgr->createWriteTaskDirectory(key->query_unique_id);
    auto buf = mgr->createFileBufferForWrite(key);
    DiskPartitionWriterPtr writer = std::make_shared<DiskPartitionWriter>(context, mgr, header, std::move(key), std::move(buf));
    auto origin_chunk = createUInt8Chunk(10, 1, 7);
    BroadcastStatus status = writer->sendImpl(std::move(origin_chunk));
    ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
    mgr->submitWriteTask(writer, nullptr);
    status = writer->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "writer test");
    ASSERT_TRUE(status.code == BroadcastStatusCode::ALL_SENDERS_DONE);
}

TEST_F(ExchangeRemoteTest, DiskExchangeDataWriteAndRead)
{
    auto context = getContext().context;
    auto header = getHeader(1);

    // write to disk
    auto key = std::make_shared<ExchangeDataKey>(query_unique_id, exchange_id, parallel_idx);
    write(context, header, manager, key);

    auto name = BrpcRemoteBroadcastReceiver::generateName(exchange_id, write_segment_id, read_segment_id, parallel_idx, host);
    auto queue = std::make_shared<MultiPathBoundedQueue>(context->getSettingsRef().exchange_remote_receiver_queue_size);
    auto receiver = std::make_shared<BrpcRemoteBroadcastReceiver>(
        key, rpc_host, context, header, true, name, std::move(queue), BrpcExchangeReceiverRegistryService::DISK_READER);
    receiver->registerToSenders(1000);
    auto packet = std::dynamic_pointer_cast<IBroadcastReceiver>(receiver)->recv(1000);
    ASSERT_TRUE(std::holds_alternative<Chunk>(packet));
    auto & ret_chunk = std::get<Chunk>(packet);
    ASSERT_EQ(ret_chunk.getNumRows(), 10);
    auto col = ret_chunk.getColumns().at(0);
    ASSERT_EQ(col->getUInt(1), 7);
}

Processors createMockExecutor(const ExchangeDataKeyPtr & key, Block header, uint64_t interval_ms, size_t rows)
{
    auto source = std::make_shared<CyclicProductionSource>(header, interval_ms, rows);
    auto context = getContext().context;
    BroadcastSenderProxyPtr sender = BroadcastSenderProxyRegistry::instance().getOrCreate(key);
    sender->accept(context, header); //  TODO context -> query context @lianxuechao
    String name = BroadcastExchangeSink::generateName(key->exchange_id);
    ExchangeOptions exchange_options{.exchange_timeout_ts = context->getQueryExpirationTimeStamp(), .force_use_buffer = true};
    auto sink = std::make_shared<BroadcastExchangeSink>(
        std::move(header), std::vector<BroadcastSenderPtr>{sender}, exchange_options, std::move(name));
    connect(source->getOutputs().front(), sink->getInputs().front());
    return {std::move(source), std::move(sink)};
}

TEST_F(ExchangeRemoteTest, DiskExchangeDataCancel)
{
    auto context = getContext().context;
    auto manager = context->getDiskExchangeDataManager();
    auto header = getHeader(1);

    auto key = std::make_shared<ExchangeDataKey>(query_unique_id, exchange_id, parallel_idx);
    write(context, header, manager, key);

    manager->submitReadTask(query_id, key, createMockExecutor(key, header, interval_ms, rows));
    // wait until executor starts execution
    ASSERT_TRUE(manager->getExecutor(key));
    while (!manager->getExecutor(key)->isExecutionInitialized())
        ;

    // register senders
    auto name = BrpcRemoteBroadcastReceiver::generateName(exchange_id, write_segment_id, read_segment_id, parallel_idx, host);
    auto queue = std::make_shared<MultiPathBoundedQueue>(context->getSettingsRef().exchange_remote_receiver_queue_size);
    auto receiver = std::make_shared<BrpcRemoteBroadcastReceiver>(
        key, rpc_host, context, header, true, name, queue, BrpcExchangeReceiverRegistryService::BRPC);
    receiver->registerToSenders(1000);
    // cancel executor
    manager->cancel(key->query_unique_id, key->exchange_id);
    auto packet = std::dynamic_pointer_cast<IBroadcastReceiver>(receiver)->recv(1000);
    ASSERT_TRUE(std::holds_alternative<BroadcastStatus>(packet));
    auto status = std::get<BroadcastStatus>(packet);
    ASSERT_TRUE(status.code == BroadcastStatusCode::SEND_CANCELLED);
}

TEST_F(ExchangeRemoteTest, DiskExchangeDataCleanup)
{
    auto context = getContext().context;
    auto header = getHeader(1);

    auto key = std::make_shared<ExchangeDataKey>(query_unique_id, exchange_id, parallel_idx);
    write(context, header, manager, key);

    manager->cleanup(key->query_unique_id);
    auto file_name = manager->getFileName(*key);
    ASSERT_TRUE(!disk->exists(file_name));
}
