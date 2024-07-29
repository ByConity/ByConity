#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <variant>
#include <Core/BackgroundSchedulePool.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/ExchangeDataTracker.h>
#include <Parsers/IParser.h>
#include <Processors/Chunk.h>
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
#include <Poco/Exception.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>

using namespace DB;
using namespace UnitTest;

namespace DB
{
namespace ErrorCodes
{
    extern const int BSP_EXCHANGE_DATA_DISK_LIMIT_EXCEEDED;
}
}

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
    /// For now, in unit test, server and worker is sharing the same brpc, but its okay,
    /// since in this test server only uses ExchangeDataTracker, and worker uses only DiskExchangeManager
    context->getExchangeDataTracker()->registerExchange(std::to_string(key->query_unique_id), key->exchange_id, 0);
    mgr->createWriteTaskDirectory(
        key->query_unique_id, std::to_string(key->query_unique_id), fmt::format("127.0.0.1:{}", brpc_server_port));
    DiskPartitionWriterPtr writer = std::make_shared<DiskPartitionWriter>(context, mgr, header, std::move(key));
    auto origin_chunk = createUInt8Chunk(10, 1, 7);
    BroadcastStatus status = writer->sendImpl(std::move(origin_chunk));
    ASSERT_EQ(status.code, BroadcastStatusCode::RUNNING) << fmt::format("status_code:{} message:{}", status.code, status.message);
    mgr->submitWriteTask(writer->getKey()->query_unique_id, {1, 0}, writer, nullptr);
    status = writer->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "writer test");
    ASSERT_EQ(status.code, BroadcastStatusCode::ALL_SENDERS_DONE) << fmt::format("status_code:{} message:{}", status.code, status.message);
}

TEST_F(ExchangeRemoteTest, DiskExchangeDataWriteAndRead)
{
    auto context = getContext().context;
    auto header = getHeader(1);

    // write to disk
    auto key = std::make_shared<ExchangeDataKey>(query_unique_id_1, exchange_id, parallel_idx);
    write(context, header, manager, key);

    auto name = BrpcRemoteBroadcastReceiver::generateName(exchange_id, write_segment_id, read_segment_id, parallel_idx, rpc_host);
    auto receiver = std::make_shared<BrpcRemoteBroadcastReceiver>(
        key, rpc_host, context, header, true, name, BrpcExchangeReceiverRegistryService::DISK_READER);
    receiver->registerToSenders(1000);
    auto packet = std::dynamic_pointer_cast<IBroadcastReceiver>(receiver)->recv(1000);
    ASSERT_TRUE(std::holds_alternative<Chunk>(packet));
    auto & ret_chunk = std::get<Chunk>(packet);
    ASSERT_EQ(ret_chunk.getNumRows(), 10);
    auto col = ret_chunk.getColumns().at(0);
    ASSERT_EQ(col->getUInt(1), 7);
    manager->cleanup(key->query_unique_id);
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
    auto context = Context::createCopy(getContext().context);
    context->setPlanSegmentInstanceId({1, static_cast<UInt32>(parallel_idx)});
    auto manager = context->getDiskExchangeDataManager();
    auto header = getHeader(1);

    auto key = std::make_shared<ExchangeDataKey>(query_unique_id_2, exchange_id, parallel_idx);
    write(context, header, manager, key);

    auto mock_executor = createMockExecutor(key, header, interval_ms, rows);
    manager->submitReadTask(query_id, key, mock_executor);
    // wait until executor starts execution
    ASSERT_TRUE(manager->getExecutor(key));
    while (!manager->getExecutor(key)->isExecutionInitialized())
        ;

    // register senders
    auto name = BrpcRemoteBroadcastReceiver::generateName(exchange_id, write_segment_id, read_segment_id, parallel_idx, rpc_host);
    auto receiver = std::make_shared<BrpcRemoteBroadcastReceiver>(
        key, rpc_host, context, header, true, name, BrpcExchangeReceiverRegistryService::BRPC);
    receiver->registerToSenders(1000);

    // wait until sender becomes real sender, so when we cancel executor, it wont be empty
    for (const auto & processor : mock_executor)
    {
        if (auto sink = std::dynamic_pointer_cast<BroadcastExchangeSink>(processor))
        {
            for (const auto & sender : sink->getSenders())
            {
                auto proxy = std::dynamic_pointer_cast<BroadcastSenderProxy>(sender);
                proxy->waitBecomeRealSender(5000);
            }
        }
    }

    // cancel executor
    manager->cancelReadTask(key->query_unique_id, key->exchange_id);
    auto packet = std::dynamic_pointer_cast<IBroadcastReceiver>(receiver)->recv(1000);
    ASSERT_TRUE(std::holds_alternative<BroadcastStatus>(packet));
    auto status = std::get<BroadcastStatus>(packet);
    ASSERT_EQ(status.code, BroadcastStatusCode::SEND_CANCELLED) << fmt::format("status_code:{} message:{}", status.code, status.message);
    manager->cleanup(key->query_unique_id);
    ASSERT_EQ(manager->getDiskWrittenBytes(), 0);
}

TEST_F(ExchangeRemoteTest, DiskExchangeDataCleanup)
{
    auto context = getContext().context;
    auto header = getHeader(1);

    auto key = std::make_shared<ExchangeDataKey>(query_unique_id_3, exchange_id, parallel_idx);
    write(context, header, manager, key);
    ASSERT_EQ(manager->getDiskWrittenBytes(), 39);
    manager->cleanup(key->query_unique_id);
    ASSERT_EQ(manager->getDiskWrittenBytes(), 0);
    auto file_name = manager->getFileName(*key);
    ASSERT_TRUE(!disk->exists(file_name));
}

TEST_F(ExchangeRemoteTest, DiskExchangeGarbageCollectionByHeartBeat)
{
    auto context = getContext().context;
    auto header = getHeader(1);
    /// test unregister
    auto test_query_unique_id = 1000;
    auto key = std::make_shared<ExchangeDataKey>(test_query_unique_id, exchange_id, parallel_idx);
    write(context, header, manager, key);
    manager->gc();
    ASSERT_TRUE(disk->exists("bsp/v-1.0.0/1000"));
    context->getExchangeDataTracker()->unregisterExchanges(std::to_string(test_query_unique_id));
    manager->gc();
    ASSERT_TRUE(!disk->exists("bsp/v-1.0.0/1000"));
    /// test invalid file name
    disk->createDirectories("bsp/v-1.0.0/invalid");
    disk->createFile(fs::path("bsp/v-1.0.0/invalid") / "tmp_file");
    disk->createFile("bsp/v-1.0.0/invalid-file");

    manager->gc();
    ASSERT_EQ(manager->getDiskWrittenBytes(), 0);
    std::mutex mu;
    std::condition_variable cv;
    for (size_t i = 0; i < 100; i++)
    {
        std::unique_lock<std::mutex> lock(mu);
        cv.wait_for(lock, std::chrono::milliseconds((10)), [&]() {
            return !disk->exists("bsp/v-1.0.0/1000") && !disk->exists("bsp/v-1.0.0/invalid") && !disk->exists("bsp/v-1.0.0/invalid-file");
        });
    }

    ASSERT_TRUE(!disk->exists("bsp/v-1.0.0/1000"));
    ASSERT_TRUE(!disk->exists("bsp/v-1.0.0/invalid"));
    ASSERT_TRUE(!disk->exists("bsp/v-1.0.0/invalid-file"));
}

TEST_F(ExchangeRemoteTest, DiskExchangeGarbageCollectionByExpire)
{
    auto context = getContext().context;
    auto header = getHeader(1);
    auto key = std::make_shared<ExchangeDataKey>(query_unique_id_4, exchange_id, parallel_idx);
    ASSERT_EQ(manager->getDiskWrittenBytes(), 0);
    write(context, header, manager, key);
    ASSERT_EQ(manager->getDiskWrittenBytes(), 39);

    manager->setFileExpireSeconds(0);
    manager->gc();
    manager->setFileExpireSeconds(10000);
    ASSERT_EQ(manager->getDiskWrittenBytes(), 0);

    std::mutex mu;
    std::condition_variable cv;
    for (size_t i = 0; i < 100; i++)
    {
        std::unique_lock<std::mutex> lock(mu);
        cv.wait_for(
            lock, std::chrono::milliseconds((10)), [&]() { return !disk->exists(fmt::format("bsp/v-1.0.0/{}", query_unique_id_4)); });
    }
    ASSERT_TRUE(!disk->exists(fmt::format("bsp/v-1.0.0/{}", query_unique_id_4)));
}

TEST_F(ExchangeRemoteTest, DiskExchangeSizeLimit)
{
    auto context = getContext().context;
    auto header = getHeader(1);
    auto query_unique_id_5 = 555;
    auto key1 = std::make_shared<ExchangeDataKey>(query_unique_id_5, exchange_id, parallel_idx);
    ASSERT_EQ(manager->getDiskWrittenBytes(), 0);
    write(context, header, manager, key1);
    ASSERT_EQ(manager->getDiskWrittenBytes(), 39);
    manager->setMaxDiskBytes(1);
    auto query_unique_id_6 = 666;
    auto key2 = std::make_shared<ExchangeDataKey>(query_unique_id_6, exchange_id, parallel_idx);
    try
    {
        write(context, header, manager, key2);
        ASSERT_TRUE(false);
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::BSP_EXCHANGE_DATA_DISK_LIMIT_EXCEEDED) << "code:" << e.code() << " e.message:" << e.message();
    }
    manager->cleanup(key1->query_unique_id);
    ASSERT_EQ(manager->getDiskWrittenBytes(), 0);
}
