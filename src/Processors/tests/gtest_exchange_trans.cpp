#include <chrono>
#include <memory>
#include <thread>
#include <variant>
#include <Columns/ColumnsNumber.h>
#include <Compression/CompressedReadBuffer.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcExchangeReceiverRegistryService.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcRemoteBroadcastReceiver.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcRemoteBroadcastSender.h>
#include <Processors/Exchange/DataTrans/Brpc/ReadBufferFromBrpcBuf.h>
#include <Processors/Exchange/DataTrans/Brpc/WriteBufferFromBrpcBuf.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include <Processors/Exchange/DataTrans/NativeChunkInputStream.h>
#include <Processors/Exchange/DataTrans/NativeChunkOutputStream.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <Processors/Exchange/ExchangeSource.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/LimitTransform.h>
#include <Processors/QueryPipeline.h>
#include <Processors/tests/gtest_processers_utils.h>
#include <brpc/server.h>
#include <Poco/Util/MapConfiguration.h>
#include <Common/Brpc/BrpcApplication.h>
#include <gtest/gtest.h>
#include <Common/ClickHouseRevision.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_utils.h>

using namespace DB;
using namespace UnitTest;

using Clock = std::chrono::system_clock;

class ExchangeRemoteTest: public ::testing::Test {
protected:
    static std::shared_ptr<std::thread> thread_server;
    static void start_brpc_server()
    {
        static brpc::Server server;
        const auto & context = getContext().context;
        BrpcExchangeReceiverRegistryService service_impl(context->getSettingsRef().exchange_stream_max_buf_size);

        // Add the service into server. Notice the second parameter, because the
        // service is put on stack, we don't want server to delete it, otherwise
        // use brpc::SERVER_OWNS_SERVICE.
        if (server.AddService(&service_impl, brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
        {
            LOG(ERROR) << "Fail to add service";
            return;
        }
        LOG(INFO) << "add service success";

        // Start the server.
        brpc::ServerOptions options;
        options.idle_timeout_sec = -1;
        if (server.Start(8001, &options) != 0)
        {
            LOG(ERROR) << "Fail to start Server";
            return;
        }
        LOG(INFO) << "start Server";
        sleep(10);
        // server.RunUntilAskedToQuit();
    }

    static void SetUpTestCase()
    {
        UnitTest::initLogger();
        thread_server = std::make_shared<std::thread>(start_brpc_server);
    }

    static void TearDownTestCase()
    {
        // server.Stop(1000);
        thread_server->detach();
    }
};

std::shared_ptr<std::thread> ExchangeRemoteTest::thread_server = std::make_shared<std::thread>();

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

void receiver1()
{
    auto receiver_data = std::make_shared<ExchangeDataKey>("q1", 1, 1, 1, "");
    Block header = getHeader(1);
    BrpcRemoteBroadcastReceiverShardPtr receiver
        = std::make_shared<BrpcRemoteBroadcastReceiver>(receiver_data, "127.0.0.1:8001", getContext().context, header);
    receiver->registerToSenders(20 * 1000);
    auto packet = receiver->recv(20 * 1000);
    EXPECT_TRUE(std::holds_alternative<Chunk>(packet));
    Chunk & chunk = std::get<Chunk>(packet);
    EXPECT_EQ(chunk.getNumRows(), 1000);
    auto col = chunk.getColumns().at(0);
    EXPECT_EQ(col->getUInt(1), 7);
}

void receiver2()
{
    auto receiver_data = std::make_shared<ExchangeDataKey>("q1", 1, 1, 2, "");
    Block header = getHeader(1);
    BrpcRemoteBroadcastReceiverShardPtr receiver
        = std::make_shared<BrpcRemoteBroadcastReceiver>(receiver_data, "127.0.0.1:8001", getContext().context, header);
    receiver->registerToSenders(20 * 1000);
    auto packet = receiver->recv(20 * 1000);
    EXPECT_TRUE(std::holds_alternative<Chunk>(packet));
    Chunk & chunk = std::get<Chunk>(packet);
    EXPECT_EQ(chunk.getNumRows(), 1000);
    auto col = chunk.getColumns().at(0);
    EXPECT_EQ(col->getUInt(1), 7);
}

TEST_F(ExchangeRemoteTest, SendWithTwoReceivers)
{
    Poco::AutoPtr<Poco::Util::MapConfiguration> map_config = new Poco::Util::MapConfiguration;
    BrpcApplication::getInstance().initialize(*map_config);
    auto receiver_data1 = std::make_shared<ExchangeDataKey>("q1", 1, 1, 1, "");
    auto receiver_data2 = std::make_shared<ExchangeDataKey>("q1", 1, 1, 2, "");

    auto origin_chunk = createUInt8Chunk(1000, 1, 7);
    auto header = getHeader(1);
    // std::thread thread_server(start_brpc_server);
    // sleep for a while waiting for server
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    std::thread thread_receiver1(receiver1);
    std::thread thread_receiver2(receiver2);

    BrpcRemoteBroadcastSender sender({receiver_data1, receiver_data2}, getContext().context, header);
    sender.waitAllReceiversReady(100 * 1000);
    sender.send(std::move(origin_chunk));
    thread_receiver1.join();
    thread_receiver2.join();
    // server.Stop(1000);
    // thread_server.detach();
}

TEST_F(ExchangeRemoteTest, SerDserChunk)
{
    // ser
    auto origin_chunk = createUInt8Chunk(1000, 1, 7);
    auto header = getHeader(1);

    WriteBufferFromBrpcBuf out;
    NativeChunkOutputStream block_out(out, ClickHouseRevision::getVersionRevision(), header, false);
    block_out.write(origin_chunk);
    auto send_buf = out.getFinishedBuf();

    // dser
    ReadBufferFromBrpcBuf read_buffer(send_buf);
    NativeChunkInputStream chunk_in(read_buffer, header);
    Chunk chunk = chunk_in.readImpl();
    EXPECT_EQ(chunk.getNumRows(), 1000);
    auto col = chunk.getColumns().at(0);
    EXPECT_EQ(col->getUInt(1), 7);
}

void sender_thread(BrpcRemoteBroadcastSender & sender_, Chunk & chunk)
{
    sender_.waitAllReceiversReady(100 * 1000);
    BroadcastStatus status = sender_.send(std::move(chunk));
    // std::cout << "sender status " << status.code << "m:" << status.message << "ismodify:" << status.is_modifer;
    ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
}

TEST_F(ExchangeRemoteTest, RemoteNormalTest)
{
    ExchangeOptions exchange_options{.exhcange_timeout_ms = 1000};
    auto header = getHeader(1);
    auto data_key = std::make_shared<ExchangeDataKey>("q1", 1, 1, 1, "");

    // std::thread thread_server(start_brpc_server);
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    Chunk chunk = createUInt8Chunk(10, 1, 7);
    auto total_bytes = chunk.bytes();

    BrpcRemoteBroadcastSender sender(data_key, getContext().context, header);

    std::thread thread_sender(sender_thread, std::ref(sender), std::ref(chunk));

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    BrpcRemoteBroadcastReceiverShardPtr receiver
        = std::make_shared<BrpcRemoteBroadcastReceiver>(data_key, "127.0.0.1:8001", getContext().context, header);

    auto exchange_source = std::make_shared<ExchangeSource>(std::move(header), receiver, exchange_options);

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

    thread_sender.join();
    // thread_server.detach();
    executor.cancel();
}

/*
TEST_F(ExchangeRemoteTest, RemoteSenderLimitTest)
{
    ExchangeOptions exchange_options{.exhcange_timeout_ms = 200};
    auto header = getHeader(1);
    auto data_key = std::make_shared<ExchangeDataKey>("q1", 1, 1, 1, "");
    Chunk chunk = createUInt8Chunk(10, 1, 8);
    BrpcRemoteBroadcastSender sender(data_key, getContext().context, header);

    for (int i = 0; i < 5; i++)
    {
        Chunk clone = chunk.clone();
        std::thread thread_sender(sender_thread, std::ref(sender), std::ref(clone));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    BrpcRemoteBroadcastReceiverShardPtr receiver
        = std::make_shared<BrpcRemoteBroadcastReceiver>(data_key, "127.0.0.1:8001", getContext().context, header);

    auto exchange_source = std::make_shared<ExchangeSource>(std::move(header), receiver, exchange_options);
    QueryPipeline pipeline;

    Pipe pipe;
    pipe.addSource(exchange_source);

    pipe.addTransform(std::make_shared<LimitTransform>(exchange_source->getPort().getHeader(), 1, 0));

    pipeline.init(std::move(pipe));

    PullingAsyncPipelineExecutor executor(pipeline);
    Chunk pull_chunk;
    ASSERT_TRUE(executor.pull(pull_chunk));
    ASSERT_TRUE(pull_chunk.getNumRows() == 1);
    ASSERT_TRUE(executor.pull(pull_chunk));
    ASSERT_FALSE(executor.pull(pull_chunk));
    executor.cancel();
}
*/
