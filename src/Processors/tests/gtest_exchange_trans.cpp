#include <thread>
#include <gtest/gtest.h>

#include <chrono>
#include <random>
#include <variant>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Compression/CompressedReadBuffer.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcExchangeReceiverRegistryService.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcRemoteBroadcastReceiver.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcRemoteBroadcastSender.h>
#include <Processors/Exchange/DataTrans/Brpc/ReadBufferFromBrpcBuf.h>
#include <Processors/Exchange/DataTrans/Brpc/WriteBufferFromBrpcBuf.h>
#include <Processors/Exchange/DataTrans/NativeChunkInputStream.h>
#include <Processors/Exchange/DataTrans/NativeChunkOutputStream.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <brpc/server.h>
#include <Common/ClickHouseRevision.h>
#include <Common/tests/gtest_global_context.h>

using namespace DB;

using Clock = std::chrono::system_clock;

static const size_t TOTAL_SIZE_IN_BYTES = 512000;
static const size_t SIZE_OF_ROW_IN_BYTES = 512;
static const size_t TOTAL_ROW_NUM = TOTAL_SIZE_IN_BYTES / SIZE_OF_ROW_IN_BYTES;

static Chunk getChunkWithSize(size_t size_of_row_in_bytes, size_t row_num)
{
    Columns columns;
    for (size_t i = 0; i < size_of_row_in_bytes; i += sizeof(UInt64))
    {
        auto col = ColumnUInt8::create(row_num, 7);
        columns.emplace_back(std::move(col));
    }
    return Chunk(std::move(columns), row_num);
}

static Block getHeader(size_t size_of_row_in_bytes)
{
    ColumnsWithTypeAndName columns;
    for (size_t i = 0; i < size_of_row_in_bytes; i += sizeof(UInt64))
    {
        columns.push_back(ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "column" + std::to_string(i)));
    }
    Block header = {columns};
    return header;
}

void start_brpc_server()
{
    const auto & context = getContext().context;
    brpc::Server server;
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
    sleep(10000);
    // server.RunUntilAskedToQuit();
}

void receiver1()
{
    auto receiver_data = std::make_shared<ExchangeDataKey>("q1", 1, 1, 1, "");
    Block header = getHeader(TOTAL_SIZE_IN_BYTES / SIZE_OF_ROW_IN_BYTES);
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
    Block header = getHeader(TOTAL_SIZE_IN_BYTES / SIZE_OF_ROW_IN_BYTES);
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

TEST(Exchange, SendWithTwoReceivers)
{
    auto receiver_data1 = std::make_shared<ExchangeDataKey>("q1", 1, 1, 1, "");
    auto receiver_data2 = std::make_shared<ExchangeDataKey>("q1", 1, 1, 2, "");

    auto origin_chunk = getChunkWithSize(SIZE_OF_ROW_IN_BYTES, TOTAL_ROW_NUM);
    auto header = getHeader(TOTAL_ROW_NUM);
    std::thread thread_server(start_brpc_server);
    // sleep for a while waiting for server
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    std::thread thread_receiver1(receiver1);
    std::thread thread_receiver2(receiver2);

    BrpcRemoteBroadcastSender sender({receiver_data1, receiver_data2}, getContext().context, header);
    sender.waitAllReceiversReady(100 * 1000);
    sender.send(std::move(origin_chunk));
    thread_receiver1.join();
    thread_receiver2.join();
    thread_server.detach();
}

TEST(Exchange, SerDserChunk)
{
    // ser
    auto origin_chunk = getChunkWithSize(SIZE_OF_ROW_IN_BYTES, TOTAL_ROW_NUM);
    auto header = getHeader(SIZE_OF_ROW_IN_BYTES);

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
