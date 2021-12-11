#include <memory>
#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/IBroadcastReceiver.h>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include <Processors/Exchange/DataTrans/Local/LocalBroadcastChannel.h>
#include <Processors/Exchange/DataTrans/Local/LocalBroadcastRegistry.h>
#include <Processors/Exchange/DataTrans/Local/LocalChannelOptions.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <Processors/Exchange/ExchangeSource.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/QueryPipeline.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/tests/gtest_utils.h>
#include "Processors/LimitTransform.h"

using namespace DB;
namespace UnitTest
{
TEST(ExchangeSource, LocalNormalTest)
{
    LocalChannelOptions options{10, 1000};
    ExchangeDataKey datakey{"", 1, 1, 1, ""};
    BroadcastSenderPtr local_sender = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsSender(datakey, options);
    BroadcastReceiverPtr local_receiver = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsReceiver(datakey, options);
    Chunk chunk = createUInt8Chunk(10, 1, 8);
    auto total_bytes = chunk.bytes();

    BroadcastStatus status = local_sender->send(std::move(chunk));
    ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);

    Block header = {ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "local_exchange_test")};

    auto exchange_source = std::make_shared<ExchangeSource>(std::move(header), local_receiver);
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


TEST(ExchangeSource, LocalSenderTimeoutTest)
{
    LocalChannelOptions options{10, 200};
    ExchangeDataKey datakey{"", 1, 1, 1, ""};
    BroadcastSenderPtr local_sender = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsSender(datakey, options);
    BroadcastReceiverPtr local_receiver = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsReceiver(datakey, options);
    Chunk chunk = createUInt8Chunk(10, 1, 8);

    for (int i = 0; i < 5; i++)
    {
        BroadcastStatus status = local_sender->send(chunk.clone());
        ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
    }

    Block header = {ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "local_exchange_test")};

    auto exchange_source = std::make_shared<ExchangeSource>(std::move(header), local_receiver);
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


TEST(ExchangeSource, LocalLimitTest)
{
    LocalChannelOptions options{10, 200};
    ExchangeDataKey datakey{"", 1, 1, 1, ""};
    BroadcastSenderPtr local_sender = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsSender(datakey, options);
    BroadcastReceiverPtr local_receiver = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsReceiver(datakey, options);
    Chunk chunk = createUInt8Chunk(10, 1, 8);

    for (int i = 0; i < 5; i++)
    {
        BroadcastStatus status = local_sender->send(chunk.clone());
        ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);
    }

    Block header = {ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "local_exchange_test")};

    auto exchange_source = std::make_shared<ExchangeSource>(std::move(header), local_receiver);
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

}
