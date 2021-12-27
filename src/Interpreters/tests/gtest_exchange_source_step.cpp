#include <memory>
#include <string>
#include <thread>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include <Processors/Exchange/DataTrans/Local/LocalBroadcastRegistry.h>
#include <Processors/Exchange/DataTrans/Local/LocalChannelOptions.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/QueryPipeline.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/RemoteExchangeSourceStep.h>
#include <Processors/tests/gtest_processers_utils.h>
#include <gtest/gtest.h>
#include <Poco/ConsoleChannel.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_utils.h>
#include <common/scope_guard.h>


using namespace DB;

namespace UnitTest
{
TEST(RemoteExchangeSourceStep, InitializePipelineTest)
{
    initLogger();
    const auto & context = getContext().context;
    auto & client_info = context->getClientInfo();
    PlanSegment plan_segment = PlanSegment();
    plan_segment.setQueryId("RemoteExchangeSourceStep_test");
    plan_segment.setPlanSegmentId(2);
    plan_segment.setContext(context);

    client_info.current_query_id = plan_segment.getQueryId() + std::to_string(plan_segment.getPlanSegmentId());
    client_info.current_user = "test";
    client_info.initial_query_id = plan_segment.getQueryId();
    AddressInfo coodinator_address("localhost", 8888, "test", "123456", 9999, 6666);
    auto coodinator_address_str = extractExchangeStatusHostPort(coodinator_address);
    plan_segment.setCoordinatorAddress(coodinator_address);

    Block header = {ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "local_exchange_test")};

    PlanSegmentInputs inputs;
    for (int i = 1; i <= 2; ++i)
    {
        auto input = std::make_shared<PlanSegmentInput>(header, PlanSegmentType::SOURCE);
        input->setParallelIndex(i);
        input->setExchangeParallelSize(1);
        input->setPlanSegmentId(1);
        inputs.push_back(input);
    }


    DataStream datastream{.header = header};
    RemoteExchangeSourceStep exchange_source_step(inputs, datastream);
    exchange_source_step.setPlanSegment(&plan_segment);

    ExchangeOptions exchange_options{.exhcange_timeout_ms = 1000, .send_threshold_in_bytes = 0, .local_debug_mode = true};
    exchange_source_step.setExchangeOptions(exchange_options);
    LocalChannelOptions options{10, exchange_options.exhcange_timeout_ms, 1};
    ExchangeDataKey datakey_1{plan_segment.getQueryId(), 1, 2, 1, coodinator_address_str};
    auto local_sender_1 = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsSender(datakey_1, options);

    ExchangeDataKey datakey_2{plan_segment.getQueryId(), 1, 2, 2, coodinator_address_str};
    auto local_sender_2 = LocalBroadcastRegistry::getInstance().getOrCreateChannelAsSender(datakey_2, options);
    Chunk chunk = createUInt8Chunk(10, 1, 8);
    auto total_bytes = chunk.bytes();

    BroadcastStatus status = local_sender_1->send(std::move(chunk));
    ASSERT_TRUE(status.code == BroadcastStatusCode::RUNNING);

    QueryPipeline pipeline;
    exchange_source_step.initializePipeline(pipeline, BuildQueryPipelineSettings::fromContext(context));

    PullingAsyncPipelineExecutor executor(pipeline);
    Chunk pull_chunk;
    ASSERT_TRUE(executor.pull(pull_chunk));
    ASSERT_TRUE(pull_chunk.getNumRows() == 10);
    ASSERT_TRUE(pull_chunk.bytes() == total_bytes);
    
    executor.cancel();
}

}
