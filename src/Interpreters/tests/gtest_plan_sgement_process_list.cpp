#include <string>
#include <thread>
#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <gtest/gtest.h>
#include <Poco/ConsoleChannel.h>
#include <common/scope_guard.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_utils.h>


using namespace DB;

namespace UnitTest
{

TEST(PlanSegmentProcessList, InsertTest)
{
    initLogger();
    const auto & context = getContext().context;
    auto & client_info = context->getClientInfo();
    auto & plan_segment_processlist = context->getPlanSegmentProcessList();
    PlanSegment plan_segment = PlanSegment();
    plan_segment.setQueryId("PlanSegmentProcessList_test");
    plan_segment.setPlanSegmentId(0);

    client_info.current_query_id = plan_segment.getQueryId() + std::to_string(plan_segment.getPlanSegmentId());
    client_info.current_user = "test";
    client_info.initial_query_id = plan_segment.getQueryId();
    AddressInfo coodinator_address("localhost", 8888, "test", "123456", 9999, 6666);
    plan_segment.setCoordinatorAddress(coodinator_address);
    plan_segment_processlist.insert(plan_segment, context);
}

TEST(PlanSegmentProcessList, InsertReplaceSuccessTest)
{
    Poco::Logger::root().setLevel("trace");
    Poco::Logger::root().setChannel(Poco::AutoPtr<Poco::ConsoleChannel>(new Poco::ConsoleChannel()));
    const auto & context = getContext().context;
    auto & client_info = context->getClientInfo();
    auto & plan_segment_processlist = context->getPlanSegmentProcessList();
    PlanSegment plan_segment = PlanSegment();
    plan_segment.setQueryId("PlanSegmentProcessList_test");
    plan_segment.setPlanSegmentId(0);

    client_info.current_query_id = plan_segment.getQueryId() + std::to_string(plan_segment.getPlanSegmentId());
    client_info.current_user = "test";
    client_info.initial_query_id = plan_segment.getQueryId();
    AddressInfo coodinator_address("localhost", 8888, "test", "123456", 9999, 6666);
    plan_segment.setCoordinatorAddress(coodinator_address);
    auto plan_segment_process_entry = plan_segment_processlist.insert(plan_segment, context);
    auto async_func = [to_release_entry = std::move(plan_segment_process_entry)]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        to_release_entry.get();
    };
    std::thread thread(std::move(async_func));
    SCOPE_EXIT({
        if (thread.joinable())
            thread.join();
    });
    plan_segment.setCoordinatorAddress(AddressInfo("localhost", 8888, "test", "123456", 6666, 9999));
    plan_segment_processlist.insert(plan_segment, context, true);
}

TEST(PlanSegmentProcessList, InsertReplaceTimeoutTest)
{
    const auto & context = getContext().context;
    auto & client_info = context->getClientInfo();
    auto & plan_segment_processlist = context->getPlanSegmentProcessList();
    PlanSegment plan_segment = PlanSegment();
    plan_segment.setQueryId("PlanSegmentProcessList_test");
    plan_segment.setPlanSegmentId(0);

    client_info.current_query_id = plan_segment.getQueryId() + std::to_string(plan_segment.getPlanSegmentId());
    client_info.current_user = "test";
    client_info.initial_query_id = plan_segment.getQueryId();
    AddressInfo coodinator_address("localhost", 8888, "test", "123456", 9999, 6666);
    plan_segment.setCoordinatorAddress(coodinator_address);
    auto plan_segment_process_entry = plan_segment_processlist.insert(plan_segment, context);

    auto async_func = [&, to_release_entry = std::move(plan_segment_process_entry)]() {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(context->getSettingsRef().replace_running_query_max_wait_ms.totalMilliseconds() + 500));
        to_release_entry.get();
    };
    std::thread thread(std::move(async_func));
    SCOPE_EXIT({
        if (thread.joinable())
            thread.join();
    });

    plan_segment.setCoordinatorAddress(AddressInfo("localhost", 8888, "test", "123456", 6666, 9999));
    ASSERT_THROW(plan_segment_processlist.insert(plan_segment, context, true), DB::Exception);
}

}
