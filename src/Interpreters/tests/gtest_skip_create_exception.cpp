#include <memory>
#include "Common/Exception.h"
#include "Common/HostWithPorts.h"
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_utils.h>
#include <Protos/RPCHelpers.h>
#include <gtest/gtest.h>

using namespace DB;


class ExceptionHandlerWithFailedInfoMock : public ExceptionHandlerWithFailedInfo
{
public:
    bool setException(std::exception_ptr && exception)
    {
        exception_times++;
        return ExceptionHandlerWithFailedInfo::setException(std::move(exception));
    }

    std::atomic<int32_t> exception_times;
};

using ExceptionHandlerWithFailedInfoMockPtr = std::shared_ptr<ExceptionHandlerWithFailedInfoMock>;

void processCallback(ExceptionHandlerWithFailedInfoMockPtr handler)
{
    auto * response = new Protos::SendResourcesResp();
    brpc::Controller * cntl = new brpc::Controller;
    cntl->SetFailed(1, "Failed");
    DB::RPCHelpers::onAsyncCallDoneWithFailedInfo(response, cntl, handler, WorkerId());

}

TEST(SkipCreateExceptionTest, TestSkipException)
{
    std::vector<std::thread> threads;
    auto handler = std::make_shared<ExceptionHandlerWithFailedInfoMock>();
    for (size_t i = 0; i < 1000; i++)
    {
        threads.emplace_back(processCallback, handler);
    }

    for (auto & thread : threads)
        thread.join();
    std::cout << "times : "  << handler->exception_times << "\n";
    ASSERT_TRUE(handler->exception_times == 1);
}
