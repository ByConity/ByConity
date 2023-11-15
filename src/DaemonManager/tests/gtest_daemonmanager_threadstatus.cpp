#include <DaemonManager/DaemonManagerThreadStatus.h>
#include <Common/CurrentThread.h>
#include <string>
#include <gtest/gtest.h>

using namespace DB::DaemonManager;
using namespace DB;

namespace DaemonManagerThreadStatusNs
{

TEST(DaemonManagerThreadStatusTest, test)
{
    DaemonManagerThreadStatus thread_status;
    thread_status.setQueryID("hello");
    String query_id = thread_status.getQueryId().toString();
    EXPECT_EQ(query_id, "hello");
    EXPECT_EQ(CurrentThread::getQueryId(), "hello");
}

} // end namespace
