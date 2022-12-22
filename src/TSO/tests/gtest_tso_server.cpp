#include <gtest/gtest.h>
#include <TSO/TSOImpl.h>


using namespace DB::TSO;
using TSOServicePtr = std::shared_ptr<TSOImpl>;

TEST(TSOImpl, testSetGetClock)
{
    size_t physical_time_expected = 13;
    size_t logical_time_expected = 0;
    TSOServicePtr tso_service = std::make_shared<TSOImpl>();

    tso_service->setPhysicalTime(physical_time_expected);
    TSOClock cur_ts_actual = tso_service->getClock();

    EXPECT_EQ(cur_ts_actual.physical , physical_time_expected);
    EXPECT_EQ(cur_ts_actual.logical, logical_time_expected);
}
