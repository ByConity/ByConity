#include <gtest/gtest.h>
#include <TSO/TSOImpl.h>


using namespace DB::TSO;

namespace GtestTso
{
    using TSOServicePtr = std::shared_ptr<TSOImpl>;

    TEST(tso_impl, test_set_leader_host_port)
    {
        std::string leader_host_port = "127.0.0.1:9286";
        bool is_leader = true;

        TSOServicePtr tso_service = std::make_shared<TSOImpl>();
        tso_service->setLeaderElectionResult(leader_host_port, is_leader);

        auto result = tso_service->getLeaderElectionResult();
        EXPECT_EQ(result.leader_host_port, leader_host_port);
        EXPECT_EQ(result.is_leader, is_leader);
    }

    TEST(tso_impl, test_set_test_get_clock)
    {
        size_t physical_time_expected = 13;
        size_t logical_time_expected = 0;
        TSOServicePtr tso_service = std::make_shared<TSOImpl>();

        tso_service->setPhysicalTime(physical_time_expected);
        TSOClock cur_ts_actual = tso_service->getClock();

        EXPECT_EQ(cur_ts_actual.physical , physical_time_expected);
        EXPECT_EQ(cur_ts_actual.logical, logical_time_expected);
    }

} // end namespace