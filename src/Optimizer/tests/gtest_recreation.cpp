#include <QueryPlan/PlanPrinter.h>
#include <gtest/gtest.h>
#include <Optimizer/tests/gtest_base_dump_plan_test.h>
static constexpr bool WITH_STATISTICS = true;

class TpcdsPlanDumpWithStatisticsTest : public ::testing::Test
{
public:
    static void SetUpTestSuite()
    {
        std::unordered_map<std::string, DB::Field> settings;
#ifndef NDEBUG
        // debug mode may time out.
        settings.emplace("cascades_optimizer_timeout", "300000");
#endif

        tester = std::make_shared<DB::BaseDumpPlanTest>(WITH_STATISTICS, settings);
    }

    static std::string reproduce(const std::string & query_id) { return tester->reproduce(query_id); }

    static std::string expected(const String & query_id) { return tester->loadExplainFromPath(query_id); }

    static std::string dump(const std::string name) { return tester->dump(name); }

    static testing::AssertionResult checkDump(const std::string & query_id)
    {
        std::vector<std::string> check_res = tester->checkDump(query_id, "test_dump");
        tester->cleanQueryFiles(query_id);
        if (check_res[0] == "true")
            return testing::AssertionSuccess();
        else
            return testing::AssertionFailure() << "\nExpected:\n" << check_res[0] << "\nActual:\n" << check_res[1];
    }

    static testing::AssertionResult equals(const std::string & actual, const std::string & expected)
    {
        if (actual == expected)
            return testing::AssertionSuccess();
        else
            return testing::AssertionFailure() << "\nExpected:\n" << expected << "\nActual:\n" << actual;
    }

    static std::shared_ptr<DB::BaseDumpPlanTest> tester;
};

std::shared_ptr<DB::BaseDumpPlanTest> TpcdsPlanDumpWithStatisticsTest::tester;


TEST_F(TpcdsPlanDumpWithStatisticsTest, dump)
{
    std::string query_id = dump("test");
    EXPECT_TRUE(checkDump(query_id));
}
TEST_F(TpcdsPlanDumpWithStatisticsTest, reproduce)
{
//    tester->unZip("reproduce");
    EXPECT_TRUE(equals(reproduce("reproduce"), expected("reproduce")));
}
