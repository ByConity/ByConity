#include <QueryPlan/PlanPrinter.h>
#include <Optimizer/tests/gtest_base_tpch_plan_test.h>

#include <gtest/gtest.h>

using namespace DB;
using namespace std::string_literals;

class PlanCheckTpch1000NotShowStats : public ::testing::Test
{
public:
    static void SetUpTestSuite()
    {
        auto settings = BasePlanTest::getDefaultOptimizerSettings();
        tester = std::make_shared<DB::BaseTpchPlanTest>(settings, 1000);
        tester->setShowStatistics(false);
        tester->setLabel("not_show_stats");
    }

    static void TearDownTestSuite()
    {
        tester.reset();
    }

    static std::string explain(const std::string & name)
    {
        return tester->explain(name);
    }

    static std::string expected(const std::string & name)
    {
        return tester->loadExplain(name);
    }

    static testing::AssertionResult equals(const std::string & actual, const std::string & expected)
    {
        if (actual == expected)
            return testing::AssertionSuccess();
        else
            return testing::AssertionFailure() << "\nExpected:\n" << expected << "\nActual:\n" << actual;
    }

    static std::shared_ptr<DB::BaseTpchPlanTest> tester;
};

std::shared_ptr<DB::BaseTpchPlanTest> PlanCheckTpch1000NotShowStats::tester;

DECLARE_GENERATE_TEST(PlanCheckTpch1000NotShowStats)

TEST_F(PlanCheckTpch1000NotShowStats, q1)
{
    EXPECT_TRUE(equals(explain("q1"), expected("q1")));
}

TEST_F(PlanCheckTpch1000NotShowStats, q2)
{
    EXPECT_TRUE(equals(explain("q2"), expected("q2")));
}

TEST_F(PlanCheckTpch1000NotShowStats, q3)
{
    EXPECT_TRUE(equals(explain("q3"), expected("q3")));
}

TEST_F(PlanCheckTpch1000NotShowStats, q4)
{
    EXPECT_TRUE(equals(explain("q4"), expected("q4")));
}

TEST_F(PlanCheckTpch1000NotShowStats, q5)
{
    EXPECT_TRUE(equals(explain("q5"), expected("q5")));
}

TEST_F(PlanCheckTpch1000NotShowStats, q6)
{
    EXPECT_TRUE(equals(explain("q6"), expected("q6")));
}

TEST_F(PlanCheckTpch1000NotShowStats, q7)
{
    EXPECT_TRUE(equals(explain("q7"), expected("q7")));
}

TEST_F(PlanCheckTpch1000NotShowStats, q8)
{
    EXPECT_TRUE(equals(explain("q8"), expected("q8")));
}

TEST_F(PlanCheckTpch1000NotShowStats, q9)
{
    EXPECT_TRUE(equals(explain("q9"), expected("q9")));
}

TEST_F(PlanCheckTpch1000NotShowStats, q10)
{
    EXPECT_TRUE(equals(explain("q10"), expected("q10")));
}

TEST_F(PlanCheckTpch1000NotShowStats, q11)
{
    EXPECT_TRUE(equals(explain("q11"), expected("q11")));
}

TEST_F(PlanCheckTpch1000NotShowStats, q12)
{
    EXPECT_TRUE(equals(explain("q12"), expected("q12")));
}

TEST_F(PlanCheckTpch1000NotShowStats, q13)
{
    EXPECT_TRUE(equals(explain("q13"), expected("q13")));
}

TEST_F(PlanCheckTpch1000NotShowStats, q14)
{
    EXPECT_TRUE(equals(explain("q14"), expected("q14")));
}

TEST_F(PlanCheckTpch1000NotShowStats, q15)
{
    EXPECT_TRUE(equals(explain("q15"), expected("q15")));
}

TEST_F(PlanCheckTpch1000NotShowStats, q16)
{
    EXPECT_TRUE(equals(explain("q16"), expected("q16")));
}

TEST_F(PlanCheckTpch1000NotShowStats, q17)
{
    EXPECT_TRUE(equals(explain("q17"), expected("q17")));
}

TEST_F(PlanCheckTpch1000NotShowStats, q18)
{
    EXPECT_TRUE(equals(explain("q18"), expected("q18")));
}

TEST_F(PlanCheckTpch1000NotShowStats, q19)
{
    EXPECT_TRUE(equals(explain("q19"), expected("q19")));
}

TEST_F(PlanCheckTpch1000NotShowStats, q20)
{
    EXPECT_TRUE(equals(explain("q20"), expected("q20")));
}

TEST_F(PlanCheckTpch1000NotShowStats, q21)
{
    EXPECT_TRUE(equals(explain("q21"), expected("q21")));
}

TEST_F(PlanCheckTpch1000NotShowStats, q22)
{
    EXPECT_TRUE(equals(explain("q22"), expected("q22")));
}
