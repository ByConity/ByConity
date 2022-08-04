#include <QueryPlan/PlanPrinter.h>
#include <Optimizer/tests/gtest_correctness_plan_test.h>

#include <gtest/gtest.h>

using namespace DB;

class CorrectnessPlanTesting : public ::testing::Test
{
public:
    static void SetUpTestSuite()
    {
        std::unordered_map<std::string, DB::Field> settings;
        tester = std::make_shared<DB::CorrectnessPlanTest>(settings);
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

    static std::shared_ptr<DB::CorrectnessPlanTest> tester;
};

std::shared_ptr<DB::CorrectnessPlanTest> CorrectnessPlanTesting::tester;

TEST_F(CorrectnessPlanTesting, generate)
{
    if (!AbstractPlanTestSuite::enforce_regenerate())
        GTEST_SKIP() << "skip generate. set env REGENERATE=1 to regenerate explains.";

    for (auto & query : tester->loadQueries())
    {
        try
        {
            std::cout << " try generate for " + query + "." << std::endl;
            tester->saveExplain(query, explain(query));
        }
        catch (...)
        {
            std::cerr << " generate for " + query + " failed." << std::endl;
            tester->saveExplain(query, "");
        }
    }
}

TEST_F(CorrectnessPlanTesting, q1)
{
    EXPECT_TRUE(equals(explain("q1"), expected("q1")));
}

TEST_F(CorrectnessPlanTesting, q2)
{
    EXPECT_TRUE(equals(explain("q2"), expected("q2")));
}
