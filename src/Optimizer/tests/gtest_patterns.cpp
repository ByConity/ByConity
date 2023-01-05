#include <Optimizer/tests/gtest_optimizer_test_utils.h>

#include <gtest/gtest.h>

using namespace DB;
using namespace DB::Patterns;

TEST(OptimizerPatternsTest, GetPatternTargetType)
{
    PatternPtr pattern1 = filter();
    PatternPtr pattern2 = join()->withSingle(filter()->withSingle(tableScan()));

    ASSERT_TRUE(pattern1->getTargetType() == IQueryPlanStep::Type::Filter);
    ASSERT_TRUE(pattern2->getTargetType() == IQueryPlanStep::Type::Join);
}

TEST(OptimizerPatternsTest, PredicateNot)
{
    PatternPredicate t = [](const PlanNodePtr &, const Captures &) { return true; };
    PatternPredicate f = predicateNot(t);
    PlanNodePtr node;
    Captures captures;
    ASSERT_TRUE(t(node, captures));
    ASSERT_TRUE(!f(node, captures));
}

TEST(OptimizerPatternsTest, ToString)
{
    Capture cap1;
    PatternPtr pattern
        = Patterns::any()->matching([](const auto &, const auto &) { return true; })->capturedAs(cap1)->withSingle(Patterns::any());

    std::cout << pattern->toString() << std::endl;
}
