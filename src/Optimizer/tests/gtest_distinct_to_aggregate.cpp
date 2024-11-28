#include <memory>
#include <Optimizer/Rule/Rewrite/DistinctToAggregate.h>
#include <Optimizer/Rule/Rule.h>
#include <Optimizer/tests/gtest_base_unittest_mock.h>
#include <Optimizer/tests/gtest_base_unittest_tester.h>
#include <Parsers/ASTFunction.h>
#include <QueryPlan/PlanPrinter.h>
#include <gtest/gtest.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include "QueryPlan/CTEInfo.h"

using namespace DB;
using namespace DB::CreateMockedPlanNode;

TEST(OptimizerUT, DistinctToAggregateTestWithLimit)
{
    OptimizerTester tester;
    auto context = tester.getContext();

    SizeLimits size_limit;
    PlanNodePtr input = PlanMocker(context).add(distinct(size_limit, 10, {"a"}, false)).add(limit(3)).add(values({"a", "b", "c"})).build();

    auto rule_ptr = std::make_shared<DistinctToAggregate>();

    tester.assertNotFire(rule_ptr, input);
}

TEST(OptimizerUT, DistinctToAggregateTestWithOutLimit)
{
    OptimizerTester tester;
    auto context = tester.getContext();

    PlanNodePtr input = PlanMocker(context).add(distinct({"a"})).add(limit(3)).add(values({"a", "b", "c"})).build();
    PlanNodePtr output = PlanMocker(context).add(aggregating({"a"})).add(limit(3)).add(values({"a", "b", "c"})).build();

    auto rule_ptr = std::make_shared<DistinctToAggregate>();

    auto plan = tester.plan(rule_ptr, input);
    tester.assertMatch(plan, output);
}
