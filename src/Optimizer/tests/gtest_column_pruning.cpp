#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Optimizer/PlanNodeSearcher.h>
#include <Optimizer/PlanOptimizer.h>
#include <Optimizer/Rewriter/ColumnPruning.h>
#include <Optimizer/tests/test_config.h>
#include <QueryPlan/QueryPlan.h>
#include <gtest/gtest.h>
#include "gtest_base_plan_test.h"

#include <boost/asio.hpp>

#include <algorithm>
#include <filesystem>
#include <memory>

using namespace DB;

TEST(OptimizerColumnPruning, JoinOutputsNotRequired)
{
    BasePlanTest test;
    auto context = test.createQueryContext();
    auto plan = test.plan("select a from (select 1 a), (select b from (select 2 b), (select c from (select 3 c), (select 4 d)));", context);
    PlanOptimizer::optimize(*plan, context, Rewriters{{std::make_shared<ColumnPruning>()}});

    auto joins = PlanNodeSearcher::searchFrom(plan->getPlanNode())
                     .where([](auto & node) { return node.getStep()->getType() == IQueryPlanStep::Type::Join; })
                     .findAll();

    for (const auto & join : joins)
    {
        auto outputs = join->getStep()->getOutputStream().getNamesToTypes();
        auto left_inputs = join->getStep()->getInputStreams()[0].getNamesToTypes();
        auto right_inputs = join->getStep()->getInputStreams()[1].getNamesToTypes();

        EXPECT_TRUE(std::none_of(left_inputs.begin(), left_inputs.end(), [&](const auto & left) { return right_inputs.count(left.first); }))
            << "no duplicate symbols in left and right";

        EXPECT_TRUE(std::all_of(outputs.begin(), outputs.end(), [&](const auto & o) {
            return left_inputs.count(o.first) || right_inputs.count(o.first);
        })) << "output symbol must exist in left or right";
    }
}

TEST(OptimizerColumnPruning, JoinOutputsNotRequired2)
{
    BasePlanTest test;
    auto context = test.createQueryContext();
    auto plan = test.plan("select 'a' from (select 1 a), (select 'b' from (select 2 b), (select 'c' from (select 3 c), (select 4 d)));", context);
    PlanOptimizer::optimize(*plan, context, Rewriters{{std::make_shared<ColumnPruning>()}});

    auto joins = PlanNodeSearcher::searchFrom(plan->getPlanNode())
                     .where([](auto & node) { return node.getStep()->getType() == IQueryPlanStep::Type::Join; })
                     .findAll();

    for (const auto & join : joins)
    {
        auto outputs = join->getStep()->getOutputStream().getNamesToTypes();
        auto left_inputs = join->getStep()->getInputStreams()[0].getNamesToTypes();
        auto right_inputs = join->getStep()->getInputStreams()[1].getNamesToTypes();

        EXPECT_TRUE(std::none_of(left_inputs.begin(), left_inputs.end(), [&](const auto & left) { return right_inputs.count(left.first); }))
            << "no duplicate symbols in left and right";

        EXPECT_TRUE(std::all_of(outputs.begin(), outputs.end(), [&](const auto & o) {
            return left_inputs.count(o.first) || right_inputs.count(o.first);
        })) << "output symbol must exist in left or right";
    }
}

