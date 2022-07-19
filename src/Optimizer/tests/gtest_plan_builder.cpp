#include <Optimizer/PlanNodeSearcher.h>
#include <Optimizer/tests/gtest_base_plan_test.h>
#include <Parsers/ASTSetQuery.h>
#include <QueryPlan/AggregatingStep.h>
#include <QueryPlan/DistinctStep.h>
#include <QueryPlan/PlanNode.h>

#include <gtest/gtest.h>

using namespace DB;

TEST(OptimizerPlanBuilder, GroupByLiteralToSymbol)
{
    // no need to set enable_replace_group_by_literal_to_symbol as dialect_type is set;
    BasePlanTest test;
    auto plan = test.plan("select a, sum(b) from (select 1 a, 2 b) group by 1", test.createQueryContext());
    auto agg = PlanNodeSearcher::searchFrom(plan->getPlanNode())
                   .where([](auto & node) { return node.getStep()->getType() == IQueryPlanStep::Type::Aggregating; })
                   .findFirst();
    ASSERT_TRUE(agg.has_value());

    const auto * agg_step = dynamic_cast<const AggregatingStep *>(agg.value()->getStep().get());
    ASSERT_EQ(agg_step->getKeys().size(), 1ul);
}

TEST(OptimizerPlanBuilder, DistinctToAggregate)
{
    BasePlanTest test;
    auto plan = test.plan("select distinct a from (select 1 a union select 2)", test.createQueryContext());

    auto distinct = PlanNodeSearcher::searchFrom(plan->getPlanNode())
                        .where([](auto & node) { return node.getStep()->getType() == IQueryPlanStep::Type::Distinct; })
                        .findFirst();
    ASSERT_FALSE(distinct.has_value());
    auto agg = PlanNodeSearcher::searchFrom(plan->getPlanNode())
                   .where([](auto & node) { return node.getStep()->getType() == IQueryPlanStep::Type::Aggregating; })
                   .findFirst();
    ASSERT_TRUE(agg.has_value());
    const auto * agg_step = dynamic_cast<const AggregatingStep *>(agg.value()->getStep().get());
    ASSERT_EQ(agg_step->getKeys().size(), 1ul);
}

TEST(OptimizerPlanBuilder, DistinctLimitHint)
{
    BasePlanTest test;
    auto plan = test.plan("select distinct a from (select 1 a union select 2) limit 1", test.createQueryContext());

    auto distinct = PlanNodeSearcher::searchFrom(plan->getPlanNode())
                        .where([](auto & node) { return node.getStep()->getType() == IQueryPlanStep::Type::Distinct; })
                        .findFirst();
    ASSERT_TRUE(distinct.has_value());

    const auto * distinct_step = dynamic_cast<const DistinctStep *>(distinct.value()->getStep().get());
    ASSERT_EQ(distinct_step->getLimitHint(), 1ul);
}

TEST(OptimizerPlanBuilder, DistinctLimitHint2)
{
    // limit don't push into distinct
    BasePlanTest test;
    auto plan = test.plan("select distinct a, b from (select 1 a, 2 b limit 1) limit 3", test.createQueryContext());

    auto distinct = PlanNodeSearcher::searchFrom(plan->getPlanNode())
                        .where([](auto & node) { return node.getStep()->getType() == IQueryPlanStep::Type::Distinct; })
                        .findFirst();
    ASSERT_FALSE(distinct.has_value());
    auto agg = PlanNodeSearcher::searchFrom(plan->getPlanNode())
                   .where([](auto & node) { return node.getStep()->getType() == IQueryPlanStep::Type::Aggregating; })
                   .findFirst();
    ASSERT_TRUE(agg.has_value());
    const auto * agg_step = dynamic_cast<const AggregatingStep *>(agg.value()->getStep().get());
    ASSERT_EQ(agg_step->getKeys().size(), 2ul);
}
