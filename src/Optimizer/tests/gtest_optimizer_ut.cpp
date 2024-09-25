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
#include "Optimizer/PlanNodeSearcher.h"
#include "Optimizer/Rewriter/ColumnPruning.h"
#include "Optimizer/Rewriter/PredicatePushdown.h"
#include "QueryPlan/CTEInfo.h"
#include "QueryPlan/TableScanStep.h"
#include <Optimizer/Rule/Rewrite/PushPartialStepThroughExchangeRules.h>
#include "Optimizer/Rewriter/AddBufferForDeadlockCTE.h"

using namespace DB;
using namespace CreateMockedPlanNode;

TEST(OptimizerUT, Settings1)
{
    OptimizerTester tester({{"enable_push_storage_filter", "false"}});
    auto context = tester.getContext();

    tester.registerTable(R"(
        CREATE TABLE customer_address1 (
            ca_address_sk Int64,
            ca_address_id Nullable(String)
        ) ENGINE = CnchMergeTree() 
        ORDER BY ca_address_sk;
    )");

    auto plan = tester.plan("select ca_address_sk from customer_address1 where ca_address_id in ('a', 'b', 'c')");
    auto table_scan_node = PlanNodeSearcher::searchFrom(plan->getPlanNode())
                               .where([](auto & node) { return node.getStep()->getType() == IQueryPlanStep::Type::TableScan; })
                               .findSingle()
                               .value();
    auto & table_scan_step = static_cast<const TableScanStep &>(*table_scan_node->getStep());
    EXPECT_TRUE(table_scan_step.getPushdownFilter() == nullptr);

    tester.printPlan(tester.plan("select ca_address_sk from customer_address1 where ca_address_id in ('a', 'b', 'c')"));
    tester.assertNotContains(
        tester.plan("select ca_address_sk from customer_address1 where ca_address_id in ('a', 'b', 'c')"), R"(ca_address_sk_1)");
}


TEST(OptimizerUT, PredicatePushdown)
{
    OptimizerTester tester;
    auto context = tester.getContext();

    PlanNodePtr input = PlanMocker(context)
                            .add(projection())
                            .add(filter(makeASTFunction("less", std::make_shared<ASTIdentifier>("a"), std::make_shared<ASTLiteral>(3))))
                            .add(join({"a"}, {"b"}))
                            .addChildren(
                                PlanMocker(context)
                                    .add(join({"a"}, {"c"}))
                                    .addChildren(
                                        PlanMocker(context).add(aggregating({"a"})).add(values({"a", "a1"})),
                                        PlanMocker(context).add(values({"c", "c3"}))),
                                PlanMocker(context).add(values({"b", "b2"})))
                            .build();

    auto rule_ptr = std::make_shared<PredicatePushdown>();

    PlanNodePtr output
        = PlanMocker(context)
              .add(projection())
              .add(join({"a"}, {"b"}))
              .addChildren(
                  PlanMocker(context)
                      .add(join({"a"}, {"c"}))
                      .addChildren(
                          PlanMocker(context)
                              .add(aggregating({"a"}))
                              .add(filter(makeASTFunction("less", std::make_shared<ASTIdentifier>("a"), std::make_shared<ASTLiteral>(3))))
                              .add(values({"a", "a1"})),
                          PlanMocker(context)
                              .add(filter(makeASTFunction("less", std::make_shared<ASTIdentifier>("c"), std::make_shared<ASTLiteral>(3))))
                              .add(values({"c", "c3"}))),
                  PlanMocker(context)
                      .add(filter(makeASTFunction("less", std::make_shared<ASTIdentifier>("b"), std::make_shared<ASTLiteral>(3))))
                      .add(values({"b", "b2"})))
              .build();

    tester.assertMatch(tester.plan(rule_ptr, input), output);
}

TEST(OptimizerUT, ImplicitArgConvert)
{
    OptimizerTester tester({{"enable_implicit_arg_type_convert", "true"}});
    auto context = tester.getContext();

    tester.registerTable("CREATE TABLE aeolus_data_table_2_2516793_prod ( `row_id_kmtq3k` Int64, `p_date` Date) ENGINE = CnchMergeTree() "
                         "PARTITION BY p_date ORDER BY (row_id_kmtq3k, intHash64(row_id_kmtq3k));");

    tester.plan("SELECT * FROM aeolus_data_table_2_2516793_prod"
                " WHERE p_date = (SELECT toString(max(p_date)) FROM aeolus_data_table_2_2516793_prod)");
}

TEST(OptimizerUT, LackOfShuffleKeys)
{
    OptimizerTester tester({{"enable_implicit_arg_type_convert", "true"}});
    auto context = tester.getContext();

    PlanNodePtr input = PlanMocker(context)
                            .add(projection({"a"}, true))
                            .add(exchange(ExchangeMode::REPARTITION, Partitioning({"b"})))
                            .add(projection())
                            .add(values({"a", "b"}))
                            .build();

    auto plan = tester.plan(std::make_shared<ColumnPruning>(), input);

    tester.assertContains(plan, "Expressions: [a, b]");
}


TEST(OptimizerUT, PushPartialAggThroughUnion)
{
    OptimizerTester tester;
    auto context = tester.getContext();

    PlanNodePtr input
        = PlanMocker(context)
              .add(aggregating({"a1"}, {}, false))
              .add(unionn())
              .addChildren(
                  PlanMocker(context).add(projection()).add(values({"a1"})), PlanMocker(context).add(projection()).add(values({"a2"})), PlanMocker(context).add(projection()).add(values({"a2"})))
              .build();

    auto plan = tester.plan(std::make_shared<PushPartialAggThroughUnion>(), input);
    EXPECT_EQ(PlanNodeSearcher::searchFrom(*plan).where([](auto & node) { return node.getStep()->getType() == IQueryPlanStep::Type::Aggregating; }).count(), 3);
}


TEST(OptimizerUT, CTEBase)
{
    OptimizerTester tester;
    auto context = tester.getContext();

    CTEInfo cte_info;
    PlanNodePtr cte1 = PlanMocker(context)
                           .add(projection())
                           .add(filter(makeASTFunction("equals", std::make_shared<ASTIdentifier>("c"), std::make_shared<ASTLiteral>(10))))
                           .add(values({"a", "b", "c"}))
                           .buildWithCTE(cte_info);

    PlanNodePtr cte2 = PlanMocker(context)
                           .add(projection())
                           .add(filter(makeASTFunction("equals", std::make_shared<ASTIdentifier>("f"), std::make_shared<ASTLiteral>(11))))
                           .add(values({"d", "e", "f"}))
                           .buildWithCTE(cte_info);

    auto node1 = PlanMocker(context)
                     .add(projection())
                     .add(join({"d"}, {"a"}))
                     .addChildren(PlanMocker(context).add(cte(cte2)), PlanMocker(context).add(cte(cte1)));

    auto node2 = PlanMocker(context)
                     .add(projection())
                     .add(join({"b"}, {"e"}))
                     .addChildren(PlanMocker(context).add(cte(cte1)), PlanMocker(context).add(cte(cte2)));

    PlanNodePtr input = PlanMocker(context)
                            .add(projection())
                            .add(filter(makeASTFunction("less", std::make_shared<ASTIdentifier>("a"), std::make_shared<ASTLiteral>(3))))
                            .add(join({"d"}, {"b"}))
                            .addChildren(node1, node2)
                            .buildWithCTE(cte_info);

    auto plan0 = tester.plan(Rewriters{}, input, cte_info);
    EXPECT_EQ(
        PlanNodeSearcher::searchFrom(*plan0)
            .where([](auto & node) { return node.getStep()->getType() == IQueryPlanStep::Type::Buffer; })
            .count(),
        0);

    auto rule = std::make_shared<AddBufferForDeadlockCTE>();
    auto plan = tester.plan(rule, input, cte_info);
    EXPECT_EQ(
        PlanNodeSearcher::searchFrom(*plan)
            .where([](auto & node) { return node.getStep()->getType() == IQueryPlanStep::Type::Buffer; })
            .count(),
        4);

    tester.registerTable(R"(
        CREATE TABLE base (
            a Int64,
            b Int64,
            c Int64
        ) ENGINE = CnchMergeTree() 
        ORDER BY a;
        )");
    auto plan2 = tester.plan(
        "with cte1 as (select a, b, c from base), cte2 as (select a as d, b as e, c as f from base) select * from (select d from cte2 join "
        "cte1 on d = a) join (select b from cte1 join cte2 on b = e) on d = b");
    
    EXPECT_EQ(
        PlanNodeSearcher::searchFrom(*plan2)
            .where([](auto & node) { return node.getStep()->getType() == IQueryPlanStep::Type::Buffer; })
            .count(),
        0);

}
