#include <Optimizer/Signature/PlanNormalizer.h>
#include <Optimizer/Signature/PlanSegmentNormalizer.h>

#include <Optimizer/PlanNodeSearcher.h>
#include <QueryPlan/PlanPrinter.h>

#include <Optimizer/tests/gtest_base_tpcds_plan_test.h>
#include <gtest/gtest.h>
#include <string>

using namespace DB;

class PlanNormalizerTest : public ::testing::Test
{
public:
    static void SetUpTestSuite()
    {
        std::unordered_map<std::string, DB::Field> settings = BasePlanTest::getDefaultOptimizerSettings();
        tester = std::make_shared<BaseTpcdsPlanTest>(settings);
    }

    static void TearDownTestSuite()
    {
        tester.reset();
    }

    void normalize(const std::string & sql1,
                   const std::string & sql2,
                   IQueryPlanStep::Type type,
                   QueryPlanStepPtr & normal1,
                   QueryPlanStepPtr & normal2,
                   const std::unordered_map<std::string, Field> & settings = {});

    void checkEqual(const std::string & sql1,
                    const std::string & sql2,
                    IQueryPlanStep::Type type,
                    const std::unordered_map<std::string, Field> & settings = {});

    void checkNotEqual(const std::string & sql1,
                       const std::string & sql2,
                       IQueryPlanStep::Type type,
                       const std::unordered_map<std::string, Field> & settings = {});

    static std::shared_ptr<BaseTpcdsPlanTest> tester;
};

std::shared_ptr<BaseTpcdsPlanTest> PlanNormalizerTest::tester;

namespace
{
PlanNodePtr findLast(PlanNodePtr root, IQueryPlanStep::Type type)
{
    return PlanNodeSearcher::searchFrom(root).where([type](PlanNodeBase & node){ return node.getStep()->getType() == type;}).findAll().back();
}

} // anonymous namespace

void PlanNormalizerTest::normalize(const std::string & sql,
                              const std::string & sql1,
                              IQueryPlanStep::Type type,
                              QueryPlanStepPtr & normal,
                              QueryPlanStepPtr & normal1,
                              const std::unordered_map<std::string, Field> & settings)
{
    auto context = tester->createQueryContext(settings);
    auto plan = tester->plan(sql, context);
    PlanNormalizer normalizer(plan->getCTEInfo(), context);
    auto node = findLast(plan->getPlanNode(), type);
    normal = normalizer.computeNormalStep(node);

    // reuse context
    plan = tester->plan(sql1, context);
    PlanNormalizer normalizer1(plan->getCTEInfo(), context);
    auto node1 = findLast(plan->getPlanNode(), type);
    normal1 = normalizer1.computeNormalStep(node1);
}

void PlanNormalizerTest::checkEqual(const std::string & sql1,
                               const std::string & sql2,
                               IQueryPlanStep::Type type,
                               const std::unordered_map<std::string, Field> & settings)
{
    QueryPlanStepPtr normal1;
    QueryPlanStepPtr normal2;
    normalize(sql1, sql2, type, normal1, normal2, settings);
    ASSERT_TRUE(normal1);
    ASSERT_TRUE(normal2);
    EXPECT_EQ(normal1->getType(), type);
    EXPECT_EQ(normal2->getType(), type);
    EXPECT_EQ(*normal1, *normal2);
    }

void PlanNormalizerTest::checkNotEqual(const std::string & sql1,
                                  const std::string & sql2,
                                  IQueryPlanStep::Type type,
                                  const std::unordered_map<std::string, Field> & settings)
{
    QueryPlanStepPtr normal1;
    QueryPlanStepPtr normal2;
    normalize(sql1, sql2, type, normal1, normal2, settings);
    ASSERT_TRUE(normal1);
    ASSERT_TRUE(normal2);
    EXPECT_EQ(normal1->getType(), type);
    EXPECT_EQ(normal2->getType(), type);
    EXPECT_NE(*normal1, *normal2);
    }

TEST_F(PlanNormalizerTest, testTableScanNormalize)
{
    std::string sql = "select d_date_sk, d_moy from date_dim group by d_date_sk, d_moy";
    std::string sql_ok = "select d_moy from date_dim where d_date_sk=rand()"; // same columns d_date_sk, d_moy and no prewhere
    std::string sql_diff = "select d_date_sk, d_moy from date_dim where d_date_sk=1"; // has prewhere d_date_sk=1, thus different
    std::string sql_diff_1 = "select d_date_sk from date_dim group by d_date_sk"; // different column, thus different

    checkEqual(sql, sql_ok, IQueryPlanStep::Type::TableScan);
    checkNotEqual(sql, sql_diff, IQueryPlanStep::Type::TableScan);
    checkNotEqual(sql, sql_diff_1, IQueryPlanStep::Type::TableScan);
}

TEST_F(PlanNormalizerTest, testTableScanNormalizeWithPushdownFilter)
{
    std::unordered_map<std::string, Field> settings;
    settings.emplace("optimizer_projection_support", 1);

    std::string sql
        = "select d_moy as d from date_dim where d_date_sk=1"; // pushdown projection will pruning column `d_date_sk`, thus different with sql_diff_2
    std::string sql_ok = "select d_moy as d from date_dim where d_date_sk=1";
    std::string sql_diff_2 = "select d_date_sk, d_moy from date_dim where d_date_sk=1";
    std::string sql_diff = "select d_date_sk, d_moy from date_dim where d_date_sk=2";
    std::string sql_diff_1 = "select d_date_sk, d_moy from date_dim where d_moy=1";

    checkEqual(sql, sql_ok, IQueryPlanStep::Type::TableScan, settings);
    checkNotEqual(sql, sql_diff, IQueryPlanStep::Type::TableScan, settings);
    checkNotEqual(sql, sql_diff_1, IQueryPlanStep::Type::TableScan, settings);
    checkNotEqual(sql, sql_diff_2, IQueryPlanStep::Type::TableScan, settings);

    // additional check on push down filter
    auto context = tester->createQueryContext(settings);
    auto plan = tester->plan(sql, context);
    PlanNormalizer normalizer(plan->getCTEInfo(), context);
    auto node = findLast(plan->getPlanNode(), IQueryPlanStep::Type::TableScan);
    auto normal = normalizer.computeNormalStep(node);
    auto node_cast = dynamic_pointer_cast<TableScanStep>(node->getStep());
    auto normal_cast = dynamic_pointer_cast<TableScanStep>(normal);
    EXPECT_TRUE(node_cast && node_cast->getPushdownFilter());
    EXPECT_TRUE(normal_cast && normal_cast->getPushdownFilter());
}

// TEST_F(PlanNormalizerTest, testFilterNormalize)
// {
//     std::string sql = "select d_moy from date_dim where d_date_sk=1 and d_moy>2";
//     std::string sql_ok = "select d_moy from date_dim where d_moy>2 and d_date_sk=1"; // order of "and" does not matter
//     std::string sql_diff = "select d_date_sk, d_moy from date_dim where d_date_sk=1"; // different filter
//     std::string sql_diff_1 = "select d_moy, d_date_sk from date_dim where d_moy=1"; // test for bottom-up normalize

//     checkEqual(sql, sql_ok, IQueryPlanStep::Type::Filter);
//     checkNotEqual(sql, sql_diff, IQueryPlanStep::Type::Filter);
//     checkNotEqual(sql_diff, sql_diff_1, IQueryPlanStep::Type::Filter);
// }

TEST_F(PlanNormalizerTest, testProjectionNormalize)
{
    std::string sql = "select d_date_sk-1, d_moy+1 from date_dim";
    std::string sql_ok = "select d_moy+1, d_date_sk-1 from date_dim"; // reordering
    std::string sql_diff = "select distinct(d_date_sk-1) from date_dim where d_moy+1>0"; // not final
    std::string sql_diff_1 = "select d_date_sk-1, d_moy+1, now() from date_dim"; // different project

    checkEqual(sql, sql_ok, IQueryPlanStep::Type::Projection);
    checkNotEqual(sql, sql_diff, IQueryPlanStep::Type::Projection);
    checkNotEqual(sql, sql_diff_1, IQueryPlanStep::Type::Projection);
}

TEST_F(PlanNormalizerTest, testAggregationNormalize)
{
    std::string sql = "select d_date_sk, count(d_moy), sum(d_moy) from date_dim where d_moy=1 group by d_date_sk";
    std::string sql_ok = "select sum(d_moy), count(d_moy), d_date_sk from date_dim where d_moy=1 group by d_date_sk";
    std::string sql_diff = "select d_moy, count(d_date_sk), sum(d_date_sk) from date_dim where d_moy=1 group by d_moy";
    std::string sql_diff_1 = "select count(d_date_sk), sum(d_date_sk) from date_dim where d_moy=1";

    checkEqual(sql, sql_ok, IQueryPlanStep::Type::Aggregating);
    checkNotEqual(sql, sql_diff, IQueryPlanStep::Type::Aggregating);
    checkNotEqual(sql, sql_diff_1, IQueryPlanStep::Type::Aggregating);
}

TEST_F(PlanNormalizerTest, DISABLED_testJoinNormalize)
{
    std::unordered_map<std::string, Field> settings;
    settings.emplace("enable_runtime_filter", 0);

    std::string sql = "select count(cs_sales_price) from catalog_sales, item"
                      "\nwhere cs_item_sk+1 = i_item_sk+1 group by cs_sold_date_sk";
    std::string sql_ok = "select count(cs_sales_price) from item join catalog_sales"
                         "\non i_item_sk+1 = cs_item_sk+1 group by cs_sold_date_sk";
    std::string sql_diff = "select count(cs_sales_price) from catalog_sales left join item"
                           "\non cs_item_sk+1 = i_item_sk+1 group by cs_sold_date_sk";

    checkEqual(sql, sql_ok, IQueryPlanStep::Type::Join, settings);
    checkEqual(sql, sql_ok, IQueryPlanStep::Type::Aggregating, settings);
    checkNotEqual(sql, sql_diff, IQueryPlanStep::Type::Join, settings);
}

// The aggregating node in Q1 has should_produce_results_in_order_of_bucket_number=true, while the sql itself is false
TEST_F(PlanNormalizerTest, DISABLED_testCTENormalize)
{
    std::unordered_map<std::string, Field> settings;
    settings.emplace("cte_mode", "SHARED");
    std::string sql = tester->loadQuery("q1").sql.front().first;
    std::string sql_with = "select sr_customer_sk, sr_store_sk, sum(sr_return_amt)\n"
                           " from store_returns, date_dim\n"
                           " where d_year = 2000 and sr_returned_date_sk = d_date_sk\n"
                           " group by sr_customer_sk, sr_store_sk";
    checkEqual(sql, sql, IQueryPlanStep::Type::CTERef, settings);

    // additional check on cte plan
    auto context = tester->createQueryContext(settings);
    auto plan = tester->plan(sql, context);
    PlanNormalizer normalizer(plan->getCTEInfo(), context);
    auto cte_node = findLast(plan->getPlanNode(), IQueryPlanStep::Type::CTERef);
    auto cte_root = plan->getCTEInfo().getCTEDef(dynamic_pointer_cast<CTERefStep>(cte_node->getStep())->getId());
    auto cte_agg = findLast(cte_root, IQueryPlanStep::Type::Aggregating);
    auto normal_root = normalizer.computeNormalStep(cte_agg);

    auto with_plan = tester->plan(sql_with, context);
    PlanNormalizer with_normalizer(with_plan->getCTEInfo(), context);
    auto with_agg = findLast(with_plan->getPlanNode(), IQueryPlanStep::Type::Aggregating);
    auto normal_with = with_normalizer.computeNormalStep(with_agg);

    ASSERT_TRUE(normal_root && normal_with);
    EXPECT_EQ(*normal_root, *normal_with);
}

/// test now(), table name, filter
TEST_F(PlanNormalizerTest, NormalizePlanSegment)
{
    const auto * sql1 = "select inv_item_sk, now() from inventory where inv_item_sk > 3 AND inv_item_sk < 6";
    std::unordered_map<std::string, Field> query_settings;
    auto context1 = tester->createQueryContext(query_settings);
    TxnTimestamp txn1 = 1;
    context1->setTemporaryTransaction(txn1, txn1, false);

    auto plan1 = tester->plan(sql1, context1);
    auto normalizer1 = PlanSegmentNormalizer(context1, &plan1->getCTEInfo());
    auto logical_plan1 = normalizer1.buildNormalPlan(plan1->getPlanNode(), {.normalize_literals = true, .normalize_storage = true});
    auto logical_plan_str1 = PlanPrinter::textPlanNode(logical_plan1, context1);

    const auto * sql2 = "select inv_item_sk, now() from inventory where inv_item_sk > 2 AND inv_item_sk < 9";
    auto context2 = tester->createQueryContext(query_settings);
    TxnTimestamp txn2 = 1;
    context2->setTemporaryTransaction(txn2, txn2, false);
    auto plan2 = tester->plan(sql2, context2);
    auto normalizer2 = PlanSegmentNormalizer(context2, &plan1->getCTEInfo());
    auto logical_plan2 = normalizer2.buildNormalPlan(plan2->getPlanNode(), {.normalize_literals = true, .normalize_storage = true});
    auto logical_plan_str2 = PlanPrinter::textPlanNode(logical_plan2, context2);

    ASSERT_EQ(logical_plan_str1, logical_plan_str2) << logical_plan_str1 << logical_plan_str2;
}
