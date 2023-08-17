#include "gtest_base_tpcds_plan_test.h"
#include "gtest_base_materialized_view_test.h"

#include <gtest/gtest.h>

#include <utility>

using namespace DB;
using namespace std::string_literals;

class MaterializedViewRewriteAdditionalTest : public ::testing::Test
{
public:
    static void SetUpTestSuite()
    {
        std::unordered_map<std::string, Field> settings;
#ifndef NDEBUG
        // debug mode may time out.
        settings.emplace("cascades_optimizer_timeout", "300000");
#endif
        settings.emplace("enable_materialized_view_rewrite", "1");
        settings.emplace("enable_materialized_view_join_rewriting", "1");
        settings.emplace("enable_materialized_view_rewrite_verbose_log", "1");
        settings.emplace("cte_mode", "INLINED");

        tester = std::make_shared<BaseMaterializedViewTest>(settings);
    }

    static void TearDownTestSuite()
    {
        tester.reset();
    }

    void SetUp() override
    {
    }

    static MaterializedViewRewriteTester sql(const String & materialize, const String & query) { return tester->sql(materialize, query); }

    static std::shared_ptr<BaseMaterializedViewTest> tester;
};

std::shared_ptr<BaseMaterializedViewTest> MaterializedViewRewriteAdditionalTest::tester;


TEST_F(MaterializedViewRewriteAdditionalTest, testHaving)
{
    String mv = ""
        "select deptno, sum(salary), sum(commission)\n"
        "from emps\n"
        "group by deptno\n"
        "having sum(salary) > 10000";
    String query =""
        "select deptno, sum(salary)\n"
        "from emps\n"
        "group by deptno\n"
        "having sum(salary) > 10000";
    sql(mv, query).noMat(); // having predicate rollup rewrite fail in clickhouse
}

TEST_F(MaterializedViewRewriteAdditionalTest, testHaving2)
{
    GTEST_SKIP() << "having predicate rollup rewrite fail.";
    String mv = ""
        "select deptno, sum(salary), sum(commission)\n"
        "from emps\n"
        "group by deptno\n"
        "having sum(salary) > 10000";
    String query =""
        "select * from\n"
        "(select deptno, sum(salary) sum\n"
        "from emps\n"
        "group by deptno\n"
        ") where sum > 10000";
    sql(mv, query).noMat(); // having predicate rollup rewrite fail in clickhouse
}

TEST_F(MaterializedViewRewriteAdditionalTest, testHaving3)
{
    String mv = ""
        "select deptno, empid, sum(salary), sum(commission)\n"
        "from emps\n"
        "group by deptno, empid\n"
        "having sum(salary) > 10000";
    String query= ""
        "select deptno, sum(salary)\n"
        "from emps\n"
        "group by deptno\n"
        "having sum(salary) > 10000";
    sql(mv, query).noMat();
}

TEST_F(MaterializedViewRewriteAdditionalTest, testAggDistinctInMvGrouping)
{
  String mv = "select avgState(deptno), empid from emps group by empid";
  String query = "select avg(deptno), max(empid) from emps group by empid";
  sql(mv, query)
      .checkingThatResultContains(
          "Projection\n"
          "│     Expressions: avg(deptno):=`expr#avgMerge(avgState(deptno))`, max(empid):=`expr#max(empid)_1`\n"
          "└─ Gather Exchange\n"
          "   └─ MergingAggregated\n"
          "      └─ Repartition Exchange\n"
          "         │     Partition by: {expr#empid}\n"
          "         └─ Aggregating\n"
          "            │     Group by: {expr#empid}\n"
          "            │     Aggregates: expr#avgMerge(avgState(deptno)):=AggNull(avgMerge)(expr#avgState(deptno)_1), "
          "expr#max(empid)_1:=AggNull(max)(expr#empid_1)\n"
          "            └─ Projection\n"
          "               │     Expressions: expr#avgState(deptno)_1:=`avgState(deptno)`, expr#empid:=empid, expr#empid_1:=empid\n"
          "               └─ TableScan test_mview.MV0_MV_DATA\n"
          "                        Outputs: [avgState(deptno), empid]")
      .ok();
}

/** The rollup aggregate of count is sum, default value is null. However, the default value of count
 * is 0. We need add coalesce on it if query is empty grouping. */
TEST_F(MaterializedViewRewriteAdditionalTest, testAggDistinctInMvGrouping2)
{
    String mv = "select count(deptno) cd, empid from emps group by empid";
    String query = "select count(deptno), max(empid) from emps";
    sql(mv, query)
        .checkingThatResultContains(
            "Projection\n"
            "│     Expressions: count(deptno):=coalesce(`expr#sum(cd)`, 0), max(empid):=`expr#max(empid)_1`\n"
            "└─ MergingAggregated\n"
            "   └─ Gather Exchange\n"
            "      └─ Aggregating\n"
            "         │     Group by: {}\n"
            "         │     Aggregates: expr#sum(cd):=AggNull(sum)(expr#cd), expr#max(empid)_1:=AggNull(max)(expr#empid)\n"
            "         └─ Projection\n"
            "            │     Expressions: expr#cd:=cd, expr#empid:=empid\n"
            "            └─ TableScan test_mview.MV0_MV_DATA\n"
            "                     Outputs: [cd, empid]")
        .ok();
}

TEST_F(MaterializedViewRewriteAdditionalTest, testAggDistinctInMvGrouping3)
{
  String mv = "select count(deptno) cd, empid from emps group by empid";
  String query = "select count(deptno), max(empid) from emps where empid = 5";
  sql(mv, query)
      .checkingThatResultContains(
          "Projection\n"
          "│     Expressions: count(deptno):=coalesce(`expr#sum(cd)`, 0), max(empid):=`expr#max(empid)_1`\n"
          "└─ MergingAggregated\n"
          "   └─ Gather Exchange\n"
          "      └─ Aggregating\n"
          "         │     Group by: {}\n"
          "         │     Aggregates: expr#sum(cd):=AggNull(sum)(expr#cd), expr#max(empid)_1:=AggNull(max)(expr#empid)\n"
          "         └─ Projection\n"
          "            │     Expressions: expr#cd:=cd, expr#empid:=empid\n"
          "            └─ Filter\n"
          "               │     Condition: empid = 5\n"
          "               └─ TableScan test_mview.MV0_MV_DATA\n"
          "                        Condition : empid = 5.\n"
          "                        Outputs: [cd, empid]")
      .ok();
}

TEST_F(MaterializedViewRewriteAdditionalTest, testSimpleAggregate)
{
  String q = "select count(*) from emps";
  sql(q, q)
      .checkingThatResultContains("Projection\n"
                                  "│     Expressions: count():=`expr#sum(count())`\n"
                                  "└─ MergingAggregated\n"
                                  "   └─ Gather Exchange\n"
                                  "      └─ Aggregating\n"
                                  "         │     Group by: {}\n"
                                  "         │     Aggregates: expr#sum(count()):=AggNull(sum)(expr#count()_2)\n"
                                  "         └─ Projection\n"
                                  "            │     Expressions: expr#count()_2:=`count()`\n"
                                  "            └─ TableScan test_mview.MV0_MV_DATA\n"
                                  "                     Outputs: [count()]")
      .ok();
}

TEST_F(MaterializedViewRewriteAdditionalTest, testEmptyGrouping)
{
  String mv = "select deptno, empid, count(commission) as count from emps group by deptno, empid";
  String query = "select count(commission) from emps";
  sql(mv, query)
      .checkingThatResultContains("Projection\n"
                                  "│     Expressions: count(commission):=coalesce(`expr#sum(count)`, 0)\n"
                                  "└─ MergingAggregated\n"
                                  "   └─ Gather Exchange\n"
                                  "      └─ Aggregating\n"
                                  "         │     Group by: {}\n"
                                  "         │     Aggregates: expr#sum(count):=AggNull(sum)(expr#count)\n"
                                  "         └─ Projection\n"
                                  "            │     Expressions: expr#count:=count\n"
                                  "            └─ TableScan test_mview.MV0_MV_DATA\n"
                                  "                     Outputs: [count]")
      .ok();
}

TEST_F(MaterializedViewRewriteAdditionalTest, testEmptyGrouping2)
{
  String mv = "select deptno, empid, count(commission) as count from emps group by deptno, empid";
  String query = "select count(commission) from emps where deptno = 1 and empid = 2";
  sql(mv, query)
      .checkingThatResultContains("Projection\n"
                                  "│     Expressions: count(commission):=coalesce(`expr#sum(count)`, 0)\n"
                                  "└─ MergingAggregated\n"
                                  "   └─ Gather Exchange\n"
                                  "      └─ Aggregating\n"
                                  "         │     Group by: {}\n"
                                  "         │     Aggregates: expr#sum(count):=AggNull(sum)(expr#count)\n"
                                  "         └─ Projection\n"
                                  "            │     Expressions: expr#count:=count\n"
                                  "            └─ Filter\n"
                                  "               │     Condition: (deptno = 1) AND (empid = 2)\n"
                                  "               └─ TableScan test_mview.MV0_MV_DATA\n"
                                  "                        Condition : (deptno = 1) AND (empid = 2).\n"
                                  "                        Outputs: [count, deptno, empid]\n")
      .ok();
}

TEST_F(MaterializedViewRewriteAdditionalTest, testAggOnViewNoAgg)
{
  String mv = ""
      "select deptno, empid\n"
      "from emps";
  String query = ""
      "select deptno, count(empid)\n"
      "from emps group by deptno";
  sql(mv, query).ok();
}

TEST_F(MaterializedViewRewriteAdditionalTest, testAggregateOnProject)
{
  sql("select empid, deptno, count(*) + 1 as c, sum(empid) as s from emps "
      "group by empid, deptno",
      "select count(*) + 1 as c, deptno from emps group by deptno, empid").noMat();
}

TEST_F(MaterializedViewRewriteAdditionalTest, testAggregateOnProject2)
{
  sql("select empid, deptno, count(*) + 1 as c, sum(empid) as s from emps "
      "group by empid, deptno",
      "select count(*) + 1 as c, deptno from emps group by deptno").noMat();
}

TEST_F(MaterializedViewRewriteAdditionalTest, testCTE)
{
  sql("SELECT empid, sumState(deptno) sum from emps group by empid",
      "WITH cte as (select empid, deptno from emps) SELECT sum(x.deptno) from cte x, cte y where x.empid = y.empid ")
      .noMat();
}
