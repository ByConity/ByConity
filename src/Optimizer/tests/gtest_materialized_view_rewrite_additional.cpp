#include "gtest_base_materialized_view_test.h"
#include "gtest_base_tpcds_plan_test.h"

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
        settings.emplace("enable_materialized_view_union_rewriting", "1");
        settings.emplace("enable_materialized_view_rewrite_verbose_log", "1");
        settings.emplace("materialized_view_consistency_check_method", "NONE");
        settings.emplace("cte_mode", "INLINED");
        tester = std::make_shared<BaseMaterializedViewTest>(settings);
    }

    static void TearDownTestSuite() { tester.reset(); }

    void SetUp() override { }

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
    String query = ""
                   "select deptno, sum(salary)\n"
                   "from emps\n"
                   "group by deptno\n"
                   "having sum(salary) > 10000";
    sql(mv, query).noMat("having predicate rollup rewrite fail in clickhouse");
}

TEST_F(MaterializedViewRewriteAdditionalTest, testHaving2)
{
    String mv = ""
                "select deptno, sum(salary), sum(commission)\n"
                "from emps\n"
                "group by deptno\n"
                "having sum(salary) > 10000";
    String query = ""
                   "select * from\n"
                   "(select deptno, sum(salary) sum\n"
                   "from emps\n"
                   "group by deptno\n"
                   ") where sum > 10000";
    sql(mv, query).noMat("having predicate rollup rewrite fail in clickhouse");
}

TEST_F(MaterializedViewRewriteAdditionalTest, testHaving3)
{
    String mv = ""
                "select deptno, empid, sum(salary), sum(commission)\n"
                "from emps\n"
                "group by deptno, empid\n"
                "having sum(salary) > 10000";
    String query = ""
                   "select deptno, sum(salary)\n"
                   "from emps\n"
                   "group by deptno\n"
                   "having sum(salary) > 10000";
    sql(mv, query).noMat("having predicate rollup rewrite fail in clickhouse");
}

TEST_F(MaterializedViewRewriteAdditionalTest, testAggDistinctInMvGrouping)
{
    String mv = "select avgState(deptno), empid from emps group by empid";
    String query = "select avg(deptno), max(empid) from emps group by empid";
    sql(mv, query).ok();
}

/** The rollup aggregate of count is sum, default value is null. However, the default value of count
 * is 0. We need add coalesce on it if query is empty grouping. */
TEST_F(MaterializedViewRewriteAdditionalTest, testAggDistinctInMvGrouping2)
{
    String mv = "select count(deptno) cd, empid from emps group by empid";
    String query = "select count(deptno), max(empid) from emps";
    sql(mv, query)
        .checkingThatResultContains(
            "Expressions: [expr#max(empid)_3], expr#count(deptno)_3:=coalesce(`expr#sum(expr#count(deptno))_2`, 0)")
        .ok();
}

TEST_F(MaterializedViewRewriteAdditionalTest, testAggDistinctInMvGrouping3)
{
    String mv = "select count(deptno) cd, empid from emps group by empid";
    String query = "select count(deptno), max(empid) from emps where empid = 5";
    sql(mv, query)
        .checkingThatResultContains(
            "Expressions: [expr#max(empid)_3], expr#count(deptno)_3:=coalesce(`expr#sum(expr#count(deptno))_2`, 0)")
        .ok();
}

TEST_F(MaterializedViewRewriteAdditionalTest, testEmptyGrouping)
{
    String mv = "select deptno, empid, count(commission) as count from emps group by deptno, empid";
    String query = "select count(commission) from emps";
    sql(mv, query)
        .checkingThatResultContains("Expressions: expr#count(commission)_3:=coalesce(`expr#sum(expr#count(commission))_2`, 0)")
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
        "select count(*) + 1 as c, deptno from emps group by deptno, empid")
        .noMat();
}

TEST_F(MaterializedViewRewriteAdditionalTest, testAggregateOnProject2)
{
    sql("select empid, deptno, count(*) + 1 as c, sum(empid) as s from emps "
        "group by empid, deptno",
        "select count(*) + 1 as c, deptno from emps group by deptno")
        .noMat();
}

TEST_F(MaterializedViewRewriteAdditionalTest, testCTE)
{
    sql("SELECT empid, sumState(deptno) sum from emps group by empid",
        "WITH cte as (select empid, deptno from emps) SELECT sum(x.deptno) from cte x, cte y where x.empid = y.empid ")
        .noMat();
}

TEST_F(MaterializedViewRewriteAdditionalTest, testUnionQueryRewrite)
{
    String q = "select deptno, salary from emps where empid > 300";
    String m = "select deptno, salary from emps where empid > 500";
    sql(m, q).checkingThatResultContains("Union").ok();
}


TEST_F(MaterializedViewRewriteAdditionalTest, testRollupUnionQueryRewrite)
{
    String q = "select deptno, sum(salary) from emps where empid > 300 group by deptno";
    String m = "select deptno, sumState(salary) from emps where empid > 500 group by deptno";
    sql(m, q).checkingThatResultContains("Union").ok();
}

TEST_F(MaterializedViewRewriteAdditionalTest, testRollupUnionQueryRewrite2)
{
    String q = "select deptno, sum(salary) from emps where empid > 300 group by deptno";
    String m = "select deptno, sumState(salary) from emps where empid < 500 group by deptno";
    sql(m, q).noMat();
}

TEST_F(MaterializedViewRewriteAdditionalTest, testRollupUnionQueryRewrite3)
{
    String q = "select deptno, sum(salary) from emps where empid > 300 group by deptno";
    String m = "select deptno, sumState(salary) from emps where empid in (500, 501) group by deptno";
    sql(m, q).checkingThatResultContains("Union").ok();
}

TEST_F(MaterializedViewRewriteAdditionalTest, testRollupUnionQueryRewrite4)
{
    String q = "select deptno, sum(salary) from emps where empid in (501, 502) and name != 'hello' group by deptno";
    String m = "select deptno, sumState(salary) from emps where empid in (501) and name != 'hello' group by deptno";
    sql(m, q).checkingThatResultContains("Union").ok();
}

TEST_F(MaterializedViewRewriteAdditionalTest, testOuterJoin0)
{
    sql("select depts.deptno, dependents.empid\n"
            "from depts\n"
            "left join dependents on (depts.name = dependents.name)\n"
            "left join locations on (locations.name = dependents.name)\n"
            "left join emps on (emps.deptno = depts.deptno)\n"
            "where depts.deptno > 10\n"
            "group by depts.deptno, dependents.empid",
        "select dependents.empid\n"
            "from depts\n"
            "left join dependents on (depts.name = dependents.name)\n"
            "left join locations on (locations.name = dependents.name)\n"
            "left join emps on (emps.deptno = depts.deptno)\n"
            "where depts.deptno > 10\n"
            "group by dependents.empid")
        .ok();
}

TEST_F(MaterializedViewRewriteAdditionalTest, testJoinDerive0)
{
    sql("select dependents.empid, depts.deptno, dependents.name, emps.deptno\n"
            "from depts\n"
            "left join dependents on (depts.name = dependents.name)\n"
            "left join emps on (emps.deptno = depts.deptno)\n"
            "where depts.deptno > 11\n"
            "group by dependents.empid, depts.deptno, dependents.name, emps.deptno",
        "select dependents.empid\n"
            "from depts\n"
            "join dependents on (depts.name = dependents.name)\n"
            "join emps on (emps.deptno = depts.deptno)\n"
            "where depts.deptno > 10\n"
            "group by dependents.empid")
        .checkingThatResultContains("isNotNull")
        .ok();
}
