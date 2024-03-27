/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "gtest_base_tpcds_plan_test.h"
#include "gtest_base_materialized_view_test.h"

#include <gtest/gtest.h>

#include <utility>

using namespace DB;
using namespace std::string_literals;

class MaterializedViewRewriteComplexTest : public ::testing::Test
{
public:
    static void SetUpTestSuite()
    {
        std::unordered_map<String, Field> settings;
#ifndef NDEBUG
        // debug mode may time out.
        settings.emplace("cascades_optimizer_timeout", "300000");
#endif
        settings.emplace("enable_materialized_view_rewrite", "1");
        settings.emplace("enable_materialized_view_join_rewriting", "1");
        settings.emplace("enable_materialized_view_rewrite_verbose_log", "1");
        settings.emplace("enable_single_distinct_to_group_by", "0");
        settings.emplace("materialized_view_consistency_check_method", "NONE");

        tester = std::make_shared<BaseMaterializedViewTest>(settings);
    }

    static void TearDownTestSuite()
    {
        tester.reset();
    }

    void SetUp() override
    {
    }

    static MaterializedViewRewriteTester sql(const String & materialize, const String & query)
    {
        return tester->sql(materialize, query);
    }

    static std::shared_ptr<BaseMaterializedViewTest> tester;
};

std::shared_ptr<BaseMaterializedViewTest> MaterializedViewRewriteComplexTest::tester;


TEST_F(MaterializedViewRewriteComplexTest, testSwapJoin)
{
    GTEST_SKIP();
    sql("select count(*) as c from foodmart.sales_fact_1997 as s"
            " join foodmart.time_by_day as t on s.time_id = t.time_id",
        "select count(*) as c from foodmart.time_by_day as t"
            " join foodmart.sales_fact_1997 as s on t.time_id = s.time_id")
        .ok();
}

/** Aggregation materialization with a project. */
TEST_F(MaterializedViewRewriteComplexTest, testAggregateProject)
{
    // Note that materialization does not start with the GROUP BY columns.
    // Not a smart way to design a materialization, but people may do it.
    sql("select deptno, count(*) as c, empid + 2, sum(empid) as s "
        "from emps group by empid, deptno",
        "select count(*) + 1 as c, deptno from emps group by deptno")
        .checkingThatResultContains("Aggregates: expr#sum(expr#count())_2:=AggNull(sum)(expr#count()_2)")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationNoAggregateFuncs1)
{
    sql("select empid, deptno from emps group by empid, deptno",
        "select empid, deptno from emps group by empid, deptno").ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationNoAggregateFuncs2)
{
    sql("select empid, deptno from emps group by empid, deptno",
        "select deptno from emps group by deptno")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationNoAggregateFuncs3)
{
    sql("select deptno from emps group by deptno",
        "select empid, deptno from emps group by empid, deptno")
        .noMat();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationNoAggregateFuncs4)
{
    sql("select empid, deptno\n"
        "from emps where deptno = 10 group by empid, deptno",
        "select deptno from emps where deptno = 10 group by deptno")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationNoAggregateFuncs5)
{
    sql("select empid, deptno\n"
            "from emps where deptno = 5 group by empid, deptno",
        "select deptno from emps where deptno = 10 group by deptno")
        .noMat();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationNoAggregateFuncs6)
{
    sql("select empid, deptno\n"
        "from emps where deptno > 5 group by empid, deptno",
        "select deptno from emps where deptno > 10 group by deptno")
        .checkingThatResultContains("Where: deptno > 10")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationNoAggregateFuncs7)
{
    sql("select empid, deptno\n"
            "from emps where deptno > 5 group by empid, deptno",
        "select deptno from emps where deptno < 10 group by deptno")
        .checkingThatResultContains("Union")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationNoAggregateFuncs8)
{
    sql("select empid from emps group by empid, deptno",
        "select deptno from emps group by deptno")
        .noMat();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationNoAggregateFuncs9)
{
    sql("select empid, deptno from emps\n"
            "where salary > 1000 group by name, empid, deptno",
        "select empid from emps\n"
            "where salary > 2000 group by name, empid")
        .noMat();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationAggregateFuncs1)
{
    sql("select empid, deptno, count(*) as c, sum(empid) as s\n"
        "from emps group by empid, deptno",
        "select deptno from emps group by deptno")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationAggregateFuncs2)
{
    sql("select empid, deptno, count(*) as c, sum(empid) as s\n"
        "from emps group by empid, deptno",
        "select deptno, count(*) as c, sum(empid) as s\n"
            "from emps group by deptno")
        .checkingThatResultContains("Aggregates: expr#sum(expr#count())_2:=AggNull(sum)(expr#count()_2), expr#sum(expr#sum(empid))_2:=AggNull(sum)(expr#sum(empid)_2)")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationAggregateFuncs3)
{
    sql("select empid, deptno, count(*) as c, sum(empid) as s\n"
        "from emps group by empid, deptno",
        "select deptno, empid, sum(empid) as s, count(*) as c\n"
        "from emps group by empid, deptno")
        .checkingThatResultContains("Aggregates: expr#sum(expr#sum(empid))_2:=AggNull(sum)(expr#sum(empid)_2), expr#sum(expr#count())_2:=AggNull(sum)(expr#count()_2)")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationAggregateFuncs4)
{
    sql("select empid, deptno, count(*) as c, sum(empid) as s\n"
        "from emps where deptno >= 10 group by empid, deptno",
        "select deptno, sum(empid) as s\n"
        "from emps where deptno > 10 group by deptno")
        .checkingThatResultContains("Where: deptno > 10")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationAggregateFuncs5)
{
    sql("select empid, deptno, count(*) + 1 as c, sum(empid) as s\n"
        "from emps where deptno >= 10 group by empid, deptno",
        "select deptno, sum(empid) + 1 as s\n"
        "from emps where deptno > 10 group by deptno")
        .checkingThatResultContains("deptno > 10")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationAggregateFuncs6)
{
    sql("select empid, deptno, count(*) + 1 as c, sum(empid) + 2 as s\n"
            "from emps where deptno >= 10 group by empid, deptno",
        "select deptno, sum(empid) + 1 as s\n"
            "from emps where deptno > 10 group by deptno")
        .noMat();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationAggregateFuncs7)
{
    sql("select empid, deptno, count(*) + 1 as c, sum(empid) as s\n"
        "from emps where deptno >= 10 group by empid, deptno",
        "select deptno + 1, sum(empid) + 1 as s\n"
        "from emps where deptno > 10 group by deptno")
        .checkingThatResultContains("Where: deptno > 10")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationAggregateFuncs8)
{
    GTEST_SKIP();
    // TODO: It should work, but top project in the query is not matched by the planner.
    // It needs further checking.
    sql("select empid, deptno + 1, count(*) + 1 as c, sum(empid) as s\n"
            "from emps where deptno >= 10 group by empid, deptno",
        "select deptno + 1, sum(empid) + 1 as s\n"
            "from emps where deptno > 10 group by deptno")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationAggregateFuncs9)
{
    GTEST_SKIP() << "time function rollup rewrite is not implemented.";
    sql("select empid, floor(cast('1997-01-20 12:34:56' as timestamp) to month), "
            "count(*) + 1 as c, sum(empid) as s\n"
            "from emps\n"
            "group by empid, floor(cast('1997-01-20 12:34:56' as timestamp) to month)",
        "select floor(cast('1997-01-20 12:34:56' as timestamp) to year), sum(empid) as s\n"
            "from emps group by floor(cast('1997-01-20 12:34:56' as timestamp) to year)")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationAggregateFuncs10)
{
    GTEST_SKIP() << "time function rollup rewrite is not implemented.";
    sql("select empid, floor(cast('1997-01-20 12:34:56' as timestamp) to month), "
            "count(*) + 1 as c, sum(empid) as s\n"
            "from emps\n"
            "group by empid, floor(cast('1997-01-20 12:34:56' as timestamp) to month)",
        "select floor(cast('1997-01-20 12:34:56' as timestamp) to year), sum(empid) + 1 as s\n"
            "from emps group by floor(cast('1997-01-20 12:34:56' as timestamp) to year)")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationAggregateFuncs11)
{
    GTEST_SKIP() << "time function rollup rewrite is not implemented.";
    sql("select empid, floor(cast('1997-01-20 12:34:56' as timestamp) to second), "
            "count(*) + 1 as c, sum(empid) as s\n"
            "from emps\n"
            "group by empid, floor(cast('1997-01-20 12:34:56' as timestamp) to second)",
        "select floor(cast('1997-01-20 12:34:56' as timestamp) to minute), sum(empid) as s\n"
            "from emps group by floor(cast('1997-01-20 12:34:56' as timestamp) to minute)")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationAggregateFuncs12)
{
    GTEST_SKIP() << "time function rollup rewrite is not implemented.";
    sql("select empid, floor(cast('1997-01-20 12:34:56' as timestamp) to second), "
            "count(*) + 1 as c, sum(empid) as s\n"
            "from emps\n"
            "group by empid, floor(cast('1997-01-20 12:34:56' as timestamp) to second)",
        "select floor(cast('1997-01-20 12:34:56' as timestamp) to month), sum(empid) as s\n"
            "from emps group by floor(cast('1997-01-20 12:34:56' as timestamp) to month)")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationAggregateFuncs13)
{
    GTEST_SKIP() << "time function rollup rewrite is not implemented.";
    sql("select empid, cast('1997-01-20 12:34:56' as timestamp), "
            "count(*) + 1 as c, sum(empid) as s\n"
            "from emps\n"
            "group by empid, cast('1997-01-20 12:34:56' as timestamp)",
        "select floor(cast('1997-01-20 12:34:56' as timestamp) to year), sum(empid) as s\n"
            "from emps group by floor(cast('1997-01-20 12:34:56' as timestamp) to year)")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationAggregateFuncs14)
{
    GTEST_SKIP() << "time function rollup rewrite is not implemented.";
    sql("select empid, floor(cast('1997-01-20 12:34:56' as timestamp) to month), "
            "count(*) + 1 as c, sum(empid) as s\n"
            "from emps\n"
            "group by empid, floor(cast('1997-01-20 12:34:56' as timestamp) to month)",
        "select floor(cast('1997-01-20 12:34:56' as timestamp) to hour), sum(empid) as s\n"
            "from emps group by floor(cast('1997-01-20 12:34:56' as timestamp) to hour)")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationAggregateFuncs15)
{
    GTEST_SKIP() << "time function rollup rewrite is not implemented.";
    sql("select eventid, floor(cast(ts as timestamp) to second), "
            "count(*) + 1 as c, sum(eventid) as s\n"
            "from events group by eventid, floor(cast(ts as timestamp) to second)",
        "select floor(cast(ts as timestamp) to minute), sum(eventid) as s\n"
            "from events group by floor(cast(ts as timestamp) to minute)")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationAggregateFuncs16)
{
    GTEST_SKIP() << "time function rollup rewrite is not implemented.";
    sql("select eventid, cast(ts as timestamp), count(*) + 1 as c, sum(eventid) as s\n"
            "from events group by eventid, cast(ts as timestamp)",
        "select floor(cast(ts as timestamp) to year), sum(eventid) as s\n"
            "from events group by floor(cast(ts as timestamp) to year)")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationAggregateFuncs17)
{
    GTEST_SKIP() << "time function rollup rewrite is not implemented.";
    sql("select eventid, floor(cast(ts as timestamp) to month), "
            "count(*) + 1 as c, sum(eventid) as s\n"
            "from events group by eventid, floor(cast(ts as timestamp) to month)",
        "select floor(cast(ts as timestamp) to hour), sum(eventid) as s\n"
            "from events group by floor(cast(ts as timestamp) to hour)")
        .checkingThatResultContains("EnumerableTableScan(table=[[hr, events]])")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationAggregateFuncs18)
{
    sql("select empid, deptno, count(*) + 1 as c, sum(empid) as s\n"
            "from emps group by empid, deptno",
        "select empid*deptno, sum(empid) as s\n"
            "from emps group by empid*deptno")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationAggregateFuncs19)
{
    sql("select empid, deptno, count(*) as c, sum(empid) as s\n"
            "from emps group by empid, deptno",
        "select empid + 10, count(*) + 1 as c\n"
            "from emps group by empid + 10")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationAggregateFuncs20)
{
    GTEST_SKIP() << "ast parser error without optimzier.";
    sql("select 11 as empno, 22 as sal, count(*) from emps group by 11, 22",
        "select * from\n"
            "(select 11 as empno, 22 as sal, count(*)\n"
            "from emps group by 11, 22) tmp\n"
            "where sal = 33")
        .checkingThatResultContains("EnumerableValues(tuples=[[]])")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinAggregateMaterializationNoAggregateFuncs1)
{
    sql("select empid, depts.deptno from emps\n"
            "join depts on emps.deptno = depts.deptno where depts.deptno > 10\n"
            "group by empid, depts.deptno",
        "select empid from emps\n"
            "join depts on emps.deptno = depts.deptno where depts.deptno > 20\n"
            "group by empid, depts.deptno")
        .checkingThatResultContains("Where: `depts.deptno` > 20")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinAggregateMaterializationNoAggregateFuncs2)
{
    sql("select depts.deptno, empid from depts\n"
            "join emps on emps.deptno = depts.deptno where depts.deptno > 10\n"
            "group by empid, depts.deptno",
        "select empid from emps\n"
            "join depts on emps.deptno = depts.deptno where depts.deptno > 20\n"
            "group by empid, depts.deptno")
        .checkingThatResultContains("Where: deptno > 20")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinAggregateMaterializationNoAggregateFuncs3)
{
    // It does not match, Project on top of query
    sql("select empid from emps\n"
            "join depts on emps.deptno = depts.deptno where depts.deptno > 10\n"
            "group by empid, depts.deptno",
        "select empid from emps\n"
            "join depts on emps.deptno = depts.deptno where depts.deptno > 20\n"
            "group by empid, depts.deptno")
        .noMat();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinAggregateMaterializationNoAggregateFuncs4)
{
    sql("select empid, depts.deptno from emps\n"
            "join depts on emps.deptno = depts.deptno where emps.deptno > 10\n"
            "group by empid, depts.deptno",
        "select empid from emps\n"
            "join depts on emps.deptno = depts.deptno where depts.deptno > 20\n"
            "group by empid, depts.deptno")
        .checkingThatResultContains("Where: `depts.deptno` > 20")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinAggregateMaterializationNoAggregateFuncs5)
{
    sql("select depts.deptno, emps.empid from depts\n"
            "join emps on emps.deptno = depts.deptno where emps.empid > 10\n"
            "group by depts.deptno, emps.empid",
        "select depts.deptno from depts\n"
            "join emps on emps.deptno = depts.deptno where emps.empid > 15\n"
            "group by depts.deptno, emps.empid")
        .checkingThatResultContains("Where: empid > 15")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinAggregateMaterializationNoAggregateFuncs6)
{
    sql("select depts.deptno, emps.empid from depts\n"
            "join emps on emps.deptno = depts.deptno where emps.empid > 10\n"
            "group by depts.deptno, emps.empid",
        "select depts.deptno from depts\n"
            "join emps on emps.deptno = depts.deptno where emps.empid > 15\n"
            "group by depts.deptno")
        .checkingThatResultContains("Where: empid > 15")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinAggregateMaterializationNoAggregateFuncs7)
{
    sql("select depts.deptno, dependents.empid\n"
            "from depts\n"
            "join dependents on (depts.name = dependents.name)\n"
            "join locations on (locations.name = dependents.name)\n"
            "join emps on (emps.deptno = depts.deptno)\n"
            "where depts.deptno > 11\n"
            "group by depts.deptno, dependents.empid",
        "select dependents.empid\n"
            "from depts\n"
            "join dependents on (depts.name = dependents.name)\n"
            "join locations on (locations.name = dependents.name)\n"
            "join emps on (emps.deptno = depts.deptno)\n"
            "where depts.deptno > 10\n"
            "group by dependents.empid")
        .checkingThatResultContains("Union")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinAggregateMaterializationNoAggregateFuncs8)
{
    sql("select depts.deptno, dependents.empid\n"
            "from depts\n"
            "join dependents on (depts.name = dependents.name)\n"
            "join locations on (locations.name = dependents.name)\n"
            "join emps on (emps.deptno = depts.deptno)\n"
            "where depts.deptno > 20\n"
            "group by depts.deptno, dependents.empid",
        "select dependents.empid\n"
            "from depts\n"
            "join dependents on (depts.name = dependents.name)\n"
            "join locations on (locations.name = dependents.name)\n"
            "join emps on (emps.deptno = depts.deptno)\n"
            "where depts.deptno > 10 and depts.deptno < 20\n"
            "group by dependents.empid")
        .noMat();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinAggregateMaterializationNoAggregateFuncs9)
{
    sql("select depts.deptno, dependents.empid\n"
            "from depts\n"
            "join dependents on (depts.name = dependents.name)\n"
            "join locations on (locations.name = dependents.name)\n"
            "join emps on (emps.deptno = depts.deptno)\n"
            "where depts.deptno > 11 and depts.deptno < 19\n"
            "group by depts.deptno, dependents.empid",
        "select dependents.empid\n"
            "from depts\n"
            "join dependents on (depts.name = dependents.name)\n"
            "join locations on (locations.name = dependents.name)\n"
            "join emps on (emps.deptno = depts.deptno)\n"
            "where depts.deptno > 10 and depts.deptno < 20\n"
            "group by dependents.empid")
        .checkingThatResultContains("Union")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinAggregateMaterializationNoAggregateFuncs10)
{
    GTEST_SKIP() << "join compensation rewrite is not implemented.";
    sql("select depts.name, dependents.name as name2, "
            "emps.deptno, depts.deptno as deptno2, "
            "dependents.empid\n"
            "from depts, dependents, emps\n"
            "where depts.deptno > 10\n"
            "group by depts.name, dependents.name, "
            "emps.deptno, depts.deptno, "
            "dependents.empid",
        "select dependents.empid\n"
            "from depts\n"
            "join dependents on (depts.name = dependents.name)\n"
            "join emps on (emps.deptno = depts.deptno)\n"
            "where depts.deptno > 10\n"
            "group by dependents.empid")
        .checkingThatResultContains("")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinAggregateMaterializationAggregateFuncs1)
{
    GTEST_SKIP() << "join rewrite is not implemented.";
    // This test relies on FK-UK relationship
    sql("select empid, depts.deptno, count(*) as c, sum(empid) as s\n"
            "from emps join depts on emps.deptno = depts.deptno\n"
            "group by empid, depts.deptno",
        "select deptno from emps group by deptno")
        .checkingThatResultContains(""
                                    "EnumerableAggregate(group=[{1}])\n"
                                    "  EnumerableTableScan(table=[[hr, MV0]])")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinAggregateMaterializationAggregateFuncs2)
{
    sql("select empid, emps.deptno, count(*) as c, sum(empid) as s\n"
            "from emps join depts on emps.deptno = depts.deptno\n"
            "group by empid, emps.deptno",
        "select depts.deptno, count(*) as c, sum(empid) as s\n"
            "from emps join depts on emps.deptno = depts.deptno\n"
            "group by depts.deptno")
        .checkingThatResultContains("Aggregates: expr#sum(expr#count())_2:=AggNull(sum)(expr#count()_2), expr#sum(expr#sum(empid))_2:=AggNull(sum)(expr#sum(empid)_2)")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinAggregateMaterializationAggregateFuncs3)
{
    GTEST_SKIP() << "join rewrite is not implemented.";
    // This test relies on FK-UK relationship
    sql("select empid, depts.deptno, count(*) as c, sum(empid) as s\n"
            "from emps join depts on emps.deptno = depts.deptno\n"
            "group by empid, depts.deptno",
        "select deptno, empid, sum(empid) as s, count(*) as c\n"
            "from emps group by empid, deptno")
        .checkingThatResultContains(""
                                    "EnumerableCalc(expr#0..3=[{inputs}], deptno=[$t1], empid=[$t0], S=[$t3], C=[$t2])\n"
                                    "  EnumerableTableScan(table=[[hr, MV0]])")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinAggregateMaterializationAggregateFuncs4)
{
    sql("select empid, emps.deptno, count(*) as c, sum(empid) as s\n"
            "from emps join depts on emps.deptno = depts.deptno\n"
            "where emps.deptno >= 10 group by empid, emps.deptno",
        "select depts.deptno, sum(empid) as s\n"
            "from emps join depts on emps.deptno = depts.deptno\n"
            "where emps.deptno > 10 group by depts.deptno")
        .checkingThatResultContains("Where: deptno > 10")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinAggregateMaterializationAggregateFuncs5)
{
    sql("select empid, depts.deptno, count(*) + 1 as c, sum(empid) as s\n"
            "from emps join depts on emps.deptno = depts.deptno\n"
            "where depts.deptno >= 10 group by empid, depts.deptno",
        "select depts.deptno, sum(empid) + 1 as s\n"
            "from emps join depts on emps.deptno = depts.deptno\n"
            "where depts.deptno > 10 group by depts.deptno")
        .checkingThatResultContains("Where: `depts.deptno` > 10")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinAggregateMaterializationAggregateFuncs6)
{
    GTEST_SKIP() << "join compensation rewrite is not implemented.";
    // This rewriting would be possible if planner generates a pre-aggregation,
    // since the materialized view would match the sub-query.
    // Initial investigation after enabling AggregateJoinTransposeRule.EXTENDED
    // shows that the rewriting with pre-aggregations is generated and the
    // materialized view rewriting happens.
    // However, we end up discarding the plan with the materialized view and still
    // using the plan with the pre-aggregations.
    // TODO: Explore and extend to choose best rewriting.
    String m = "select depts.name, sum(salary) as s\n"
        "from emps\n"
        "join depts on (emps.deptno = depts.deptno)\n"
        "group by depts.name";
    String q = "select dependents.empid, sum(salary) as s\n"
        "from emps\n"
        "join depts on (emps.deptno = depts.deptno)\n"
        "join dependents on (depts.name = dependents.name)\n"
        "group by dependents.empid";
    sql(m, q).ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinAggregateMaterializationAggregateFuncs7)
{
    GTEST_SKIP() << "join compensation rewrite is not implemented.";
    sql("select dependents.empid, emps.deptno, sum(salary) as s\n"
            "from emps\n"
            "join dependents on (emps.empid = dependents.empid)\n"
            "group by dependents.empid, emps.deptno",
        "select dependents.empid, sum(salary) as s\n"
            "from emps\n"
            "join depts on (emps.deptno = depts.deptno)\n"
            "join dependents on (emps.empid = dependents.empid)\n"
            "group by dependents.empid")
        .checkingThatResultContains(""
                                    "EnumerableAggregate(group=[{0}], S=[$SUM0($2)])\n"
                                    "  EnumerableHashJoin(condition=[=($1, $3)], joinType=[inner])\n"
                                    "    EnumerableTableScan(table=[[hr, MV0]])\n"
                                    "    EnumerableTableScan(table=[[hr, depts]])")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinAggregateMaterializationAggregateFuncs8)
{
    GTEST_SKIP() << "join compensation rewrite is not implemented.";
    sql("select dependents.empid, emps.deptno, sum(salary) as s\n"
            "from emps\n"
            "join dependents on (emps.empid = dependents.empid)\n"
            "group by dependents.empid, emps.deptno",
        "select depts.name, sum(salary) as s\n"
            "from emps\n"
            "join depts on (emps.deptno = depts.deptno)\n"
            "join dependents on (emps.empid = dependents.empid)\n"
            "group by depts.name")
        .checkingThatResultContains(""
                                    "EnumerableAggregate(group=[{4}], S=[$SUM0($2)])\n"
                                    "  EnumerableHashJoin(condition=[=($1, $3)], joinType=[inner])\n"
                                    "    EnumerableTableScan(table=[[hr, MV0]])\n"
                                    "    EnumerableTableScan(table=[[hr, depts]])")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinAggregateMaterializationAggregateFuncs9)
{
    sql("select dependents.empid, emps.deptno, uniqState(salary) as s\n"
            "from emps\n"
            "join dependents on (emps.empid = dependents.empid)\n"
            "group by dependents.empid, emps.deptno",
        "select emps.deptno, uniq(salary) as s\n"
            "from emps\n"
            "join dependents on (emps.empid = dependents.empid)\n"
            "group by dependents.empid, emps.deptno")
        .checkingThatResultContains("uniqMerge")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinAggregateMaterializationAggregateFuncs10)
{
    GTEST_SKIP() << "join rewrite is not implemented.";
    sql("select dependents.empid, emps.deptno, count(distinct salary) as s\n"
            "from emps\n"
            "join dependents on (emps.empid = dependents.empid)\n"
            "group by dependents.empid, emps.deptno",
        "select emps.deptno, count(distinct salary) as s\n"
            "from emps\n"
            "join dependents on (emps.empid = dependents.empid)\n"
            "group by emps.deptno")
        .noMat();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinAggregateMaterializationAggregateFuncs11)
{
    sql("select depts.deptno, dependents.empid, count(emps.salary) as s\n"
            "from depts\n"
            "join dependents on (depts.name = dependents.name)\n"
            "join locations on (locations.name = dependents.name)\n"
            "join emps on (emps.deptno = depts.deptno)\n"
            "where depts.deptno > 11 and depts.deptno < 19\n"
            "group by depts.deptno, dependents.empid",
        "select dependents.empid, count(emps.salary) + 1\n"
            "from depts\n"
            "join dependents on (depts.name = dependents.name)\n"
            "join locations on (locations.name = dependents.name)\n"
            "join emps on (emps.deptno = depts.deptno)\n"
            "where depts.deptno > 10 and depts.deptno < 20\n"
            "group by dependents.empid")
        .checkingThatResultContains("Union")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinAggregateMaterializationAggregateFuncs12)
{
    sql("select depts.deptno, dependents.empid, "
            "count(distinct emps.salary) as s\n"
            "from depts\n"
            "join dependents on (depts.name = dependents.name)\n"
            "join locations on (locations.name = dependents.name)\n"
            "join emps on (emps.deptno = depts.deptno)\n"
            "where depts.deptno > 11 and depts.deptno < 19\n"
            "group by depts.deptno, dependents.empid",
        "select dependents.empid, count(distinct emps.salary) + 1\n"
            "from depts\n"
            "join dependents on (depts.name = dependents.name)\n"
            "join locations on (locations.name = dependents.name)\n"
            "join emps on (emps.deptno = depts.deptno)\n"
            "where depts.deptno > 10 and depts.deptno < 20\n"
            "group by dependents.empid")
        .noMat();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinAggregateMaterializationAggregateFuncs13)
{
    sql("select dependents.empid, emps.deptno, count(distinct salary) as s\n"
            "from emps\n"
            "join dependents on (emps.empid = dependents.empid)\n"
            "group by dependents.empid, emps.deptno",
        "select emps.deptno, count(salary) as s\n"
            "from emps\n"
            "join dependents on (emps.empid = dependents.empid)\n"
            "group by dependents.empid, emps.deptno")
        .noMat();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinAggregateMaterializationAggregateFuncs14)
{
    sql("select empid, emps.name, emps.deptno, depts.name, "
            "count(*) as c, sum(empid) as s\n"
            "from emps join depts on emps.deptno = depts.deptno\n"
            "where (depts.name is not null and emps.name = 'a') or "
            "(depts.name is not null and emps.name = 'b')\n"
            "group by empid, emps.name, depts.name, emps.deptno",
        "select depts.deptno, sum(empid) as s\n"
            "from emps join depts on emps.deptno = depts.deptno\n"
            "where depts.name is not null and emps.name = 'a'\n"
            "group by depts.deptno")
        .ok();
}

/** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4276">[CALCITE-4276]
   * If query contains join and rollup function (FLOOR), rewrite to materialized
   * view contains bad field offset</a>. */
TEST_F(MaterializedViewRewriteComplexTest, testJoinAggregateMaterializationAggregateFuncs15)
{
    GTEST_SKIP() << "time function rollup rewrite is not implemented.";
    String m = ""
        "SELECT deptno,\n"
        "  COUNT(*) AS dept_size,\n"
        "  SUM(salary) AS dept_budget\n"
        "FROM emps\n"
        "GROUP BY deptno";
    String q = ""
        "SELECT FLOOR(CREATED_AT TO YEAR) AS by_year,\n"
        "  COUNT(*) AS num_emps\n"
        "FROM (SELECT deptno\n"
        "    FROM emps) AS t\n"
        "JOIN (SELECT deptno,\n"
        "        inceptionDate as CREATED_AT\n"
        "    FROM depts2) on emps.deptno = depts.deptno\n"
        "GROUP BY FLOOR(CREATED_AT TO YEAR)";
    String plan = ""
        "EnumerableAggregate(group=[{8}], num_emps=[$SUM0($1)])\n"
        "  EnumerableCalc(expr#0..7=[{inputs}], expr#8=[FLAG(YEAR)], "
        "expr#9=[FLOOR($t3, $t8)], proj#0..7=[{exprs}], $f8=[$t9])\n"
        "    EnumerableHashJoin(condition=[=($0, $4)], joinType=[inner])\n"
        "      EnumerableTableScan(table=[[hr, MV0]])\n"
        "      EnumerableTableScan(table=[[hr, depts2]])\n";
    sql(m, q)
        .checkingThatResultContains(plan)
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinMaterialization1)
{
    String q = "select *\n"
        "from (select * from emps where empid < 300) t1\n"
        "join depts on t1.deptno = depts.deptno";
    sql("select * from emps where empid < 500", q).ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinMaterialization2)
{
    GTEST_SKIP() << "join compensation rewrite is not implemented.";
    String q = "select *\n"
        "from emps\n"
        "join depts on emps.deptno = depts.deptno";
    String m = "select deptno, empid, name,\n"
        "salary, commission from emps";
    sql(m, q).ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinMaterialization3)
{
    String q = "select empid deptno from emps\n"
        "join depts on emps.deptno = depts.deptno where empid = 1";
    String m = "select empid deptno from emps\n"
        "join depts on emps.deptno = depts.deptno";
    sql(m, q).ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinMaterialization4)
{
    sql("select empid deptno from emps\n"
            "join depts on emps.deptno = depts.deptno",
        "select empid deptno from emps\n"
            "join depts on emps.deptno = depts.deptno where empid = 1")
        .checkingThatResultContains("Where: deptno = 1")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinMaterialization5)
{
    GTEST_SKIP() << "cast rewrite is not implemented.";
    sql("select cast(empid as BIGINT) from emps\n"
            "join depts on emps.deptno = depts.deptno",
        "select empid deptno from emps\n"
            "join depts on emps.deptno = depts.deptno where empid > 1")
        .checkingThatResultContains("not implemented")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinMaterialization6)
{
    GTEST_SKIP() << "cast rewrite is not implemented.";
    sql("select cast(empid as BIGINT) from emps\n"
            "join depts on emps.deptno = depts.deptno",
        "select empid deptno from emps\n"
            "join depts on emps.deptno = depts.deptno where empid = 1")
        .checkingThatResultContains("not implemented")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinMaterialization7)
{
    sql("select depts.name\n"
            "from emps\n"
            "join depts on (emps.deptno = depts.deptno)",
        "select dependents.empid\n"
            "from emps\n"
            "join depts on (emps.deptno = depts.deptno)\n"
            "join dependents on (depts.name = dependents.name)")
        .checkingThatResultContains("Inner Join")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinMaterialization8)
{
    GTEST_SKIP() << "join compensation rewrite is not implemented.";
    sql("select depts.name\n"
            "from emps\n"
            "join depts on (emps.deptno = depts.deptno)",
        "select dependents.empid\n"
            "from depts\n"
            "join dependents on (depts.name = dependents.name)\n"
            "join emps on (emps.deptno = depts.deptno)")
        .checkingThatResultContains("not implemented")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinMaterialization9)
{
    GTEST_SKIP() << "join compensation rewrite is not implemented.";
    sql("select depts.name\n"
            "from emps\n"
            "join depts on (emps.deptno = depts.deptno)",
        "select dependents.empid\n"
            "from depts\n"
            "join dependents on (depts.name = dependents.name)\n"
            "join locations on (locations.name = dependents.name)\n"
            "join emps on (emps.deptno = depts.deptno)")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinMaterialization10)
{
    sql("select depts.deptno, dependents.empid\n"
            "from depts\n"
            "join dependents on (depts.name = dependents.name)\n"
            "join emps on (emps.deptno = depts.deptno)\n"
            "where depts.deptno > 30",
        "select dependents.empid\n"
            "from depts\n"
            "join dependents on (depts.name = dependents.name)\n"
            "join emps on (emps.deptno = depts.deptno)\n"
            "where depts.deptno > 10")
        .checkingThatResultContains("Union")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinMaterialization11)
{
    sql("select empid from emps\n"
            "join depts on emps.deptno = depts.deptno",
        "select empid from emps\n"
            "where deptno in (select deptno from depts)")
        .noMat();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinMaterialization12)
{
    sql("select empid, emps.name, emps.deptno, depts.name\n"
            "from emps join depts on emps.deptno = depts.deptno\n"
            "where (depts.name is not null and emps.name = 'a') or "
            "(depts.name is not null and emps.name = 'b') or "
            "(depts.name is not null and emps.name = 'c')",
        "select depts.deptno, depts.name\n"
            "from emps join depts on emps.deptno = depts.deptno\n"
            "where (depts.name is not null and emps.name = 'a') or "
            "(depts.name is not null and emps.name = 'b')")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinMaterializationUKFK1)
{
    GTEST_SKIP() << "join rewrite is not implemented.";
    sql("select a.empid deptno from\n"
            "(select * from emps where empid = 1) a\n"
            "join depts on emps.deptno = depts.deptno\n"
            "join dependents on emps.empid = dependents.empid",
        "select a.empid from \n"
            "(select * from emps where empid = 1) a\n"
            "join dependents on emps.empid = dependents.empid")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinMaterializationUKFK2)
{
    GTEST_SKIP() << "join rewrite is not implemented.";
    sql("select a.empid, a.deptno from\n"
            "(select * from emps where empid = 1) a\n"
            "join depts on emps.deptno = depts.deptno\n"
            "join dependents on emps.empid = dependents.empid",
        "select a.empid from \n"
            "(select * from emps where empid = 1) a\n"
            "join dependents on emps.empid = dependents.empid\n")
        .checkingThatResultContains(""
                                    "EnumerableCalc(expr#0..1=[{inputs}], empid=[$t0])\n"
                                    "  EnumerableTableScan(table=[[hr, MV0]])")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinMaterializationUKFK3)
{
    GTEST_SKIP() << "join rewrite is not implemented.";
    sql("select a.empid, a.deptno from\n"
            "(select * from emps where empid = 1) a\n"
            "join depts on emps.deptno = depts.deptno\n"
            "join dependents on emps.empid = dependents.empid",
        "select a.name from \n"
            "(select * from emps where empid = 1) a\n"
            "join dependents on emps.empid = dependents.empid\n")
        .noMat();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinMaterializationUKFK4)
{
    GTEST_SKIP() << "join rewrite is not implemented.";
    sql("select empid deptno from\n"
            "(select * from emps where empid = 1)\n"
            "join depts on emps.deptno = depts.deptno",
        "select empid from emps where empid = 1\n")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinMaterializationUKFK5)
{
    GTEST_SKIP() << "join rewrite is not implemented.";
    sql("select emps.empid, emps.deptno from emps\n"
            "join depts on emps.deptno = depts.deptno\n"
            "join dependents on emps.empid = dependents.empid"
            "where emps.empid = 1",
        "select emps.empid from emps\n"
            "join dependents on emps.empid = dependents.empid\n"
            "where emps.empid = 1")
        .checkingThatResultContains(""
                                    "EnumerableCalc(expr#0..1=[{inputs}], empid=[$t0])\n"
                                    "  EnumerableTableScan(table=[[hr, MV0]])")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinMaterializationUKFK6)
{
    GTEST_SKIP() << "join rewrite is not implemented.";
    sql("select emps.empid, emps.deptno from emps\n"
            "join depts a on (emps.deptno=a.deptno)\n"
            "join depts b on (emps.deptno=b.deptno)\n"
            "join dependents on emps.empid = dependents.empid"
            "where emps.empid = 1",
        "select emps.empid from emps\n"
            "join dependents on emps.empid = dependents.empid\n"
            "where emps.empid = 1")
        .checkingThatResultContains(""
                                    "EnumerableCalc(expr#0..1=[{inputs}], empid=[$t0])\n"
                                    "  EnumerableTableScan(table=[[hr, MV0]])")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinMaterializationUKFK7)
{
    GTEST_SKIP() << "join rewrite is not implemented.";
    sql("select emps.empid, emps.deptno from emps\n"
            "join depts a on (emps.name=a.name)\n"
            "join depts b on (emps.name=b.name)\n"
            "join dependents on emps.empid = dependents.empid"
            "where emps.empid = 1",
        "select emps.empid from emps\n"
            "join dependents on emps.empid = dependents.empid\n"
            "where emps.empid = 1")
        .noMat();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinMaterializationUKFK8)
{
    GTEST_SKIP() << "join rewrite is not implemented.";
    sql("select emps.empid, emps.deptno from emps\n"
            "join depts a on (emps.deptno=a.deptno)\n"
            "join depts b on (emps.name=b.name)\n"
            "join dependents on emps.empid = dependents.empid"
            "where emps.empid = 1",
        "select emps.empid from emps\n"
            "join dependents on emps.empid = dependents.empid\n"
            "where emps.empid = 1")
        .noMat();
}

TEST_F(MaterializedViewRewriteComplexTest, testJoinMaterializationUKFK9)
{
    GTEST_SKIP() << "join rewrite is not implemented.";
    sql("select * from emps\n"
            "join dependents on emps.empid = dependents.empid",
        "select emps.empid, dependents.empid, emps.deptno\n"
            "from emps\n"
            "join dependents on emps.empid = dependents.empid"
            "join depts a on (emps.deptno=a.deptno)\n"
            "where emps.name = 'Bill'")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testQueryProjectWithBetween)
{
    sql("select *"
        " from foodmart.sales_fact_1997 as s"
        " where s.store_id = 1",
        "select s.time_id between 1 and 3"
            " from foodmart.sales_fact_1997 as s"
            " where s.store_id = 1")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, DISABLED_testJoinQueryProjectWithBetween)
{
    sql("select *"
            " from foodmart.sales_fact_1997 as s"
            " join foodmart.time_by_day as t on s.time_id = t.time_id"
            " where s.store_id = 1",
        "select s.time_id between 1 and 3"
            " from foodmart.sales_fact_1997 as s"
            " join foodmart.time_by_day as t on s.time_id = t.time_id"
            " where s.store_id = 1")
        .checkingThatResultContains("Gather Exchange\n"
                                    "└─ Projection\n"
                                    "   │     Expressions: and(greaterOrEquals(s.time_id, 1), lessOrEquals(s.time_id, 3)):=(time_id >= 1) AND (time_id <= 3)\n"
                                    "   └─ TableScan test_mview.MV0_MV_DATA\n"
                                    "            Outputs: [time_id]")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testViewProjectWithBetween)
{
    sql("select s.time_id, s.time_id between 1 and 3"
        " from foodmart.sales_fact_1997 as s"
        " where s.store_id = 1",
        "select s.time_id"
        " from foodmart.sales_fact_1997 as s"
        " where s.store_id = 1")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testQueryAndViewProjectWithBetween)
{
    sql("select s.time_id, s.time_id between 1 and 3"
        " from foodmart.sales_fact_1997 as s"
        " where s.store_id = 1",
        "select s.time_id between 1 and 3"
        " from foodmart.sales_fact_1997 as s"
        " where s.store_id = 1")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testViewProjectWithMultifieldExpressions)
{
    sql("select s.time_id, s.time_id >= 1 and s.time_id < 3,"
        " s.time_id >= 1 or s.time_id < 3, "
        " s.time_id + s.time_id, "
        " s.time_id * s.time_id"
        " from foodmart.sales_fact_1997 as s"
        " where s.store_id = 1",
        "select s.time_id"
        " from foodmart.sales_fact_1997 as s"
        " where s.store_id = 1")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateOnJoinKeys)
{
    GTEST_SKIP() << "aggregate on join keys rollup rewrite is not implemented.";
    sql("select deptno, empid, salary "
            "from emps\n"
            "group by deptno, empid, salary",
        "select empid, depts.deptno "
            "from emps\n"
            "join depts on depts.deptno = empid group by empid, depts.deptno")
        .checkingThatResultContains(""
                                    "EnumerableCalc(expr#0=[{inputs}], empid=[$t0], empid0=[$t0])\n"
                                    "  EnumerableAggregate(group=[{1}])\n"
                                    "    EnumerableHashJoin(condition=[=($1, $3)], joinType=[inner])\n"
                                    "      EnumerableTableScan(table=[[hr, MV0]])\n"
                                    "      EnumerableTableScan(table=[[hr, depts]])")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateOnJoinKeys2)
{
    GTEST_SKIP() << "join compensation rewrite is not implemented.";
    sql("select deptno, empid, salary, sum(1) "
            "from emps\n"
            "group by deptno, empid, salary",
        "select sum(1) "
            "from emps\n"
            "join depts on depts.deptno = empid group by empid, depts.deptno")
        .checkingThatResultContains(""
                                    "EnumerableCalc(expr#0..1=[{inputs}], EXPR$0=[$t1])\n"
                                    "  EnumerableAggregate(group=[{1}], EXPR$0=[$SUM0($3)])\n"
                                    "    EnumerableHashJoin(condition=[=($1, $4)], joinType=[inner])\n"
                                    "      EnumerableTableScan(table=[[hr, MV0]])\n"
                                    "      EnumerableTableScan(table=[[hr, depts]])")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationOnCountDistinctQuery1)
{
    // The column empid is already unique, thus DISTINCT is not
    // in the COUNT of the resulting rewriting
    sql("select deptno, empid, salary\n"
        "from emps\n"
        "group by deptno, empid, salary",
        "select deptno, count(distinct empid) as c from (\n"
        "select deptno, empid\n"
        "from emps\n"
        "group by deptno, empid)\n"
        "group by deptno")
        .checkingThatResultContains("Aggregates: expr#uniqExact(empid")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationOnCountDistinctQuery2)
{
    // The column empid is already unique, thus DISTINCT is not
    // in the COUNT of the resulting rewriting
    sql("select deptno, salary, empid\n"
        "from emps\n"
        "group by deptno, salary, empid",
        "select deptno, count(distinct empid) as c from (\n"
        "select deptno, empid\n"
        "from emps\n"
        "group by deptno, empid)\n"
        "group by deptno")
        .checkingThatResultContains("Aggregates: expr#uniqExact(empid")
        .ok();
}

TEST_F(MaterializedViewRewriteComplexTest, testAggregateMaterializationOnCountDistinctQuery3)
{
    // The column salary is not unique, thus we end up with
    // a different rewriting
    sql("select deptno, empid, salary\n"
        "from emps\n"
        "group by deptno, empid, salary",
        "select deptno, count(distinct salary) from (\n"
        "select deptno, salary\n"
        "from emps\n"
        "group by deptno, salary)\n"
        "group by deptno")
        .checkingThatResultContains("Aggregates: expr#uniqExact(salary)_3:=AggNull(uniqExact)(salary_2)")
        .ok();
}
