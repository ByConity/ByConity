#include <Advisor/ColumnUsage.h>

#include <Advisor/WorkloadQuery.h>

#include <gtest/gtest.h>
#include <Advisor/tests/gtest_workload_test.h>

namespace DB
{

class ColumnUsageTest : public ::testing::Test
{
public:
    static void SetUpTestSuite()
    {
        tester = std::make_shared<BaseWorkloadTest>();
        tester->execute("CREATE TABLE IF NOT EXISTS emps("
                        "  empid UInt32 not null,"
                        "  deptno UInt32 not null,"
                        "  name Nullable(String),"
                        "  salary Nullable(Float64),"
                        "  commission Nullable(UInt32),"
                        "  history Array(UInt32)"
                        ") ENGINE=Memory();");
        tester->execute("CREATE TABLE IF NOT EXISTS depts("
                        "  deptno UInt32 not null,"
                        "  name Nullable(String)"
                        ") ENGINE=Memory();");
    }

    static void TearDownTestCase() { tester.reset(); }

    ColumnUsages buildColumnUsagesFromSQL(std::initializer_list<std::string> sql_list)
    {
        auto context = tester->createQueryContext();
        std::vector<std::string> sqls(sql_list);
        ThreadPool query_thread_pool{std::min<size_t>(size_t(context->getSettingsRef().max_threads), sqls.size())};
        WorkloadQueries queries = WorkloadQuery::build(std::move(sqls), context, query_thread_pool);
        return buildColumnUsages(queries);
    }

    static std::shared_ptr<BaseWorkloadTest> tester;
};

std::shared_ptr<BaseWorkloadTest> ColumnUsageTest::tester;

namespace
{
    using ColumnFrequencies = std::unordered_map<QualifiedColumnName, size_t, QualifiedColumnNameHash>;

    ColumnFrequencies getColumnFrequencies(const ColumnUsages & column_usages, ColumnUsageType type, bool is_source_table)
    {
        ColumnFrequencies res;
        for (const auto & [column, metrics] : column_usages)
        {
            auto usage = metrics.getFrequency(type, is_source_table);
            if (usage)
                res[column] += usage;
        }
        return res;
    }
}

TEST_F(ColumnUsageTest, testEmpty)
{
    auto column_usages = buildColumnUsagesFromSQL({"select 1"});
    EXPECT_TRUE(column_usages.empty());
}

TEST_F(ColumnUsageTest, testSelect)
{
    auto column_usages = buildColumnUsagesFromSQL({"select 1",
                                            "select * from emps where empid=1",
                                            "select empid from emps"});
    auto select_usages = getColumnFrequencies(column_usages, ColumnUsageType::SCANNED, true);
    auto empid_column = QualifiedColumnName{tester->getDatabaseName(), "emps", "empid"};
    EXPECT_EQ(select_usages.size(), 6);
    ASSERT_TRUE(select_usages.contains(empid_column));
    EXPECT_EQ(select_usages[empid_column], 2);
}

TEST_F(ColumnUsageTest, testJoin)
{
    auto column_usages = buildColumnUsagesFromSQL({"select * from (select deptno from depts) d1, (select deptno from depts d2)",
                                            "select * from emps, depts where emps.deptno = depts.deptno"});
    auto join_usages = getColumnFrequencies(column_usages, ColumnUsageType::EQUI_JOIN, true);
    auto deptno_column = QualifiedColumnName{tester->getDatabaseName(), "depts", "deptno"};
    EXPECT_EQ(join_usages.size(), 2); // only count inner join
    ASSERT_TRUE(join_usages.contains(deptno_column));
    EXPECT_EQ(join_usages[deptno_column], 1);
}

TEST_F(ColumnUsageTest, testNestedJoin)
{
    auto column_usages = buildColumnUsagesFromSQL({"select * from emps e1, emps e2, emps e3 where e1.empid = e2.empid and e2.deptno = e3.deptno"});

    auto join_usages = getColumnFrequencies(column_usages, ColumnUsageType::EQUI_JOIN, true);
    auto empid_column = QualifiedColumnName{tester->getDatabaseName(), "emps", "empid"};
    auto deptno_column = QualifiedColumnName{tester->getDatabaseName(), "emps", "deptno"};

    EXPECT_EQ(join_usages.size(), 2); // 2 joins
    ASSERT_TRUE(join_usages.contains(empid_column));
    ASSERT_TRUE(join_usages.contains(deptno_column));
    EXPECT_GE(join_usages[empid_column], 1);
    EXPECT_GE(join_usages[deptno_column], 1);
    EXPECT_EQ(join_usages[empid_column] + join_usages[deptno_column], 3);
}

TEST_F(ColumnUsageTest, testNestedJoinCountAll)
{
    tester->execute("CREATE TABLE IF NOT EXISTS A(a UInt32 not null, b UInt32 not null) ENGINE=Memory();");
    tester->execute("CREATE TABLE IF NOT EXISTS B(b UInt32 not null, c UInt32 not null) ENGINE=Memory();");
    tester->execute("CREATE TABLE IF NOT EXISTS C(c UInt32 not null, d UInt32 not null) ENGINE=Memory();");

    auto column_usages = buildColumnUsagesFromSQL({"select * from A, B, C where A.b = B.b and B.c = C.c"});

    auto source_table_join_usages = getColumnFrequencies(column_usages, ColumnUsageType::EQUI_JOIN, true);
    EXPECT_EQ(source_table_join_usages.size(), 3);
    auto all_join_usages = getColumnFrequencies(column_usages, ColumnUsageType::EQUI_JOIN, false);
    EXPECT_EQ(all_join_usages.size(), 4);
}

TEST_F(ColumnUsageTest, testGroupBy)
{
    auto column_usages = buildColumnUsagesFromSQL({"select sum(deptno) from emps group by empid"});

    auto select_usages = getColumnFrequencies(column_usages, ColumnUsageType::SCANNED, true);
    auto group_by_usages = getColumnFrequencies(column_usages, ColumnUsageType::GROUP_BY, true);
    auto empid_column = QualifiedColumnName{tester->getDatabaseName(), "emps", "empid"};
    ASSERT_TRUE(group_by_usages.contains(empid_column));
    EXPECT_EQ(group_by_usages[empid_column], 1);
    EXPECT_EQ(select_usages.size(), 2);
}

TEST_F(ColumnUsageTest, testFilter)
{
    auto column_usages = buildColumnUsagesFromSQL({"select empid from emps where empid = 1", // eq-usage +1
        "select empid from emps where empid > 2", // range-usage +1
        "select empid from emps where empid = 3 and empid < 4", // eq-usage +1 range-usage +1
        "select empid from emps where empid >= 5 and empid <= 6", // range-usage +2
        "select empid from emps where empid between 7 and 8", // range-usage +2
        "select empid from emps where empid = 9 and (empid > 10 or empid < 11)", // eq-usage +1
        "select empid from emps where empid = 12 or empid < 13", // nousage
    });
    auto empid_column = QualifiedColumnName{tester->getDatabaseName(), "emps", "empid"};

    auto equality_usages = getColumnFrequencies(column_usages, ColumnUsageType::EQUALITY_PREDICATE, true);
    ASSERT_TRUE(equality_usages.contains(empid_column));
    EXPECT_EQ(equality_usages[empid_column], 3);

    auto range_usages = getColumnFrequencies(column_usages, ColumnUsageType::RANGE_PREDICATE, true);
    ASSERT_TRUE(range_usages.contains(empid_column));
    EXPECT_EQ(range_usages[empid_column], 6);
}

TEST_F(ColumnUsageTest, testInFilter)
{
    auto column_usages = buildColumnUsagesFromSQL({"select empid from emps where empid in (1,2,3)"});
    auto empid_column = QualifiedColumnName{tester->getDatabaseName(), "emps", "empid"};

    auto in_usages = getColumnFrequencies(column_usages, ColumnUsageType::IN_PREDICATE, true);
    ASSERT_TRUE(in_usages.contains(empid_column));
    EXPECT_EQ(in_usages[empid_column], 1);
}

TEST_F(ColumnUsageTest, testArraySetFnction)
{
    auto column_usages = buildColumnUsagesFromSQL({"select if(arraySetCheck(history, (9000)), 'hint', 'miss') from emps "
                                                   "where has(history, 9000) and arraySetCheck(history, (9000)) = 1"});
    auto history_column = QualifiedColumnName{tester->getDatabaseName(), "emps", "history"};

    auto array_set_usages = getColumnFrequencies(column_usages, ColumnUsageType::ARRAY_SET_FUNCTION, true);
    ASSERT_TRUE(array_set_usages.contains(history_column));
    EXPECT_GE(array_set_usages[history_column], 2);
}

TEST_F(ColumnUsageTest, testPrewhere)
{
    auto column_usages = buildColumnUsagesFromSQL({"select empid from emps "
                                                   "prewhere arraySetCheck(history, (9000)) = 1 where empid in (1,2,3)"});
    auto history_column = QualifiedColumnName{tester->getDatabaseName(), "emps", "history"};

    auto array_set_usages = getColumnFrequencies(column_usages, ColumnUsageType::ARRAY_SET_FUNCTION, true);
    ASSERT_TRUE(array_set_usages.contains(history_column));
    EXPECT_GE(array_set_usages[history_column], 1);
}


} // namespace DB
