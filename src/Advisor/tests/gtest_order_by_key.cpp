#include <Advisor/Rules/OrderByKeyAdvise.h>

#include <Advisor/Rules/WorkloadAdvisor.h>
#include <Advisor/WorkloadQuery.h>
#include <Advisor/WorkloadTable.h>
#include <Parsers/formatAST.h>

#include <iostream>
#include <Advisor/tests/gtest_workload_test.h>
#include <gtest/gtest.h>

using namespace DB;

class OrderByKeyTest : public ::testing::Test
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
                             "  commission Nullable(UInt32)"
                             ") ENGINE=Memory();");
        tester->execute("CREATE TABLE IF NOT EXISTS depts("
                             "  deptno UInt32 not null,"
                             "  name Nullable(String)"
                             ") ENGINE=Memory();");
    }

    static void TearDownTestCase()
    {
        tester.reset();
    }

    static std::shared_ptr<BaseWorkloadTest> tester;
};

std::shared_ptr<BaseWorkloadTest> OrderByKeyTest::tester;

TEST_F(OrderByKeyTest, testSimple)
{
    auto context = tester->createQueryContext();
    std::vector<std::string> sqls(
        {"select 1", "select * from emps where empid=1", "select empid from emps where empid>1", "select * from emps where deptno=1"});
    ThreadPool query_thread_pool{std::min<size_t>(size_t(context->getSettingsRef().max_threads), sqls.size())};
    WorkloadQueries queries = WorkloadQuery::build(sqls, context, query_thread_pool);
    WorkloadTables tables(context);
    AdvisorContext advisor_context = AdvisorContext::buildFrom(context, tables, queries, query_thread_pool);
    auto advise = OrderByKeyAdvisor().analyze(advisor_context);
    EXPECT_EQ(advise.size(), 1);
    QualifiedTableName emps{tester->getDatabaseName(), "emps"};
    EXPECT_EQ(advise[0]->getTable(), emps);
    EXPECT_EQ(advise[0]->getOptimizedValue(), "empid");
}

TEST_F(OrderByKeyTest, testUpdateOrderBy)
{
    std::string database = tester->getDatabaseName();
    std::string table_ddl = "CREATE TABLE IF NOT EXISTS " + database
        + ".emps("
          "  empid UInt32 not null,"
          "  deptno UInt32 not null,"
          "  name Nullable(String),"
          "  salary Nullable(Float64),"
          "  commission Nullable(UInt32)"
          ") ENGINE=CnchMergeTree()"
          "order by deptno;";

    auto query_context = tester->createQueryContext();
    query_context->applySettingsChanges({DB::SettingChange("dialect_type", "CLICKHOUSE")});
    auto create_ast = tester->parse(table_ddl, query_context);
    WorkloadTable table(nullptr, create_ast, WorkloadTableStats::build(query_context, tester->getDatabaseName(), "emps"));
    table.updateOrderBy(std::make_shared<ASTIdentifier>("empid"));

    std::string local_ddl = serializeAST(*table.getDDL());
    std::cout << local_ddl << std::endl;
    EXPECT_TRUE(local_ddl.find("ORDER BY deptno") == std::string::npos);
    EXPECT_TRUE(local_ddl.find("ORDER BY empid") != std::string::npos);
}

