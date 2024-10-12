#include <Advisor/Rules/PartitionKeyAdvise.h>

#include <Advisor/AdvisorContext.h>

#include <gtest/gtest.h>
#include <Advisor/tests/gtest_workload_test.h>

using namespace DB;

class PartitionKeyTest : public ::testing::Test
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
                        ") ENGINE=CnchMergeTree() order by empid;");
    }

    static std::shared_ptr<BaseWorkloadTest> tester;
};

std::shared_ptr<BaseWorkloadTest> PartitionKeyTest::tester;


TEST_F(PartitionKeyTest, testFrequencyBasedAdvise)
{
    auto context = tester->createQueryContext();
    std::vector<std::string> sqls{"select * from emps e1, emps e2 where e1.empid=e2.empid"};
    ThreadPool query_thread_pool{1};
    WorkloadQueries queries = WorkloadQuery::build(std::move(sqls), context, query_thread_pool);
    WorkloadTables tables(context);
    AdvisorContext advisor_context = AdvisorContext::buildFrom(context, tables, queries, query_thread_pool);
    auto advise = PartitionKeyAdvisor().frequencyBasedAdvise(advisor_context);
    EXPECT_EQ(advise.size(), 1);
    QualifiedTableName emps{tester->getDatabaseName(), "emps"};
    EXPECT_EQ(advise[0]->getTable(), emps);
    EXPECT_EQ(advise[0]->getOptimizedValue(), "empid");
}

TEST_F(PartitionKeyTest, testUpdateClusterBy)
{
    std::string database = tester->getDatabaseName();
    std::string create_table_ddl = "CREATE TABLE IF NOT EXISTS " + database
        + ".emps("
          "  empid UInt32 not null,"
          "  deptno UInt32 not null,"
          "  name Nullable(String),"
          "  salary Nullable(Float64),"
          "  commission Nullable(UInt32)"
          ") ENGINE=CnchMergeTree()"
          " cluster by deptno into 128 buckets"
          " order by deptno;";

    auto query_context = tester->createQueryContext();
    auto create_ast = tester->parse(create_table_ddl, query_context);
    WorkloadTable table(nullptr, create_ast, WorkloadTableStats::build(query_context, tester->getDatabaseName(), "emps"));
    table.updateClusterByKey(std::make_shared<ASTIdentifier>("empid"));

    std::string optimal_ddl = serializeAST(*table.getDDL());
    std::cout << optimal_ddl << std::endl;
    EXPECT_TRUE(optimal_ddl.find("CLUSTER BY deptno") == std::string::npos);
    EXPECT_TRUE(optimal_ddl.find("CLUSTER BY empid INTO 128 BUCKETS") != std::string::npos);
}

TEST_F(PartitionKeyTest, testColocatedJoin)
{
    tester->execute("CREATE TABLE IF NOT EXISTS A(a UInt32 not null, b UInt32 not null) ENGINE=Memory");
    auto context = tester->createQueryContext();
    context->applySettingsChanges({DB::SettingChange("cascades_optimizer_timeout", "2000000")});
    context->applySettingsChanges({DB::SettingChange("enum_replicate_no_stats", "0")});
    std::vector<std::string> sqls{"select * from A A1, A A2 where A1.a = A2.a;", "select * from A A1, A A2 where A1.a = A2.a;"};
    ThreadPool query_thread_pool{std::min<size_t>(size_t(context->getSettingsRef().max_threads), sqls.size())};
    WorkloadQueries queries = WorkloadQuery::build(std::move(sqls), context, query_thread_pool);

    TableLayout no_partition{};
    double cost_no_partition = 0.0;
    for (auto & query : queries)
        cost_no_partition += query->getOptimalCost(no_partition);

    TableLayout wrong_partition{{QualifiedTableName{tester->getDatabaseName(), "A"},
                                 WorkloadTablePartitioning{QualifiedColumnName{tester->getDatabaseName(), "A", "b"}}}};
    double cost_wrong_partition = 0.0;
    for (auto & query : queries)
        cost_wrong_partition += query->getOptimalCost(wrong_partition);

    TableLayout arbitrary_partition{{QualifiedTableName{tester->getDatabaseName(), "A"}, WorkloadTablePartitioning::starPartition()}};
    double cost_arbitrary_partition = 0.0;
    for (auto & query : queries)
        cost_arbitrary_partition += query->getOptimalCost(arbitrary_partition);

    TableLayout correct_partition{{QualifiedTableName{tester->getDatabaseName(), "A"},
                                   WorkloadTablePartitioning{QualifiedColumnName{tester->getDatabaseName(), "A", "a"}}}};
    double cost_correct_partition = 0.0;
    for (auto & query : queries)
        cost_correct_partition += query->getOptimalCost(correct_partition);

    std::cout << cost_no_partition << ' ' << cost_wrong_partition << ' '
              << cost_arbitrary_partition << ' ' << cost_correct_partition << std::endl;
    EXPECT_EQ(cost_no_partition, cost_wrong_partition);
    // EXPECT_EQ(cost_arbitrary_partition, cost_correct_partition);
    EXPECT_TRUE(cost_correct_partition < cost_no_partition);
}

TEST_F(PartitionKeyTest, testMemoBasedAdvise)
{
    tester->execute("CREATE TABLE IF NOT EXISTS A(a UInt32 not null, b UInt32 not null) ENGINE=Memory");
    std::vector<std::string> sqls{"select * from A A1, A A2 where A1.a = A2.a;", "select * from A A1, A A2 where A1.a = A2.a;"};
    auto context = tester->createQueryContext();
    context->applySettingsChanges({DB::SettingChange("enum_replicate_no_stats", "0")});
    ThreadPool query_thread_pool{std::min<size_t>(size_t(context->getSettingsRef().max_threads), sqls.size())};
    WorkloadQueries queries = WorkloadQuery::build(sqls, context, query_thread_pool);
    WorkloadTables tables(context);

    AdvisorContext advisor_context = AdvisorContext::buildFrom(context, tables, queries, query_thread_pool);
    auto advise = PartitionKeyAdvisor().memoBasedAdvise(advisor_context);
    ASSERT_EQ(advise.size(), 1);
    QualifiedTableName table_A{tester->getDatabaseName(), "A"};
    EXPECT_EQ(advise[0]->getTable(), table_A);
    EXPECT_EQ(advise[0]->getOptimizedValue(), "a");
}


