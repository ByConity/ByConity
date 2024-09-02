#include <Common/SettingsChanges.h>
#include <QueryPlan/PlanPrinter.h>
#include <Optimizer/Dump/DDLDumper.h>
#include <Optimizer/Dump/QueryDumper.h>
#include <Optimizer/Dump/PlanReproducer.h>
#include <Optimizer/CardinalityEstimate/CardinalityEstimator.h>
#include <Optimizer/CostModel/CostCalculator.h>

#include <Optimizer/tests/gtest_dump_plan_test.h>
#include <gtest/gtest.h>

class PlanDumpTest : public ::testing::Test
{
public:
    static void SetUpTestSuite()
    {
        std::unordered_map<std::string, DB::Field> settings;
#ifndef NDEBUG
        // debug mode may time out.
        settings.emplace("cascades_optimizer_timeout", "300000");
#endif

        tester = std::make_shared<DB::BaseDumpPlanTest>(settings);
    }

    static testing::AssertionResult checkFile(const std::string & file_actual, const std::string & file_expected)
    {
        try
        {
            auto actual_path = std::filesystem::path(file_actual);
            if (!std::filesystem::exists(actual_path))
                return testing::AssertionFailure() << "\n File Not Found. " << file_actual;
            auto expected_path = std::filesystem::path(file_expected);
            if (!std::filesystem::exists(expected_path))
                return testing::AssertionFailure() << "\n File Not Found. " << file_expected;

            std::ifstream in(expected_path);
            std::ostringstream ss;
            ss << in.rdbuf();
            auto expected = ss.str();

            std::ifstream in_actual(actual_path);
            std::ostringstream ss_actual;
            ss_actual << in_actual.rdbuf();
            auto actual = ss_actual.str();

            if (expected != actual)
            {
                return testing::AssertionFailure() << "\nIn " << actual_path
                                                   << "\nExpected:\n" << expected << "\nActual:\n" << actual ;
            }
        } catch (...)
        {
            return testing::AssertionFailure() << "failed to open file in: " << file_actual << " or " << file_expected;
        }

        return testing::AssertionSuccess();
    }

    static std::shared_ptr<DB::BaseDumpPlanTest> tester;
};

std::shared_ptr<DB::BaseDumpPlanTest> PlanDumpTest::tester;


TEST_F(PlanDumpTest, testDumpWithoutStats)
{
    std::string query_id = "test";
    tester->cleanDumpFiles(query_id);
    auto query = tester->loadQuery(query_id);
    ASSERT_EQ(query.sql.size(), 1);
    std::string sql = query.sql.front().first;
    auto context = tester->createQueryContext();
    context->applySettingsChanges({DB::SettingChange("graphviz_path", PLAN_DUMP_PATH)});
    context->setCurrentQueryId(query_id);

    std::string dump_path = std::string(PLAN_DUMP_PATH) + query_id;
    std::string expected_folder = std::string(PLAN_DUMP_PATH) + "expected";

    DB::QueryDumper query_dumper(dump_path);
    DB::DDLDumper ddl_dumper(dump_path);

    DB::QueryDumper::Query query_elem(query_id, sql, context->getCurrentDatabase());
    auto & workload_query = query_dumper.add(query_elem);
    workload_query.settings = std::make_shared<DB::Settings>(context->getSettingsRef());

    DB::ContextMutablePtr query_context = workload_query.buildQueryContextFrom(context);
    DB::ASTPtr ast = DB::ReproduceUtils::parse(workload_query.query, query_context);

    ddl_dumper.addTableFromSelectQuery(ast, query_context);

    std::string explain = DB::ReproduceUtils::obtainExplainString(workload_query.query, query_context);
    workload_query.info.emplace(DB::DumpUtils::QueryInfo::explain, explain);

    // test dump ddl
    Poco::JSON::Object::Ptr dump_json(new Poco::JSON::Object);
    dump_json = ddl_dumper.getJsonDumpResult();
    DB::DumpUtils::writeJsonToAbsolutePath(*dump_json, dump_path + "/dump_result.json");
    EXPECT_TRUE(checkFile(dump_path + "/dump_result.json", expected_folder + "/dump_result.json"));

    // test reproduce query
    dump_json = ddl_dumper.getJsonDumpResult();
    dump_json->set("queries", query_dumper.getJsonDumpResult());
    DB::DumpUtils::writeJsonToAbsolutePath(*dump_json, dump_path + "/dump_result.json");
    DB::DumpUtils::zipDirectory(dump_path);
    DB::PlanReproducer reproducer(dump_path + ".zip", context);
    DB::PlanReproducer::Query reproduced_query = reproducer.getQuery(query_id);
    EXPECT_TRUE(sql == reproduced_query.query);

    tester->cleanDumpFiles(query_id);
}

TEST_F(PlanDumpTest, testDumpMaterializedView)
{
    const char * folder = "test";
    tester->execute("create table test_table(a Int32 not null, b Int32 not null) engine=CnchMergeTree() order by tuple()");
    tester->execute("create materialized view test_view(a Int32 not null, b Int32 not null) engine=CnchMergeTree() order by tuple() as select * from test_table");
    tester->execute(String{"dump compress_directory=1 ddl from "} + tester->getDefaultDatabase() + " into '" + PLAN_DUMP_PATH + folder + "'");
    auto database = DB::DatabaseCatalog::instance().tryGetDatabase(tester->getDefaultDatabase(), tester->createQueryContext());
    ASSERT_TRUE(database);
    database->detachTable("test_table");
    database->detachTable("test_view");
    EXPECT_FALSE(database->tryGetTable("test_view", tester->createQueryContext()));
    tester->execute(String{"reproduce ddl source '"} + PLAN_DUMP_PATH + folder + ".zip'");
    EXPECT_TRUE(database->tryGetTable("test_view", tester->createQueryContext()));
    tester->cleanDumpFiles(folder);
}
