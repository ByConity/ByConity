#pragma once

#include <Optimizer/tests/gtest_base_plan_test.h>
#include <Optimizer/tests/test_config.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <../base/daemon/BaseDaemon.h>

namespace DB
{

/**
 * change resource path in test.config.h.in.
 */
class BaseDumpPlanTest : public AbstractPlanTestSuite
{
public:
    explicit BaseDumpPlanTest(
        const std::unordered_map<String, Field> & settings = {})
        : AbstractPlanTestSuite("test_dump", settings) {
        createTables();
    }

    std::vector<std::filesystem::path> getTableDDLFiles() override { return {PLAN_DUMP_TABLE_DDL_FILE}; }
    std::filesystem::path getStatisticsFile() override { return PLAN_DUMP_TABLE_STATISTICS_FILE; }
    std::filesystem::path getQueriesDir() override { return PLAN_DUMP_QUERIES_DIR; }
    std::filesystem::path getExpectedExplainDir() override { return PLAN_DUMP_PATH; }

    void cleanDumpFiles(const std::string & query_id)
    {
        try
        {
            if (std::filesystem::exists(PLAN_DUMP_PATH))
            {
                String zip_file = PLAN_DUMP_PATH + query_id + ".zip";
                String dir_file = PLAN_DUMP_PATH + query_id + "/";
                std::filesystem::remove_all(dir_file.c_str());
                std::filesystem::remove_all(zip_file.c_str());
            }
        }
        catch (...)
        {
        }
    }

    // fixme: currently unit test is run without Poco::Util::Application::instance()
    // this causes Context.getConfigRef() to throw NPE
    void createOriginalTables()
    {
        auto context = createQueryContext();

        auto drop = std::make_shared<ASTDropQuery>();
        drop->uuid = UUIDHelpers::Nil;
        drop->database = "test_dump";
        drop->if_exists = true;
        InterpreterDropQuery drop_interpreter(drop, context);
        drop_interpreter.execute();

        context = createQueryContext();
        auto create_db = std::make_shared<ASTCreateQuery>();
        create_db->uuid = UUIDHelpers::Nil;
        create_db->database = "test_dump";
        InterpreterCreateQuery create_db_interpreter(create_db, context);
        create_db_interpreter.execute();

        for (auto & file : getTableDDLFiles())
        {
            for (auto & ddl : loadFile(file, ';'))
            {
                auto query_context = createQueryContext();
                query_context->applySettingsChanges({DB::SettingChange("dialect_type", "CLICKHOUSE")});
                ASTPtr ast = parse(ddl, query_context);

                if (auto * create = ast->as<ASTCreateQuery>())
                {
                    create->uuid = UUIDHelpers::Nil;
                    InterpreterCreateQuery create_interpreter(ast, query_context);
                    create_interpreter.execute();
                }

            }
        }
    }
};

}
