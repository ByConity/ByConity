#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Optimizer/tests/test_config.h>
#include <QueryPlan/QueryPlan.h>

#include <filesystem>
#include <memory>

namespace DB
{

class BasePlanTest : public std::enable_shared_from_this<BasePlanTest>
{
public:
    explicit BasePlanTest(const String & database = "testdb", const std::unordered_map<String, Field> & session_settings = {});

    virtual ~BasePlanTest() = default;

    ASTPtr parse(const std::string & query, ContextMutablePtr query_context);

    QueryPlanPtr plan(const String & query, ContextMutablePtr query_context);

    QueryPlanPtr plan(const String & query) { return plan(query, createQueryContext()); }

    PlanSegmentTreePtr planSegment(const String & query, ContextMutablePtr query_context);

    std::string execute(const String & query, ContextMutablePtr query_context);

    std::string execute(const String & query) { return execute(query, createQueryContext()); }

    ContextMutablePtr createQueryContext(std::unordered_map<std::string, Field> settings = {});

    const String & getDefaultDatabase() const { return database_name; }

protected:
    String database_name;
    ContextMutablePtr session_context;
};

class AbstractPlanTestSuite : public BasePlanTest
{
public:
    explicit AbstractPlanTestSuite(const String & database, const std::unordered_map<String, Field> & session_settings = {})
        : BasePlanTest(database, session_settings)
    {
    }

    struct Query
    {
        std::string name;
        std::unordered_map<std::string, Field> settings;
        std::vector<std::pair<std::string, ASTPtr>> sql;
    };

    std::vector<std::string> loadQueries();
    Query loadQuery(const std::string & name);
    std::string explain(const std::string & name);
    std::string loadExplain(const std::string & name);
    void saveExplain(const std::string & name, const std::string & explain);

    void unZip(const String & query_id);
    String loadExplainFromPath(const String & query_id);
    std::filesystem::path getPlanDumpPath() { return std::filesystem::path(PLAN_DUMP_PATH) / "dump_reproduce/"; }
    void createTablesFromJson(const String & path);
    void createClusterInfo(const String & path);
    String dump(const String & name);
    String reproduce(const String & path);
    std::vector<std::string> checkDump(const String & query_id, const String & query_id2);
    std::vector<std::string> getPathDumpFiles(const String & query_id);
    void cleanQueryFiles(const String & query_id);

    void createTables();
    void dropTableStatistics() { execute("drop stats all", createQueryContext()); }
    void loadTableStatistics();

    static bool enforce_regenerate();

protected:
    virtual std::vector<std::filesystem::path> getTableDDLFiles() = 0;
    virtual std::filesystem::path getStatisticsFile() = 0;
    virtual std::filesystem::path getQueriesDir() = 0;
    virtual std::filesystem::path getExpectedExplainDir() = 0;

    static std::vector<std::string> loadFile(const std::filesystem::path & path, char sep = {});
};

}
