#pragma once

#include <Interpreters/Context.h>
#include <QueryPlan/QueryPlan.h>

#include <filesystem>

namespace DB
{

class BasePlanTest
{
public:
    explicit BasePlanTest(const String & database = "testdb", const std::unordered_map<String, Field> & session_settings = {});

    virtual ~BasePlanTest() = default;

    ASTPtr parse(const std::string & query, ContextMutablePtr query_context);

    QueryPlanPtr plan(const String & query, ContextMutablePtr query_context);

    PlanSegmentTreePtr planSegment(const String & query, ContextMutablePtr query_context);

    std::string execute(const String & query, ContextMutablePtr query_context);

    ContextMutablePtr createQueryContext(std::unordered_map<std::string, Field> settings = {});

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
