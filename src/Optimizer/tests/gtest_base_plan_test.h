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

#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Optimizer/tests/test_config.h>
#include <QueryPlan/QueryPlan.h>

#include <boost/asio.hpp>

#include <filesystem>
#include <memory>
#include <unordered_map>

namespace DB
{

class BasePlanTest : public std::enable_shared_from_this<BasePlanTest>
{
public:
    explicit BasePlanTest(const String & database = "testdb", const std::unordered_map<String, Field> & session_settings = {});

    virtual ~BasePlanTest() = default;

    ASTPtr parse(const std::string & query, ContextMutablePtr query_context);

    QueryPlanPtr plan(const String & query, ContextMutablePtr query_context);

    QueryPlanPtr plan(const String & query, const std::unordered_map<String, Field> & settings = {})
    {
        return plan(query, createQueryContext(settings));
    }

    PlanSegmentTreePtr planSegment(const String & query, ContextMutablePtr query_context);

    std::string execute(const String & query, ContextMutablePtr query_context);

    std::string execute(const String & query) { return execute(query, createQueryContext()); }

    ContextMutablePtr createQueryContext(std::unordered_map<std::string, Field> settings = {});

    const String & getDefaultDatabase() const { return database_name; }

    static std::unordered_map<String, Field> getDefaultOptimizerSettings();

    ContextMutablePtr getSessionContext() { return session_context; }
    std::string getDatabaseName() { return database_name; }

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
    static int regenerate_task_thread_size();

    void setShowStatistics(bool show_statistics_) { show_statistics = show_statistics_; }

    void setTimerRounds(int n) { timer_rounds = n; }
    String printMetric()
    {
        // auto str = fmt::format(
        //     FMT_STRING("vanilla: ser {}s, deser {}s\nprotobuf:ser {}s, deser {}s"),
        //     timers[TimerOption::VanillaSer],
        //     timers[TimerOption::VanillaDeser],
        //     timers[TimerOption::ProtobufSer],
        //     timers[TimerOption::ProtobufDeser]);
        // timers.clear();
        return {};
    }

protected:
    virtual std::vector<std::filesystem::path> getTableDDLFiles() = 0;
    virtual std::filesystem::path getStatisticsFile() = 0;
    virtual std::filesystem::path getQueriesDir() = 0;
    virtual std::filesystem::path getExpectedExplainDir() = 0;

    bool show_statistics = true;

    static std::vector<std::string> loadFile(const std::filesystem::path & path, char sep = {});
    enum class TimerOption
    {
        VanillaSer = 0,
        VanillaDeser = 1,
        ProtobufSer = 2,
        ProtobufDeser = 3,
    };
    int timer_rounds = 10;
    std::unordered_map<TimerOption, double> timers;
};

#define DECLARE_GENERATE_TEST(TEST_SUITE_NAME) \
    TEST_F(TEST_SUITE_NAME, generate) \
    { \
        if (!AbstractPlanTestSuite::enforce_regenerate()) \
            GTEST_SKIP() << "skip generate. set env REGENERATE=1 to regenerate explains."; \
\
        int thread_size = AbstractPlanTestSuite::regenerate_task_thread_size(); \
        std::cout << "use " << thread_size << " threads for generate task" << std::endl; \
        boost::asio::thread_pool pool(thread_size); \
\
        for (auto & query : tester->loadQueries()) \
        { \
            boost::asio::post(pool, [&, query]() { \
                try \
                { \
                    std::cout << " try generate for " + query + "." << std::endl; \
                    tester->saveExplain(query, explain(query)); \
                } \
                catch (...) \
                { \
                    std::cerr << " generate for " + query + " failed." << std::endl; \
                    tester->saveExplain(query, ""); \
                } \
            }); \
        } \
\
        pool.join(); \
    }
}
