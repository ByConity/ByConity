#pragma once

#include <Optimizer/tests/gtest_base_plan_test.h>
#include <Optimizer/tests/gtest_base_tpcds_plan_test.h>
#include <Optimizer/tests/test_config.h>

#include <Core/Field.h>
#include <Interpreters/Context_fwd.h>

#include <string>
#include <unordered_map>
#include <vector>

namespace DB
{

class BaseWorkloadTest : public AbstractPlanTestSuite
{
public:
    explicit BaseWorkloadTest(int sf_ = 1000)
        : AbstractPlanTestSuite("tpcds" + std::to_string(sf_), BasePlanTest::getDefaultOptimizerSettings()), sf(sf_)
    {
        createTables();
        dropTableStatistics();
        loadTableStatistics();
    }

    std::vector<std::filesystem::path> getTableDDLFiles() override { return {TPCDS_TABLE_DDL_FILE}; }
    std::filesystem::path getStatisticsFile() override { return TPCDS1000_TABLE_STATISTICS_FILE; }
    std::filesystem::path getQueriesDir() override { return TPCDS_QUERIES_DIR; }
    std::filesystem::path getExpectedExplainDir() override
    {
        std::string dir = "tpcds" + std::to_string(sf);
        return std::filesystem::path(TPCDS_EXPECTED_EXPLAIN_RESULT) / dir;
    }

    ContextMutablePtr getSessionContext() { return session_context; }

    std::string getDatabaseName() { return database_name; }

    int sf;
};

} // namespace DB
