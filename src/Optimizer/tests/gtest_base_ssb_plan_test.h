#pragma once

#include <Optimizer/tests/test_config.h>
#include <Optimizer/tests/gtest_base_plan_test.h>

namespace DB
{

/**
 * change resource path in test.config.h.in.
 */
class BaseSsbPlanTest : public AbstractPlanTestSuite
{
public:
    explicit BaseSsbPlanTest(const std::unordered_map<String, Field> & settings = {}) : AbstractPlanTestSuite("ssb", settings)
    {
        createTables();
        dropTableStatistics();
        loadTableStatistics();
    }

    std::vector<std::filesystem::path> getTableDDLFiles() override { return {SSB_TABLE_DDL_FILE}; }
    std::filesystem::path getStatisticsFile() override { return SSB_TABLE_STATISTICS_FILE; }
    std::filesystem::path getQueriesDir() override { return SSB_QUERIES_DIR; }
    std::filesystem::path getExpectedExplainDir() override { return std::filesystem::path(SSB_EXPECTED_EXPLAIN_RESULT) / "ssb"; }
};

}
