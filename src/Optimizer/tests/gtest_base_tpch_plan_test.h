#pragma once

#include <Optimizer/tests/test_config.h>
#include <Optimizer/tests/gtest_base_plan_test.h>

namespace DB
{

/**
 * change resource path in test.config.h.in.
 */
class BaseTpchPlanTest : public AbstractPlanTestSuite
{
public:
    explicit BaseTpchPlanTest(const std::unordered_map<String, Field> & settings = {}) : AbstractPlanTestSuite("tpch", settings)
    {
        createTables();
        dropTableStatistics();
        loadTableStatistics();
    }

    std::vector<std::filesystem::path> getTableDDLFiles() override { return {TPCH_TABLE_DDL_FILE}; }
    std::filesystem::path getStatisticsFile() override { return TPCH_TABLE_STATISTICS_FILE; }
    std::filesystem::path getQueriesDir() override { return TPCH_QUERIES_DIR; }
    std::filesystem::path getExpectedExplainDir() override { return std::filesystem::path(TPCH_EXPECTED_EXPLAIN_RESULT) / "tpch"; }
};

}
