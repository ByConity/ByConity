#pragma once

#include <Optimizer/tests/gtest_base_plan_test.h>
#include <Optimizer/tests/test_config.h>

namespace DB
{

/**
 * change resource path in test.config.h.in.
 */
class CorrectnessPlanTest : public AbstractPlanTestSuite
{
public:
    explicit CorrectnessPlanTest(const std::unordered_map<String, Field> & settings = {}) : AbstractPlanTestSuite("correctness", settings)
    {
        createTables();
    }

    std::vector<std::filesystem::path> getTableDDLFiles() override { return {CORRECTNESS_TABLE_DDL_FILE}; }
    std::filesystem::path getStatisticsFile() override { return ""; }
    std::filesystem::path getQueriesDir() override { return CORRECTNESS_QUERIES_DIR; }
    std::filesystem::path getExpectedExplainDir() override { return std::filesystem::path(CORRECTNESS_EXPECTED_EXPLAIN_RESULT) / "correctness"; }
};

}
