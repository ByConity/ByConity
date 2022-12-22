#pragma once

#include <Optimizer/tests/gtest_base_plan_test.h>
#include <Optimizer/tests/test_config.h>

namespace DB
{

/**
 * change resource path in test.config.h.in.
 */
class BaseTpcdsPlanTest : public AbstractPlanTestSuite
{
public:
    explicit BaseTpcdsPlanTest(bool enable_statistics_ = true, const std::unordered_map<String, Field> & settings = {})
        : AbstractPlanTestSuite("tpcds", settings), enable_statistics(enable_statistics_)
    {
        createTables();
        dropTableStatistics();
        if (enable_statistics)
            loadTableStatistics();
    }

    std::vector<std::filesystem::path> getTableDDLFiles() override { return {TPCDS_TABLE_DDL_FILE}; }
    std::filesystem::path getStatisticsFile() override { return TPCDS_TABLE_STATISTICS_FILE; }
    std::filesystem::path getQueriesDir() override { return TPCDS_QUERIES_DIR; }
    std::filesystem::path getExpectedExplainDir() override
    {
        return std::filesystem::path(TPCDS_EXPECTED_EXPLAIN_RESULT) / (enable_statistics ? "tpcds" : "tpcds_no_statistics");
    }

    bool enable_statistics;
};

}
