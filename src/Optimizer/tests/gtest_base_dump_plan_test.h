#pragma once

#include <Optimizer/tests/gtest_base_plan_test.h>
#include <Optimizer/tests/test_config.h>

namespace DB
{

/**
 * change resource path in test.config.h.in.
 */
class BaseDumpPlanTest : public AbstractPlanTestSuite
{
public:
    explicit BaseDumpPlanTest(
        bool enable_statistics_ = true,
        const std::unordered_map<String, Field> & settings = {})
        :
        AbstractPlanTestSuite("test_dump", settings),enable_statistics(enable_statistics_)
    {
        createTables();
        dropTableStatistics();
        if (enable_statistics)
            loadTableStatistics();
    }

    std::vector<std::filesystem::path> getTableDDLFiles() override { return {std::filesystem::path(PLAN_DUMP_PATH) / "create_table.sql"}; }
    std::filesystem::path getStatisticsFile() override { return std::filesystem::path(PLAN_DUMP_PATH) / "test_dump.bin"; }
    std::filesystem::path getQueriesDir() override { return std::filesystem::path(PLAN_DUMP_PATH) / "queries"; }
    std::filesystem::path getExpectedExplainDir() override { return std::filesystem::path(PLAN_DUMP_PATH) / ""; }

    bool enable_statistics;
};

}
