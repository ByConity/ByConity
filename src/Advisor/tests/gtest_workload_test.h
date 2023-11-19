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
    explicit BaseWorkloadTest() : AbstractPlanTestSuite("workload_test", BasePlanTest::getDefaultOptimizerSettings())
    {
        createTables();
        // dropTableStatistics();
        // loadTableStatistics();
    }

    std::vector<std::filesystem::path> getTableDDLFiles()
    {
        return {};
    }
    std::filesystem::path getStatisticsFile()
    {
        return "";
    }
    std::filesystem::path getQueriesDir()
    {
        return "";
    }
    std::filesystem::path getExpectedExplainDir()
    {
        return "";
    }
};

} // namespace DB
