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

#include <string>
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
    explicit BaseTpcdsPlanTest(const std::unordered_map<String, Field> & settings = {}, int sf_ = 1000)
        : AbstractPlanTestSuite("tpcds" + std::to_string(sf_), settings)
        , sf(sf_)
    {
        if (sf_ != 1000 && sf_ != 100)
            throw Exception("sf only support 100 or 1000", ErrorCodes::BAD_ARGUMENTS);
        createTables();
        dropTableStatistics();
        loadTableStatistics();
    }

    std::vector<std::filesystem::path> getTableDDLFiles() override { return {TPCDS_TABLE_DDL_FILE}; }
    std::filesystem::path getStatisticsFile() override
    {
        if (sf == 100)
            return TPCDS100_TABLE_STATISTICS_FILE;

        return TPCDS1000_TABLE_STATISTICS_FILE;
    }
    std::filesystem::path getQueriesDir() override { return TPCDS_QUERIES_DIR; }
    std::filesystem::path getExpectedExplainDir() override
    {
        std::string dir = "tpcds" + std::to_string(sf) + label;
        return std::filesystem::path(TPCDS_EXPECTED_EXPLAIN_RESULT) / dir;
    }
    void setLabel(const std::string & label_) { this->label = "_" + label_; }

    int sf;
    std::string label;

};

}
