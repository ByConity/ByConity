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

#include <Optimizer/tests/gtest_base_plan_test.h>
#include <Optimizer/tests/test_config.h>

namespace DB
{

/**
 * change resource path in test_config.h.in.
 */
class BaseSsbPlanTest : public AbstractPlanTestSuite
{
public:
    explicit BaseSsbPlanTest(const std::unordered_map<String, Field> & settings = {}, int sf_ = 100, bool use_sample = false)
        : AbstractPlanTestSuite("ssb" + std::to_string(sf_) + (use_sample ? "_sample" : ""), settings), sf(sf_)
    {
        createTables();
        dropTableStatistics();
        loadTableStatistics();
    }

    std::vector<std::filesystem::path> getTableDDLFiles() override { return {SSB_TABLE_DDL_FILE}; }
    std::filesystem::path getStatisticsFile() override
    {
        return std::filesystem::path(SSB_TABLE_STATISTICS_FOLDER) / getDatabaseName() / "stats.json";
    }
    std::filesystem::path getQueriesDir() override { return SSB_QUERIES_DIR; }
    std::filesystem::path getExpectedExplainDir() override
    {
        auto dir = getDatabaseName() + label;
        return std::filesystem::path(SSB_EXPECTED_EXPLAIN_RESULT) / dir;
    }

    void setLabel(const std::string & label_) { this->label = "_" + label_; }

    int sf;
    std::string label;
};

}
