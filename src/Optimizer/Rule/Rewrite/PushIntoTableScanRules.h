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

#include <Optimizer/Rule/Rule.h>

namespace DB
{

class PushStorageFilter : public Rule
{
public:
    RuleType getType() const override
    {
        return RuleType::PUSH_STORAGE_FILTER;
    }
    String getName() const override
    {
        return "PUSH_STORAGE_FILTER";
    }
    bool isEnabled(ContextPtr context) const override
    {
        return context->getSettingsRef().enable_push_storage_filter;
    }

    ConstRefPatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;

    static ASTPtr pushStorageFilter(TableScanStep & table_step, ASTPtr filter, PlanNodeStatisticsPtr storage_statistics, ContextMutablePtr context);
};


class PushLimitIntoTableScan : public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_LIMIT_INTO_TABLE_SCAN; }
    String getName() const override { return "PUSH_LIMIT_INTO_TABLE_SCAN"; }
    bool isEnabled(ContextPtr context) const override { return context->getSettingsRef().enable_push_limit_into_table_scan; }
    ConstRefPatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class PushAggregationIntoTableScan : public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_AGGREGATION_INTO_TABLE_SCAN; }
    String getName() const override { return "PUSH_AGGREGATION_INTO_TABLE_SCAN"; }
    bool isEnabled(ContextPtr context) const override { return context->getSettingsRef().enable_push_aggregation_into_table_scan; }
    ConstRefPatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class PushProjectionIntoTableScan : public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_PROJECTION_INTO_TABLE_SCAN; }
    String getName() const override { return "PUSH_PROJECTION_INTO_TABLE_SCAN"; }
    bool isEnabled(ContextPtr context) const override { return context->getSettingsRef().enable_push_projection_into_table_scan; }
    ConstRefPatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class PushFilterIntoTableScan : public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_FILTER_INTO_TABLE_SCAN; }
    String getName() const override { return "PUSH_FILTER_INTO_TABLE_SCAN"; }
    bool isEnabled(ContextPtr context) const override { return context->getSettingsRef().enable_push_filter_into_table_scan; }
    ConstRefPatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class PushIndexProjectionIntoTableScan : public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_INDEX_PROJECTION_INTO_TABLE_SCAN; }
    String getName() const override { return "PUSH_INDEX_PROJECTION_INTO_TABLE_SCAN"; }
    bool isEnabled(ContextPtr context) const override
    {
        return context->getSettingsRef().enable_push_index_projection_into_table_scan;
    }
    ConstRefPatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
