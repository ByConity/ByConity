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
#include <vector>
#include <Optimizer/Rule/Rule.h>

namespace DB
{
class Rules
{
public:
    static std::vector<RulePtr> mergeSetRules();
    static std::vector<RulePtr> implementSetRules();
    static std::vector<RulePtr> normalizeExpressionRules();
    static std::vector<RulePtr> swapPredicateRules();
    static std::vector<RulePtr> simplifyExpressionRules();
    static std::vector<RulePtr> simplifyPrewhereRules();
    static std::vector<RulePtr> inlineProjectionRules();
    static std::vector<RulePtr> pushPartialStepRules();
    static std::vector<RulePtr> optimizeAggregateRules();
    static std::vector<RulePtr> pushAggRules();
    static std::vector<RulePtr> pushDownLimitRules();
    static std::vector<RulePtr> pushDownTopNRules();
    static std::vector<RulePtr> createTopNFilteringRules();
    static std::vector<RulePtr> pushDownTopNFilteringRules();
    static std::vector<RulePtr> removeRedundantRules();
    static std::vector<RulePtr> distinctToAggregateRules();
    static std::vector<RulePtr> pushIntoTableScanRules();
    static std::vector<RulePtr> swapAdjacentRules();
    static std::vector<RulePtr> pushTableScanEmbeddedStepRules();
    static std::vector<RulePtr> explainAnalyzeRules();
    static std::vector<RulePtr> pushApplyRules();
    static std::vector<RulePtr> unnestingSubqueryRules();
    static std::vector<RulePtr> pushDownBitmapProjection();
    static std::vector<RulePtr> pushProjectionIntoTableScanRules();
    static std::vector<RulePtr> pushIndexProjectionIntoTableScanRules();
    static std::vector<RulePtr> crossJoinToUnion();
    static std::vector<RulePtr> sumIfToCountIf();
    static std::vector<RulePtr> extractBitmapImplicitFilterRules();
    static std::vector<RulePtr> pushUnionThroughJoin();
    static std::vector<RulePtr> addRepartitionColumn();
};

}
