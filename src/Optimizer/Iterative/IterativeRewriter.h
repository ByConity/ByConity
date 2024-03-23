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

#include <Optimizer/Rewriter/Rewriter.h>
#include <Optimizer/Rule/Rule.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/IQueryPlanStep.h>

#include <unordered_map>
#include <vector>
#include <chrono>
#include <map>

namespace DB
{

// use a linked multi map to make rule application process more reasonable
using RuleIndex = std::unordered_map<IQueryPlanStep::Type, std::vector<RulePtr>>;

struct IterativeRewriterContext
{
    ContextMutablePtr globalContext;
    CTEInfo & cte_info;
    UInt64 optimizer_timeout;
    ExcludedRulesMap * excluded_rules_map;
    Stopwatch watch{CLOCK_THREAD_CPUTIME_ID};
    // for debugging
    QueryPlan & plan;
    int rule_apply_count = 0;
};

/**
 * A IterativeOptimizer will loop to apply `Rule`s recursively until
 * the plan does not change or the optimizer timeout been exhausted.
 */
class IterativeRewriter : public Rewriter
{
public:
    IterativeRewriter(const std::vector<RulePtr> & rules_, std::string name_);
    static std::map<std::underlying_type_t<RuleType>, size_t> getRuleCallTimes();
    String name() const override { return names; }
private:
    bool isEnabled(ContextMutablePtr context) const override { return context->getSettingsRef().enable_iterative_rewriter; }
    void rewrite(QueryPlan & plan, ContextMutablePtr context) const override;

    String names;
    RuleIndex rules;

    bool explorePlan(PlanNodePtr & plan, IterativeRewriterContext & context) const;
    bool exploreNode(PlanNodePtr & node, IterativeRewriterContext & context) const;
    bool exploreChildren(PlanNodePtr & plan, IterativeRewriterContext & context) const;

    static void checkTimeoutNotExhausted(const String & rule_name, const IterativeRewriterContext & context);
};

}
