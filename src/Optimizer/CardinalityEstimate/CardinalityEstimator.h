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

#include <Analyzers/TypeAnalyzer.h>
#include <Interpreters/Context.h>
#include <Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/CTEVisitHelper.h>
#include <Optimizer/DataDependency/InclusionDependency.h>

namespace DB
{
class CardinalityEstimator
{
public:
    static std::optional<PlanNodeStatisticsPtr> estimate(
        QueryPlanStepPtr & step,
        CTEInfo & cte_info,
        std::vector<PlanNodeStatisticsPtr> children_stats,
        ContextMutablePtr context,
        bool simple_children,
        std::vector<bool> is_table_scan,
        std::vector<double> children_filter_selectivity,
        const InclusionDependency & inclusion_dependency = {});

    static std::optional<PlanNodeStatisticsPtr> estimate(
        PlanNodeBase & node,
        CTEInfo & cte_info,
        ContextMutablePtr context,
        bool recursive = false, 
        bool re_estimate = false);

    static void estimate(QueryPlan & plan, ContextMutablePtr context, bool re_estimate = false);
};

struct CardinalityContext
{
    ContextMutablePtr context;
    CTEInfo & cte_info;
    std::vector<PlanNodeStatisticsPtr> children_stats;
    bool simple_children = false;
    std::vector<bool> children_are_table_scan = {};
    bool is_table_scan = false;
    bool re_estimate = false;
    std::vector<double> children_filter_selectivity = {};
    InclusionDependency inclusion_dependency = {};
};

class CardinalityVisitor : public StepVisitor<PlanNodeStatisticsPtr, CardinalityContext>
{
public:
    PlanNodeStatisticsPtr visitStep(const IQueryPlanStep &, CardinalityContext &) override;

#define VISITOR_DEF(TYPE) PlanNodeStatisticsPtr visit##TYPE##Step(const TYPE##Step &, CardinalityContext &) override;
    APPLY_STEP_TYPES(VISITOR_DEF)
#undef VISITOR_DEF
};

class PlanCardinalityVisitor : public PlanNodeVisitor<PlanNodeStatisticsPtr, CardinalityContext>
{
public:
    explicit PlanCardinalityVisitor(CTEInfo & cte_info) : cte_helper(cte_info) { }

    PlanNodeStatisticsPtr visitPlanNode(PlanNodeBase &, CardinalityContext &) override;
    PlanNodeStatisticsPtr visitCTERefNode(CTERefNode & node, CardinalityContext & context) override;
private:
    SimpleCTEVisitHelper<void> cte_helper;
};

}
