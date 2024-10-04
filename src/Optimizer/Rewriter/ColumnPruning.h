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
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/SimplePlanRewriter.h>

namespace DB
{
class ColumnPruning : public Rewriter
{
public:
    explicit ColumnPruning() = default;
    String name() const override { return "ColumnPruning"; }

private:
    bool isEnabled(ContextMutablePtr context) const override { return context->getSettingsRef().enable_column_pruning; }
    bool rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
};

class AddProjectionPruning : public Rewriter
{
public:
    explicit AddProjectionPruning() = default;
    String name() const override { return "AddProjectionPruning"; }

private:
    bool isEnabled(ContextMutablePtr context) const override { return context->getSettingsRef().enable_add_projection_to_pruning; }
    bool rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
};
class DistinctToAggregatePruning : public Rewriter
{
public:
    explicit DistinctToAggregatePruning() = default;
    String name() const override { return "DistinctToAggregatePruning"; }

private:
    bool isEnabled(ContextMutablePtr context) const override { return context->getSettingsRef().enable_distinct_to_aggregate; }
    bool rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
};

class WindowToSortPruning : public Rewriter
{
public:
    WindowToSortPruning() = default;
    String name() const override { return "WindowToSortPruning"; }

private:
    bool isEnabled(ContextMutablePtr context) const override { return context->getSettingsRef().enable_filter_window_to_sorting_limit; }
    bool rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
};

struct ColumnPruningContext
{
    NameSet name_set;
    bool is_parent_from_projection = false;
};

class ColumnPruningVisitor : public PlanNodeVisitor<PlanNodePtr, ColumnPruningContext>
{
public:
    explicit ColumnPruningVisitor(
        ContextMutablePtr context_,
        CTEInfo & cte_info_,
        PlanNodePtr & root,
        bool add_projection_,
        bool distinct_to_aggregate_,
        bool filter_window_to_sort_limit_)
        : context(std::move(context_))
        , post_order_cte_helper(cte_info_, root)
        , add_projection(add_projection_)
        , distinct_to_aggregate(distinct_to_aggregate_)
        , filter_window_to_sort_limit(filter_window_to_sort_limit_)
    {
    }
    static String selectColumnWithMinSize(NamesAndTypesList source_columns, StoragePtr storage);

private:

    PlanNodePtr visitPlanNode(PlanNodeBase & node, ColumnPruningContext & column_pruning_context) override;

#define VISITOR_DEF(TYPE) PlanNodePtr visit##TYPE##Node(TYPE##Node &, ColumnPruningContext &) override;
    APPLY_STEP_TYPES(VISITOR_DEF)
#undef VISITOR_DEF

    template <bool require_all>
    PlanNodePtr visitDefault(PlanNodeBase & node, ColumnPruningContext & column_pruning_context);

    PlanNodePtr addProjection(PlanNodePtr node, NameSet & require);
    PlanNodePtr convertDistinctToGroupBy(PlanNodePtr node);
    PlanNodePtr convertFilterWindowToSortingLimit(PlanNodePtr node, NameSet & require);

    ContextMutablePtr context;
    PostorderCTEVisitHelper post_order_cte_helper;
    std::unordered_map<CTEId, ColumnPruningContext> cte_require_columns{};

    bool add_projection;
    bool distinct_to_aggregate;
    bool filter_window_to_sort_limit;
};

}
