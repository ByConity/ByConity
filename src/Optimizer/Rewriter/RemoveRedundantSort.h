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

#include <Optimizer/Rewriter/Rewriter.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/SimplePlanRewriter.h>


namespace DB
{

class RemoveRedundantSort : public Rewriter
{
public:
    String name() const override { return "RemoveRedundantSort"; }

private:
    bool rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override { return context->getSettingsRef().enable_redundant_sort_removal; }
};

struct RedundantSortContext
{
    ContextMutablePtr context;
    bool can_sort_be_removed = false;
};

class RedundantSortVisitor : public SimplePlanRewriter<RedundantSortContext>
{
public:
    explicit RedundantSortVisitor(ContextMutablePtr context_, CTEInfo & cte_info_)
        : SimplePlanRewriter(context_, cte_info_)
    {
    }

    static bool isStateful(ConstASTPtr expression, ContextMutablePtr context);
    static bool isOrderDependentAggregateFunction(const String & aggname);
    const static std::unordered_set<String> order_dependent_agg;

private:
    PlanNodePtr visitPlanNode(PlanNodeBase & node, RedundantSortContext & sort_context) override;
    PlanNodePtr visitProjectionNode(ProjectionNode & node, RedundantSortContext & sort_context) override;
    PlanNodePtr visitAggregatingNode(AggregatingNode & node, RedundantSortContext & sort_context) override;
    PlanNodePtr visitJoinNode(JoinNode & node, RedundantSortContext & sort_context) override;
    PlanNodePtr visitUnionNode(UnionNode & node, RedundantSortContext & sort_context) override;
    PlanNodePtr visitIntersectNode(IntersectNode & node, RedundantSortContext & sort_context) override;
    PlanNodePtr visitExceptNode(ExceptNode & node, RedundantSortContext & sort_context) override;
    PlanNodePtr visitSortingNode(SortingNode & node, RedundantSortContext & sort_context) override;
    PlanNodePtr visitCTERefNode(CTERefNode & node, RedundantSortContext & sort_context) override;

    PlanNodePtr processChildren(PlanNodeBase & node, RedundantSortContext & sort_context);
    PlanNodePtr resetChild(PlanNodeBase & node, PlanNodes & children, RedundantSortContext & sort_context);

    std::unordered_map<CTEId, RedundantSortContext> cte_require_context{};
};

class StatefulVisitor : public ConstASTVisitor<void, ContextMutablePtr>
{
public:
    void visitNode(const ConstASTPtr & node, ContextMutablePtr & context) override;
    void visitASTFunction(const ConstASTPtr & node, ContextMutablePtr & context) override;
    bool isStateful() const { return is_stateful; }

private:
    bool is_stateful = false;
};
}
