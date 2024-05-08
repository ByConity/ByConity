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

#include <Interpreters/Context.h>
#include <Optimizer/EqualityInference.h>
#include <Optimizer/Rewriter/Rewriter.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/CTEInfo.h>
#include <common/logger_useful.h>

namespace DB
{
class PredicatePushdown : public Rewriter
{
public:
    explicit PredicatePushdown(bool pushdown_filter_into_cte_ = false, bool simplify_common_filter_ = false)
        : pushdown_filter_into_cte(pushdown_filter_into_cte_), simplify_common_filter(simplify_common_filter_)
    {
    }
    String name() const override { return "PredicatePushdown"; }

private:
    void rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override { return context->getSettingsRef().enable_predicate_pushdown_rewrite; }
    const bool pushdown_filter_into_cte;
    const bool simplify_common_filter;
};

struct PredicateContext
{
    ConstASTPtr predicate;
    ConstASTPtr extra_predicate_for_simplify_outer_join;
    ContextMutablePtr context;
};

struct InnerJoinResult;
struct OuterJoinResult;

class PredicateVisitor : public PlanNodeVisitor<PlanNodePtr, PredicateContext>
{
public:
    PredicateVisitor(
        bool pushdown_filter_into_cte_,
        bool simplify_common_filter_,
        ContextMutablePtr context_,
        CTEInfo & cte_info_,
        const std::unordered_map<CTEId, UInt64> & cte_reference_counts_)
        : pushdown_filter_into_cte(pushdown_filter_into_cte_)
        , simplify_common_filter(simplify_common_filter_)
        , context(context_)
        , cte_info(cte_info_)
        , cte_reference_counts(cte_reference_counts_)
    {
    }

    PlanNodePtr visitPlanNode(PlanNodeBase &, PredicateContext &) override;
    PlanNodePtr visitProjectionNode(ProjectionNode &, PredicateContext &) override;
    PlanNodePtr visitFilterNode(FilterNode &, PredicateContext &) override;
    PlanNodePtr visitAggregatingNode(AggregatingNode &, PredicateContext &) override;
    PlanNodePtr visitMarkDistinctNode(MarkDistinctNode & node, PredicateContext & predicate_context) override;
    PlanNodePtr visitJoinNode(JoinNode &, PredicateContext &) override;
    PlanNodePtr visitArrayJoinNode(ArrayJoinNode &, PredicateContext &) override;
    PlanNodePtr visitExchangeNode(ExchangeNode & node, PredicateContext & predicate_context) override;
    PlanNodePtr visitWindowNode(WindowNode &, PredicateContext &) override;
    PlanNodePtr visitMergeSortingNode(MergeSortingNode &, PredicateContext &) override;
    PlanNodePtr visitPartialSortingNode(PartialSortingNode &, PredicateContext &) override;
    PlanNodePtr visitSortingNode(SortingNode &, PredicateContext &) override;
    PlanNodePtr visitUnionNode(UnionNode &, PredicateContext &) override;
    PlanNodePtr visitDistinctNode(DistinctNode &, PredicateContext &) override;
    PlanNodePtr visitAssignUniqueIdNode(AssignUniqueIdNode &, PredicateContext &) override;
    PlanNodePtr visitCTERefNode(CTERefNode & node, PredicateContext & context) override;

private:
    const bool pushdown_filter_into_cte;
    const bool simplify_common_filter;
    ContextMutablePtr context;
    CTEInfo & cte_info;
    const std::unordered_map<CTEId, UInt64> & cte_reference_counts;
    std::unordered_map<CTEId, std::vector<std::pair<const CTERefStep *, ConstASTPtr>>> cte_predicates{};
    Poco::Logger * logger = &Poco::Logger::get("PredicateVisitor");

    PlanNodePtr process(PlanNodeBase &, PredicateContext &);
    PlanNodePtr processChild(PlanNodeBase &, PredicateContext &);
    InnerJoinResult processInnerJoin(
        ConstASTPtr & inherited_predicate,
        ConstASTPtr & left_predicate,
        ConstASTPtr & right_predicate,
        ConstASTPtr & join_predicate,
        std::set<String> & left_symbols,
        std::set<String> & right_symbols);
    OuterJoinResult processOuterJoin(
        ConstASTPtr & inherited_predicate,
        ConstASTPtr & outer_predicate,
        ConstASTPtr & inner_predicate,
        ConstASTPtr & join_predicate,
        std::set<String> & outer_symbols,
        std::set<String> & inner_symbols);

    // utils of outer join to inner join
    static void tryNormalizeOuterToInnerJoin(JoinNode & node, const ConstASTPtr & inherited_predicate, ContextMutablePtr context);
    static bool canConvertOuterToInner(
        const std::unordered_map<String, Field> & inner_symbols_for_outer_join,
        const ConstASTPtr & inherited_predicate,
        ContextMutablePtr context,
        const NameToType & column_types);
    static ASTTableJoin::Kind useInnerForLeftSide(ASTTableJoin::Kind kind);
    static ASTTableJoin::Kind useInnerForRightSide(ASTTableJoin::Kind kind);
    static bool isRegularJoin(const JoinStep & step);
};

struct InnerJoinResult
{
    ASTPtr left_predicate;
    ASTPtr right_predicate;
    ASTPtr join_predicate;
    ASTPtr post_join_predicate;
};

struct OuterJoinResult
{
    ASTPtr outer_predicate;
    ASTPtr inner_predicate;
    ASTPtr join_predicate;
    ASTPtr post_join_predicate;
};

/**
 * Computes the effective predicate at the top of the specified PlanNode
 *
 * Note: non-deterministic predicates cannot be pulled up (so they will be ignored)
 */
class EffectivePredicateExtractor
{
public:
    static ASTPtr extract(PlanNodePtr & node, ContextMutablePtr & context);
    static ASTPtr extract(PlanNodeBase & node, ContextMutablePtr & context);
};

class EffectivePredicateVisitor : public PlanNodeVisitor<ASTPtr, ContextMutablePtr>
{
protected:
    ASTPtr visitPlanNode(PlanNodeBase & node, ContextMutablePtr & context) override;

public:
    ASTPtr visitLimitNode(LimitNode &, ContextMutablePtr &) override;
    ASTPtr visitEnforceSingleRowNode(EnforceSingleRowNode &, ContextMutablePtr &) override;
    ASTPtr visitProjectionNode(ProjectionNode &, ContextMutablePtr &) override;
    ASTPtr visitFilterNode(FilterNode &, ContextMutablePtr &) override;
    ASTPtr visitAggregatingNode(AggregatingNode &, ContextMutablePtr &) override;
    ASTPtr visitJoinNode(JoinNode &, ContextMutablePtr &) override;
    ASTPtr visitExchangeNode(ExchangeNode &, ContextMutablePtr &) override;
    ASTPtr visitWindowNode(WindowNode &, ContextMutablePtr &) override;
    ASTPtr visitMergeSortingNode(MergeSortingNode &, ContextMutablePtr &) override;
    ASTPtr visitUnionNode(UnionNode &, ContextMutablePtr &) override;
    ASTPtr visitTableScanNode(TableScanNode &, ContextMutablePtr &) override;
    ASTPtr visitDistinctNode(DistinctNode &, ContextMutablePtr &) override;
    ASTPtr visitAssignUniqueIdNode(AssignUniqueIdNode &, ContextMutablePtr &) override;
    ASTPtr visitCTERefNode(CTERefNode & node, ContextMutablePtr & context) override;

    explicit EffectivePredicateVisitor() = default;

private:
    ASTPtr process(PlanNodeBase & node, ContextMutablePtr & context);
    static ASTPtr pullExpressionThroughSymbols(ASTPtr & expression, std::vector<String> symbols, ContextMutablePtr & context);
};

}
