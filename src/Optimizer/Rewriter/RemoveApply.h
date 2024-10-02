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
#include <Optimizer/Rewriter/Rewriter.h>
#include <Optimizer/Rule/Rule.h>
#include <QueryPlan/SimplePlanRewriter.h>
#include <QueryPlan/TranslationMap.h>

namespace DB
{
/**
 * Reference paper:
 *
 * 1 Orthogonal Optimization of Subqueries and Aggregation
 * 2 Unnesting Arbitrary Queries
 */

/**
 * Pattern match
 *
 * 1 correlation columns exist
 * 2 subquery is scalar aggregation
 *
 * It transforms:
 * <pre>
 * - Apply (correlation: [c], filter: true, output: a, count, agg)
 *      - Input (a, c)
 *      - Aggregation global
 *        count <- count(*)
 *        agg <- agg(b)
 *           - Source (b) with correlated filter (b > c)
 * </pre>
 * Into:
 * <pre>
 * - Project (a <- a, count <- count, agg <- agg)
 *      - Aggregation (group by [a, c, unique])
 *        count <- count(*) mask(non_null)
 *        agg <- agg(b) mask(non_null)
 *           - LEFT join (filter: b > c)
 *                - UniqueId (unique)
 *                     - Input (a, c)
 *                - Project (non_null <- TRUE)
 *                     - Source (b) decorrelated
 * </pre>
 */
class RemoveCorrelatedScalarSubquery : public Rewriter
{
public:
    String name() const override { return "RemoveCorrelatedScalarSubquery"; }

private:
    bool isEnabled(ContextMutablePtr context) const override { return context->getSettingsRef().enable_remove_correlated_scalar_subquery; }
    bool rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
};

/**
 * It is important to clarify the two forms of aggregation in SQL,
 * whose behavior diverges on an empty input.
 *
 * “Vector” aggregation specifies grouping columns as well as aggregates
 * to compute, for example:
 *
 * select o_orderdate, sum(o_totalprice) from orders group by o_orderdate
 *
 * If orders is empty, the result of the query is also empty.
 *
 * “Scalar” aggregation on the other hand, does not specify grouping columns.
 * For example, get the total sales in the table:
 *
 * select sum(o_totalprice) from orders.
 *
 * This second query always returns exactly one row. The result value on
 * an empty input depends on the aggregate; for sum it is null, while for
 * count it is 0.
 *
 * In algebraic expressions we denote vector aggregate as G(A,F) , where A are
 * the grouping columns and F are the aggregates to compute; and denote scalar
 * aggregate as G(1,F).
 */
class CorrelatedScalarSubqueryVisitor : public SimplePlanRewriter<Void>
{
public:
    CorrelatedScalarSubqueryVisitor(ContextMutablePtr context_, CTEInfo & cte_info_) : SimplePlanRewriter(context_, cte_info_) { }

private:
    PlanNodePtr visitApplyNode(ApplyNode &, Void &) override;
};

/**
 * Pattern match
 *
 * 1 correlation columns not exist
 * 2 subquery is scalar aggregation
 *
 * It transforms:
 * <pre>
 * - Apply (correlation: [], assignment : a = count)
 *      - Input (a, c)
 *      - Aggregation global
 *        count <- count(*)
 *        agg <- agg(b)
 *           - Source (b)
 * </pre>
 * Into:
 * <pre>
 * - Cross JOIN
 *      - Input (a, c)
 *      - Aggregation global
 *        count <- count(*)
 *        agg <- agg(b)
 *           - Source (b)
 * </pre>
 */
class RemoveUnCorrelatedScalarSubquery : public Rewriter
{
public:
    String name() const override { return "RemoveUnCorrelatedScalarSubquery"; }

private:
    bool rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override
    {
        return context->getSettingsRef().enable_remove_uncorrelated_scalar_subquery;
    }
};

class UnCorrelatedScalarSubqueryVisitor : public SimplePlanRewriter<Void>
{
public:
    UnCorrelatedScalarSubqueryVisitor(ContextMutablePtr context_, CTEInfo & cte_info_) : SimplePlanRewriter(context_, cte_info_) { }

private:
    PlanNodePtr visitApplyNode(ApplyNode &, Void &) override;
};

/**
 * Pattern match
 *
 * 1 correlation columns exist
 * 2 subquery is in subquery
 *
 * Transforms:
 * <pre>
 * - Apply (output: a in B.b)
 *    - input: some plan A producing symbol a
 *    - subquery: some plan B producing symbol b, using symbols from A
 * </pre>
 * Into:
 * <pre>
 * - Project (output: CASE WHEN (countmatches > 0) THEN true WHEN (countnullmatches > 0) THEN null ELSE false END)
 *   - Aggregate (countmatches=count(*) where a, b not null; countnullmatches where a,b null but buildSideKnownNonNull is not null)
 *     grouping by (A'.*)
 *     - LeftJoin on (A and B correlation condition)
 *       - AssignUniqueId (A')
 *         - A
 * </pre>
 */
class RemoveCorrelatedInSubquery : public Rewriter
{
public:
    String name() const override { return "RemoveCorrelatedInSubquery"; }

private:
    bool rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override { return context->getSettingsRef().enable_remove_correlated_in_subquery; }
};

class CorrelatedInSubqueryVisitor : public SimplePlanRewriter<Void>
{
public:
    CorrelatedInSubqueryVisitor(ContextMutablePtr context_, CTEInfo & cte_info_) : SimplePlanRewriter(context_, cte_info_) { }
    PlanNodePtr visitApplyNode(ApplyNode &, Void &) override;
};

/**
 * Pattern match
 *
 * 1 correlation columns not exist
 * 2 subquery is in subquery
 *
 * Transforms:
 *
 * <pre>
 * Filter(a IN b):
 *   Apply
 *     - correlation: []  // empty
 *     - input: some plan A producing symbol a
 *     - subquery: some plan B producing symbol b
 * </pre>
 *
 * Into:
 * <pre>
 * Filter(non-null = 1):
 *   Left Join (a = b)
 *     - source: plan A
 *     - Project: symbol non-null (default value = 1)
 *          - Distinct: symbol b
 *              - Source: symbol b
 * </pre>
*/
class RemoveUnCorrelatedInSubquery : public Rewriter
{
public:
    String name() const override { return "RemoveUnCorrelatedInSubquery"; }

private:
    bool rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override { return context->getSettingsRef().enable_remove_uncorrelated_in_subquery; }
};

class UnCorrelatedInSubqueryVisitor : public SimplePlanRewriter<Void>
{
public:
    UnCorrelatedInSubqueryVisitor(ContextMutablePtr context_, CTEInfo & cte_info_) : SimplePlanRewriter(context_, cte_info_) { }
    PlanNodePtr visitApplyNode(ApplyNode &, Void &) override;
};

/**
 * Pattern match
 *
 * 1 correlation columns exist
 * 2 subquery is exists subquery
 */
class RemoveCorrelatedExistsSubquery : public Rewriter
{
public:
    String name() const override { return "RemoveCorrelatedExistsSubquery"; }

private:
    bool rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override { return context->getSettingsRef().enable_remove_correlated_exists_subquery; }
};

class CorrelatedExistsSubqueryVisitor : public SimplePlanRewriter<Void>
{
public:
    CorrelatedExistsSubqueryVisitor(ContextMutablePtr context_, CTEInfo & cte_info_) : SimplePlanRewriter(context_, cte_info_) { }
    PlanNodePtr visitApplyNode(ApplyNode &, Void &) override;
};

/**
 * Pattern match
 *
 * 1 correlation columns not exist
 * 2 subquery is exists subquery
 */
class RemoveUnCorrelatedExistsSubquery : public Rewriter
{
public:
    String name() const override { return "RemoveUnCorrelatedExistsSubquery"; }

private:
    bool rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override
    {
        return context->getSettingsRef().enable_remove_uncorrelated_exists_subquery;
    }
};

class UnCorrelatedExistsSubqueryVisitor : public SimplePlanRewriter<Void>
{
public:
    UnCorrelatedExistsSubqueryVisitor(ContextMutablePtr context_, CTEInfo & cte_info_) : SimplePlanRewriter(context_, cte_info_) { }
    PlanNodePtr visitApplyNode(ApplyNode &, Void &) override;
};

/**
 * Pattern match
 *
 * 1 correlation columns not exist
 * 2 subquery is quantified comparison subquery
 */
class RemoveUnCorrelatedQuantifiedComparisonSubquery : public Rewriter
{
public:
    String name() const override { return "RemoveUnCorrelatedQuantifiedComparisonSubquery"; }

private:
    bool rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override
    {
        return context->getSettingsRef().enable_remove_uncorrelated_quantified_comparison_subquery;
    }
};

class UnCorrelatedQuantifiedComparisonSubqueryVisitor : public SimplePlanRewriter<Void>
{
public:
    UnCorrelatedQuantifiedComparisonSubqueryVisitor(ContextMutablePtr context_, CTEInfo & cte_info) : SimplePlanRewriter(context_, cte_info)
    {
    }

private:
    PlanNodePtr visitApplyNode(ApplyNode &, Void &) override;
};

/**
 * Pattern match
 *
 * 1 correlation columns exist
 * 2 subquery is quantified comparison subquery
 */
class RemoveCorrelatedQuantifiedComparisonSubquery : public Rewriter
{
public:
    String name() const override { return "RemoveCorrelatedQuantifiedComparisonSubquery"; }

private:
    bool rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override
    {
        return context->getSettingsRef().enable_remove_correlated_quantified_comparison_subquery;
    }
};

class CorrelatedQuantifiedComparisonSubqueryVisitor : public SimplePlanRewriter<Void>
{
public:
    CorrelatedQuantifiedComparisonSubqueryVisitor(ContextMutablePtr context_, CTEInfo & cte_info) : SimplePlanRewriter(context_, cte_info)
    {
    }

private:
    PlanNodePtr visitApplyNode(ApplyNode &, Void &) override;
};


class UnnestingWithWindow : public Rule
{
public:
    RuleType getType() const override { return RuleType::UNNESTING_WITH_WINDOW; }
    String getName() const override { return "UNNESTING_WITH_WINDOW"; }
    ConstRefPatternPtr getPattern() const override;
    bool isEnabled(ContextPtr context) const override { return context->getSettingsRef().enable_unnesting_subquery_with_window; }

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class UnnestingWithProjectionWindow : public UnnestingWithWindow
{
public:
    RuleType getType() const override { return RuleType::UNNESTING_WITH_PROJECTION_WINDOW; }
    String getName() const override { return "UNNESTING_WITH_PROJECTION_WINDOW"; }
    bool isEnabled(ContextPtr context) const override { return context->getSettingsRef().enable_unnesting_subquery_with_window; }
    ConstRefPatternPtr getPattern() const override;
};

class ExistsToSemiJoin : public Rule
{
public:
    RuleType getType() const override { return RuleType::EXISTS_TO_SEMI_JOIN; }
    String getName() const override { return "EXISTS_TO_SEMI_JOIN"; }
    bool isEnabled(ContextPtr context) const override { return context->getSettingsRef().enable_unnesting_subquery_with_semi_anti_join; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class InToSemiJoin : public Rule
{
public:
    RuleType getType() const override { return RuleType::IN_TO_SEMI_JOIN; }
    String getName() const override { return "IN_TO_SEMI_JOIN"; }
    bool isEnabled(ContextPtr context) const override { return context->getSettingsRef().enable_unnesting_subquery_with_semi_anti_join; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
