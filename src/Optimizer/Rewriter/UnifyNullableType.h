#pragma once

#include <Interpreters/Context.h>
#include <Optimizer/Rewriter/Rewriter.h>
#include <QueryPlan/SimplePlanRewriter.h>

namespace DB
{
/**
 * Unify Nullable type.
 *
 * For left join, the un-matched value will be append null in normal.
 * but in clickhouse, it depends on the column type. if the column type
 * is Nullable, the un-matched value is null, otherwise, is 0.
 *
 * This is not compatible with standard sql behavior. so clickhouse has
 * a config: join_use_nulls. when it set true, clickhouse will output null.
 *
 * Hence, the type of (left/right/full) join output columns should reset,
 * and all the other plan nodes, which relay on the join output columns,
 * should reset too. otherwise will throw runtime exception when convert
 * NULL value to Not nullable column.
 */
class UnifyNullableType : public Rewriter
{
public:
    void rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    String name() const override { return "UnifyNullableType"; }
};

class UnifyNullableVisitor : public SimplePlanRewriter<Void>
{
public:
    UnifyNullableVisitor(ContextMutablePtr context_, CTEInfo & cte_info_) : SimplePlanRewriter(context_, cte_info_) { }
    PlanNodePtr visitProjectionNode(ProjectionNode &, Void &) override;
    PlanNodePtr visitFilterNode(FilterNode &, Void &) override;
    PlanNodePtr visitJoinNode(JoinNode &, Void &) override;
    PlanNodePtr visitAggregatingNode(AggregatingNode &, Void &) override;
    PlanNodePtr visitWindowNode(WindowNode &, Void &) override;
    PlanNodePtr visitUnionNode(UnionNode &, Void &) override;
    PlanNodePtr visitExchangeNode(ExchangeNode &, Void &) override;
    PlanNodePtr visitPartialSortingNode(PartialSortingNode &, Void &) override;
    PlanNodePtr visitMergeSortingNode(MergeSortingNode &, Void &) override;
    PlanNodePtr visitLimitNode(LimitNode &, Void &) override;
    PlanNodePtr visitEnforceSingleRowNode(EnforceSingleRowNode &, Void &) override;
    PlanNodePtr visitAssignUniqueIdNode(AssignUniqueIdNode &, Void &) override;
};

}
