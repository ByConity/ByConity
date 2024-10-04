#pragma once

#include <Optimizer/Rewriter/Rewriter.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/SimplePlanRewriter.h>


namespace DB
{

struct TrivialCountContext
{
    bool has_other_node = false;
    ASTs filters;
    StoragePtr storage;
    ASTPtr query;
    NamesWithAliases column_alias;
    bool pushdowned_index_projection = false;
};

/**
 * COUNT optimization
 * When calling the count function, if count() or count(*) is used
 * and also meet the following conditions, the number of rows will be read directly from storage.
 * For example:
 * select count()/count(*) from table1;
 * select count()/count(*) from table1 prewhere i < 1 where j > 1;  -- Both i and j are partition keys
 *
 * select count()/count(*) from table1 where i+i < 1; -- Even though i is the partition key, this count() can't be optimized either
 *
 *  Conditions
 *  1. There is only one aggregation function in the AggregatingNode, and the function type is AggregateFunctionCount, and the size of output header columns is 1;
 *  2. There are no nodes below this node that change the number of output lines except for the FilterNode；
 *  3. Where and prewhere clauses are empty or the columns involved in the where and prewhere clauses are PartitionKey；
 *  4. Can get the row count in storage;
 *  5. setting optimize_trivial_count_query=1 and max_parallel_replicas=1;
 *
 *
 */


class OptimizeTrivialCount : public Rewriter
{
public:
    String name() const override { return "OptimizeTrivialCount"; }

private:
    bool rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override { return context->getSettingsRef().optimize_trivial_count_query; }
};

class TrivialCountVisitor : public SimplePlanRewriter<Void>
{
public:
    explicit TrivialCountVisitor(ContextMutablePtr context_, CTEInfo & cte_info_) : SimplePlanRewriter(context_, cte_info_) { }

    static NameSet getRequiredColumns(ASTs & filters);
    static ASTs replaceColumnsAlias(ASTs & filters, NamesWithAliases & column_alias);

private:
    PlanNodePtr visitAggregatingNode(AggregatingNode & node, Void &) override;
};

class CountContextVisitor : public PlanNodeVisitor<void, TrivialCountContext>
{
private:
    void visitProjectionNode(ProjectionNode & node, TrivialCountContext &) override;
    void visitFilterNode(FilterNode & node, TrivialCountContext & context) override;
    void visitSortingNode(SortingNode & node, TrivialCountContext &) override;
    void visitTableScanNode(TableScanNode & node, TrivialCountContext &) override;
    void visitPlanNode(PlanNodeBase & node, TrivialCountContext &) override;
};
}
