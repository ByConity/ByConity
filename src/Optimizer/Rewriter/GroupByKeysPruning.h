#pragma once


#include <Interpreters/Context.h>
#include <Optimizer/DataDependency/DataDependency.h>
#include <Optimizer/DataDependency/DataDependencyDeriver.h>
#include <Optimizer/Property/Constants.h>
#include <Optimizer/Property/Equivalences.h>
#include <Optimizer/Rewriter/Rewriter.h>
#include <QueryPlan/SimplePlanRewriter.h>
#include <QueryPlan/SimplePlanVisitor.h>

namespace DB
{

class GroupByKeysPruning : public Rewriter
{
public:
    String name() const override { return "GroupByKeysPruning"; }

private:
    bool rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override { return context->getSettingsRef().enable_group_by_keys_pruning; }
    class Rewriter;
};

struct PlanAndDataDependencyWithConstants
{
    PlanNodePtr plan;
    DataDependency data_dependency;
    Constants constants;
};

class GroupByKeysPruning::Rewriter : public PlanNodeVisitor<PlanAndDataDependencyWithConstants, Void>
{
public:
    explicit Rewriter(ContextMutablePtr context_, CTEInfo & cte_info_) : context(context_), cte_helper(cte_info_) { }
    PlanAndDataDependencyWithConstants visitPlanNode(PlanNodeBase &, Void &) override;
    // PlanAndDataDependency visitJoinNode(JoinNode &, Void &) override;
    // PlanAndDataDependency visitTableScanNode(TableScanNode &, Void &) override;
    // PlanAndDataDependency visitFilterNode(FilterNode &, Void &) override;
    // PlanAndDataDependency visitUnionNode(UnionNode &, Void &) override;
    // PlanAndDataDependency visitExchangeNode(ExchangeNode &, Void &) override;
    PlanAndDataDependencyWithConstants visitAggregatingNode(AggregatingNode &, Void &) override;
    PlanAndDataDependencyWithConstants visitCTERefNode(CTERefNode &, Void &) override;

private:
    ContextMutablePtr context;
    SimpleCTEVisitHelper<PlanAndDataDependencyWithConstants> cte_helper;
};


}
