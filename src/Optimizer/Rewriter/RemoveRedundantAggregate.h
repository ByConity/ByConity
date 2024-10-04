#pragma once

#include <Optimizer/Rewriter/Rewriter.h>
#include <QueryPlan/CTEVisitHelper.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/SimplePlanRewriter.h>

namespace DB
{
class RemoveRedundantDistinct : public Rewriter
{
public:
    String name() const override { return "RemoveRedundantDistinct"; }

private:
    bool rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override { return context->getSettingsRef().enable_distinct_remove; }
};
struct RemoveRedundantAggregateContext
{
    ContextMutablePtr context;
    std::vector<NameSet> distincts;
};
class RemoveRedundantAggregateVisitor : public PlanNodeVisitor<PlanNodePtr, RemoveRedundantAggregateContext>
{
public:
    explicit RemoveRedundantAggregateVisitor(ContextMutablePtr context_, CTEInfo & cte_info, PlanNodePtr &)
        : cte_helper(cte_info), context(context_)
    {
    }

private:
    PlanNodePtr visitDistinctNode(DistinctNode & node, RemoveRedundantAggregateContext & context) override;
    PlanNodePtr visitProjectionNode(ProjectionNode & node, RemoveRedundantAggregateContext & context) override;
    PlanNodePtr visitLimitByNode(LimitByNode & node, RemoveRedundantAggregateContext & context) override;
    PlanNodePtr visitFilterNode(FilterNode & node, RemoveRedundantAggregateContext & context) override;
    PlanNodePtr visitCTERefNode(CTERefNode & node, RemoveRedundantAggregateContext & context) override;
    PlanNodePtr visitJoinNode(JoinNode & node, RemoveRedundantAggregateContext & context) override;
    PlanNodePtr visitTableScanNode(TableScanNode & node, RemoveRedundantAggregateContext & context) override;
    PlanNodePtr visitAggregatingNode(AggregatingNode & node, RemoveRedundantAggregateContext & context) override;
    PlanNodePtr visitPlanNode(PlanNodeBase & node, RemoveRedundantAggregateContext & context) override;
    bool isDistinctNames(const Names &, const NameSet &);
    std::set<std::string> extractSymbol(const ConstASTPtr & node);
    PlanNodePtr resetChildren(PlanNodeBase & node, PlanNodes & children, RemoveRedundantAggregateContext & context);

    SimpleCTEVisitHelper<PlanNodePtr> cte_helper;
    std::unordered_map<CTEId, std::vector<NameSet>> visit_results;
    ContextMutablePtr context;
};
}
