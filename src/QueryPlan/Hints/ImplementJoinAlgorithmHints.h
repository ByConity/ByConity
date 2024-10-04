#pragma once
#include <Optimizer/Rewriter/Rewriter.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/SimplePlanRewriter.h>


namespace DB
{
using JoinStepPtr = std::shared_ptr<JoinStep>;
using HintsStringSet = std::unordered_set<String>;

class ImplementJoinAlgorithmHints : public Rewriter
{
public:
    String name() const override;

private:
    bool rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override { return context->getSettingsRef().enable_join_algorithm_hints; }
};

class JoinAlgorithmHintsVisitor : public PlanNodeVisitor<HintsStringSet, Void>
{
public:
    explicit JoinAlgorithmHintsVisitor(ContextMutablePtr /*context_*/, CTEInfo & cte_info_) : cte_helper(cte_info_)
    {
    }

private:
    HintsStringSet visitJoinNode(JoinNode & node, Void &) override;
    HintsStringSet visitPlanNode(PlanNodeBase & node, Void &) override;
    HintsStringSet visitCTERefNode(CTERefNode & node, Void &) override;
    HintsStringSet visitTableScanNode(TableScanNode & node, Void &) override;

    SimpleCTEVisitHelper<void> cte_helper;
};

}
