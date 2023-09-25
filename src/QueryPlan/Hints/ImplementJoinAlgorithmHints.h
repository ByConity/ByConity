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
    void rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    String name() const override;
};

class JoinAlgorithmHintsVisitor : public PlanNodeVisitor<HintsStringSet, Void>
{
public:
    explicit JoinAlgorithmHintsVisitor(ContextMutablePtr & context_, CTEInfo & cte_info_, PlanNodePtr & root)
        : context(context_), post_order_cte_helper(cte_info_, root), cte_info(cte_info_)
    {
    }

private:
    HintsStringSet visitJoinNode(JoinNode & node, Void &) override;
    HintsStringSet visitPlanNode(PlanNodeBase & node, Void &) override;
    HintsStringSet visitCTERefNode(CTERefNode & node, Void &) override;
    HintsStringSet visitTableScanNode(TableScanNode & node, Void &) override;

    ContextMutablePtr & context;
    CTEPostorderVisitHelper post_order_cte_helper;
    CTEInfo & cte_info;
};

}
