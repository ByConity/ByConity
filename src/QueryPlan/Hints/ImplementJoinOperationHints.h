#pragma once
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/Rewriter/Rewriter.h>
#include <QueryPlan/Assignment.h>
#include <QueryPlan/JoinStep.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/SimplePlanRewriter.h>


namespace DB
{
using JoinStepPtr = std::shared_ptr<JoinStep>;

class ImplementJoinOperationHints : public Rewriter
{
public:
    String name() const override { return "ImplementJoinOperationHints"; }

private:
    bool rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override { return context->getSettingsRef().enable_join_operation_hints; }
};

class JoinOperationHintsVisitor : public PlanNodeVisitor<void, Void>
{
public:
    explicit JoinOperationHintsVisitor(ContextMutablePtr & context_, CTEInfo & cte_info_) : context(context_), cte_helper(cte_info_)
    {
    }

    static bool supportSwap(const JoinStep & s) { return s.supportSwap() && PredicateUtils::isTruePredicate(s.getFilter()); }

private:
    void visitJoinNode(JoinNode & node, Void &) override;
    void visitPlanNode(PlanNodeBase & node, Void &) override;
    void visitCTERefNode(CTERefNode & node, Void &) override;

    static void setStepOptions(JoinStepPtr & step, DistributionType distribution_type, bool isOrdered = false);

    ContextMutablePtr & context;
    SimpleCTEVisitHelper<void> cte_helper;
};

struct TableScanContext
{
    PlanHints hint_list = {};
};
class TableScanHintVisitor : public PlanNodeVisitor<void, TableScanContext>
{
private:
    void visitTableScanNode(TableScanNode & node, TableScanContext & table_names) override;
    void visitPlanNode(PlanNodeBase & node, TableScanContext & table_names) override;
};

}
