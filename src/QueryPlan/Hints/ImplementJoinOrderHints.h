#pragma once

#include <Interpreters/Context.h>
#include <Optimizer/JoinGraph.h>
#include <Optimizer/Rewriter/Rewriter.h>
#include <Parsers/ASTVisitor.h>
#include <QueryPlan/Hints/Leading.h>
#include <QueryPlan/Hints/SwapJoinOrder.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/SimplePlanRewriter.h>

#include <utility>

namespace DB
{
using SwapOrderPtr = std::shared_ptr<SwapJoinOrder>;
using LeadingPtr = std::shared_ptr<Leading>;
using Leading_RPN_List = std::vector<DB::Int64>;

class ImplementJoinOrderHints : public Rewriter
{
public:
    String name() const override { return "ImplementJoinOrderHints"; }

private:
    bool rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override { return context->getSettingsRef().enable_join_order_hint; }
};

class JoinOrderHintsVisitor : public SimplePlanRewriter<Void>
{
public:
    explicit JoinOrderHintsVisitor(ContextMutablePtr context_, CTEInfo & cte_info_) : SimplePlanRewriter(context_, cte_info_) { }
    PlanNodePtr visitJoinNode(JoinNode &, Void &) override;

private:
    PlanNodePtr getLeadingJoinOrder(PlanNodePtr join_ptr, LeadingPtr & leading_hint);
    PlanNodePtr buildLeadingJoinOrder(JoinGraph & graph, LeadingPtr & leading_hint);
    static Leading_RPN_List buildLeadingList(JoinGraph & graph, LeadingPtr & leading);

    PlanNodePtr swapJoinOrder(PlanNodePtr node, SwapOrderPtr & swap_hint);
};

//get the table name list of a node
class TableNameVisitor : public PlanNodeVisitor<void, String>
{
private:
    void visitTableScanNode(TableScanNode & node, String & table_name) override;
    void visitPlanNode(PlanNodeBase & node, String & table_name) override;
};

}
