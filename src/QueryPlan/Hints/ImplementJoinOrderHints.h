#pragma once

#include <Interpreters/Context.h>
#include <Optimizer/JoinGraph.h>
#include <Optimizer/Rewriter/Rewriter.h>
#include <Parsers/ASTVisitor.h>
#include <QueryPlan/Hints/Leading.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/SimplePlanRewriter.h>

#include <utility>

namespace DB
{
using LeadingPtr = std::shared_ptr<Leading>;
using Leading_RPN_List = std::vector<DB::Int64>;

class ImplementJoinOrderHints : public Rewriter
{
public:
    String name() const override { return "ImplementJoinOrderHints"; }

private:
    void rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override { return context->getSettingsRef().enable_join_order_hint; }
};

class JoinOrderHintsVisitor : public SimplePlanRewriter<Void>
{
public:
    explicit JoinOrderHintsVisitor(ContextMutablePtr context_, CTEInfo & cte_info_) : SimplePlanRewriter(context_, cte_info_) { }
    PlanNodePtr visitJoinNode(JoinNode &, Void &) override;

    Leading_RPN_List buildLeadingList(JoinGraph & graph, LeadingPtr & leading);

private:
    PlanNodePtr getJoinOrder(JoinGraph & graph, LeadingPtr & leading_hint);
};

//get the table name list of a node
class TableNameVisitor : public PlanNodeVisitor<void, String>
{
private:
    void visitTableScanNode(TableScanNode & node, String & table_name) override;
    void visitProjectionNode(ProjectionNode & node, String & table_name) override;
    void visitFilterNode(FilterNode & node, String & table_name) override;
    void visitPlanNode(PlanNodeBase & node, String & table_name) override;
};

}
