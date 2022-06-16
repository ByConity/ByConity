#pragma once

#include <Interpreters/Context.h>
#include <Optimizer/JoinGraph.h>
#include <Optimizer/Rewriter/Rewriter.h>
#include <Parsers/ASTVisitor.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/SimplePlanRewriter.h>

#include <utility>

namespace DB
{
class SimplifyCrossJoin : public Rewriter
{
public:
    void rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    String name() const override { return "SimplifyCrossJoin"; }
};

class SimplifyCrossJoinVisitor : public SimplePlanRewriter<Void>
{
public:
    explicit SimplifyCrossJoinVisitor(ContextMutablePtr context_, CTEInfo & cte_info) : SimplePlanRewriter(context_, cte_info) { }
    PlanNodePtr visitJoinNode(JoinNode &, Void &) override;

private:
    std::unordered_set<PlanNodeId> reordered;
    static bool isOriginalOrder(std::vector<UInt32> & join_order);
    static std::vector<UInt32> getJoinOrder(JoinGraph & graph);
    PlanNodePtr buildJoinTree(std::vector<String> & expected_output_symbols, JoinGraph & graph, std::vector<UInt32> & join_order);
};

class ComparePlanNode
{
public:
    std::unordered_map<PlanNodeId, UInt32> priorities;

    explicit ComparePlanNode(std::unordered_map<PlanNodeId, UInt32> priorities_) : priorities(std::move(priorities_)) { }

    bool operator()(const PlanNodePtr & node1, const PlanNodePtr & node2)
    {
        PlanNodeId id1 = node1->getId();
        PlanNodeId id2 = node2->getId();
        UInt32 value1 = priorities[id1];
        UInt32 value2 = priorities[id2];
        return value1 > value2;
    }
};

}
