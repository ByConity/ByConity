#pragma once

#include <Interpreters/Context.h>
#include <Optimizer/Rewriter/Rewriter.h>
#include <Optimizer/Rule/Transformation/JoinEnumOnGraph.h>
#include <QueryPlan/SimplePlanVisitor.h>

namespace DB
{
class UnifyJoinOutputs : public Rewriter
{
public:
    void rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    String name() const override { return "UnifyJoinOutputs"; }

private:
    class UnionFindExtractor;
    class Rewriter;
};

class UnifyJoinOutputs::UnionFindExtractor : public SimplePlanVisitor<std::unordered_map<PlanNodeId, UnionFind>>
{
public:
    static std::unordered_map<PlanNodeId, UnionFind> extract(QueryPlan & plan);

private:
    explicit UnionFindExtractor(CTEInfo & cte_info) : SimplePlanVisitor(cte_info) { }
    Void visitJoinNode(JoinNode &, std::unordered_map<PlanNodeId, UnionFind> &) override;
    Void visitCTERefNode(CTERefNode & node, std::unordered_map<PlanNodeId, UnionFind> & context) override;
};

class UnifyJoinOutputs::Rewriter : public PlanNodeVisitor<PlanNodePtr, std::set<String>>
{
public:
    Rewriter(ContextMutablePtr context_, CTEInfo & cte_info_, std::unordered_map<PlanNodeId, UnionFind> & union_find_map_)
        : context(context_), cte_helper(cte_info_), union_find_map(union_find_map_)
    {
    }
    PlanNodePtr visitPlanNode(PlanNodeBase &, std::set<String> &) override;
    PlanNodePtr visitJoinNode(JoinNode &, std::set<String> &) override;
    PlanNodePtr visitCTERefNode(CTERefNode & node, std::set<String> &) override;

private:
    ContextMutablePtr context;
    CTEPreorderVisitHelper cte_helper;
    std::unordered_map<PlanNodeId, UnionFind> & union_find_map;
};
}
