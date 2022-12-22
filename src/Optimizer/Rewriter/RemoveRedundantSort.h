#include <Optimizer/Rewriter/Rewriter.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/SimplePlanRewriter.h>


namespace DB
{

class RemoveRedundantSort : public Rewriter
{
public:
    void rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    String name() const override { return "RemoveRedundantSort"; }
};

struct RedundantSortContext
{
    ContextMutablePtr context;
    bool can_sort_be_removed = false;
};

class RedundantSortVisitor : public SimplePlanRewriter<RedundantSortContext>
{
public:
    explicit RedundantSortVisitor(ContextMutablePtr context_, CTEInfo & cte_info_, PlanNodePtr & root)
        : SimplePlanRewriter(context_, cte_info_), post_order_cte_helper(cte_info_, root)
    {}

    static bool isStateful(ConstASTPtr expression, ContextMutablePtr context);
    static bool isOrderDependentAggregateFunction(const String& aggname);
    const static std::unordered_set<String> order_dependent_agg;

private:
    PlanNodePtr visitProjectionNode(ProjectionNode & node, RedundantSortContext & sort_context) override;
    PlanNodePtr visitAggregatingNode(AggregatingNode & node, RedundantSortContext & sort_context) override;
    PlanNodePtr visitJoinNode(JoinNode & node, RedundantSortContext & sort_context) override;
    PlanNodePtr visitUnionNode(UnionNode & node, RedundantSortContext & sort_context) override;
    PlanNodePtr visitIntersectNode(IntersectNode & node, RedundantSortContext & sort_context) override;
    PlanNodePtr visitExceptNode(ExceptNode & node, RedundantSortContext & sort_context) override;
    PlanNodePtr visitLimitNode(LimitNode & node, RedundantSortContext & sort_context) override;
    PlanNodePtr visitLimitByNode(LimitByNode & node, RedundantSortContext & sort_context) override;
    PlanNodePtr visitSortingNode(SortingNode & node, RedundantSortContext & sort_context) override;
    PlanNodePtr visitCTERefNode(CTERefNode & node, RedundantSortContext & sort_context) override;

    PlanNodePtr processChildren(PlanNodeBase & node, RedundantSortContext & sort_context);
    PlanNodePtr resetChild(PlanNodeBase & node, PlanNodes & children, RedundantSortContext & sort_context);

    CTEPostorderVisitHelper post_order_cte_helper;
    std::unordered_map<CTEId, RedundantSortContext> cte_require_context{};

};

class StatefulVisitor : public ConstASTVisitor<void, ContextMutablePtr>
{
public:
    void visitNode(const ConstASTPtr & node, ContextMutablePtr & context) override;
    void visitASTFunction(const ConstASTPtr & node, ContextMutablePtr & context) override;
    bool isStateful() const { return is_stateful; }

private:
    bool is_stateful = false;
};
}
