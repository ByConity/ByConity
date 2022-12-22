#include <QueryPlan/PlanPattern.h>

#include <Optimizer/JoinGraph.h>
#include <QueryPlan/JoinStep.h>

namespace DB
{
bool PlanPattern::isSimpleQuery(QueryPlan & plan)
{
    SimpleQueryPlanPatternVisitor visitor{plan.getCTEInfo()};
    Void context;
    VisitorUtil::accept(plan.getPlanNode(), visitor, context);
    return visitor.isSimpleQuery();
}

bool PlanPattern::hasCrossJoin(QueryPlan & plan)
{
    CrossJoinPlanPatternVisitor visitor{plan.getCTEInfo()};
    Void context;
    VisitorUtil::accept(plan.getPlanNode(), visitor, context);
    return visitor.hasCrossJoin();
}

bool PlanPattern::hasOuterJoin(QueryPlan & plan)
{
    OuterJoinPlanPatternVisitor visitor{plan.getCTEInfo()};
    Void context;
    VisitorUtil::accept(plan.getPlanNode(), visitor, context);
    return visitor.hasOuterJoin();
}

size_t PlanPattern::maxJoinSize(QueryPlan & plan, ContextMutablePtr & context)
{
    GetMaxJoinSizeVisitor visitor{context, plan.getCTEInfo()};
    Void v;
    VisitorUtil::accept(plan.getPlanNode(), visitor, v);
    return visitor.getMaxSize();
}

Void SimpleQueryPlanPatternVisitor::visitJoinNode(JoinNode &, Void &)
{
    simple_query = false;
    return Void{};
}

Void SimpleQueryPlanPatternVisitor::visitApplyNode(ApplyNode &, Void &)
{
    simple_query = false;
    return Void{};
}

Void SimpleQueryPlanPatternVisitor::visitIntersectNode(IntersectNode &, Void &)
{
    simple_query = false;
    return Void{};
}

Void SimpleQueryPlanPatternVisitor::visitExceptNode(ExceptNode &, Void &)
{
    simple_query = false;
    return Void{};
}

Void SimpleQueryPlanPatternVisitor::visitCTERefNode(CTERefNode &, Void &)
{
    simple_query = false;
    return Void{};
}

Void CrossJoinPlanPatternVisitor::visitJoinNode(JoinNode & node, Void & context)
{
    visitPlanNode(node, context);

    const auto & join_step = *node.getStep();

    if (join_step.isCrossJoin())
        has_cross_join = true;

    return Void{};
}

Void OuterJoinPlanPatternVisitor::visitJoinNode(JoinNode & node, Void & context)
{
    visitPlanNode(node, context);

    const auto & join_step = *node.getStep();

    if (join_step.getKind() == ASTTableJoin::Kind::Left || join_step.getKind() == ASTTableJoin::Kind::Right
        || join_step.getKind() == ASTTableJoin::Kind::Full)
    {
        has_outer_join = true;
    }
    return Void{};
}

Void GetMaxJoinSizeVisitor::visitJoinNode(JoinNode & node, Void & v)
{
    visitPlanNode(node, v);
    JoinGraph join_graph = JoinGraph::build(node.shared_from_this(), context);
    if (join_graph.size() > max_size)
    {
        max_size = join_graph.size();
    }
    return Void{};
}
}
