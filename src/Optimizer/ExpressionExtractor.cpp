#include <Optimizer/ExpressionExtractor.h>

#include <Optimizer/PredicateUtils.h>
#include <QueryPlan/AggregatingStep.h>
#include <QueryPlan/ApplyStep.h>
#include <QueryPlan/FilterStep.h>
#include <QueryPlan/ProjectionStep.h>

namespace DB
{
std::vector<ConstASTPtr> ExpressionExtractor::extract(PlanNodePtr & node)
{
    std::vector<ConstASTPtr> expressions;
    ExpressionVisitor visitor;
    VisitorUtil::accept(node, visitor, expressions);
    return expressions;
}

Void ExpressionVisitor::visitPlanNode(PlanNodeBase & node, std::vector<ConstASTPtr> & expressions)
{
    for (const auto & item : node.getChildren())
    {
        VisitorUtil::accept(*item, *this, expressions);
    }
    return Void{};
}

Void ExpressionVisitor::visitProjectionNode(ProjectionNode & node, std::vector<ConstASTPtr> & expressions)
{
    const auto & step = *node.getStep();
    auto assignments = step.getAssignments();
    for (auto & ass : assignments)
    {
        expressions.emplace_back(ass.second->clone());
    }
    return visitPlanNode(node, expressions);
}

Void ExpressionVisitor::visitFilterNode(FilterNode & node, std::vector<ConstASTPtr> & expressions)
{
    const auto & step = *node.getStep();
    ASTPtr filter = step.getFilter()->clone();
    expressions.emplace_back(filter);
    return visitPlanNode(node, expressions);
}

Void ExpressionVisitor::visitAggregatingNode(AggregatingNode & node, std::vector<ConstASTPtr> & expressions)
{
    const auto & step = *node.getStep();
    for (const auto & agg : step.getAggregates())
    {
        for (const auto & name : agg.argument_names)
        {
            expressions.emplace_back(std::make_shared<ASTIdentifier>(name));
        }
    }
    return visitPlanNode(node, expressions);
}

Void ExpressionVisitor::visitApplyNode(ApplyNode & node, std::vector<ConstASTPtr> & expressions)
{
    const auto & step = *node.getStep();
    expressions.emplace_back(step.getAssignment().second->clone());
    return visitPlanNode(node, expressions);
}

Void ExpressionVisitor::visitJoinNode(JoinNode & node, std::vector<ConstASTPtr> & expressions)
{
    const auto & step = *node.getStep();
    if (!PredicateUtils::isTruePredicate(step.getFilter()))
    {
        expressions.emplace_back(step.getFilter()->clone());
    }
    return visitPlanNode(node, expressions);
}

ConstASTSet SubExpressionExtractor::extract(ConstASTPtr node)
{
    ConstASTSet result;
    if (node == nullptr)
    {
        return result;
    }
    SubExpressionVisitor visitor;
    ASTVisitorUtil::accept(node, visitor, result);
    return result;
}

Void SubExpressionVisitor::visitNode(const ConstASTPtr & node, ConstASTSet & context)
{
    for (ConstASTPtr child : node->children)
    {
        ASTVisitorUtil::accept(child, *this, context);
    }
    return Void{};
}

Void SubExpressionVisitor::visitASTFunction(const ConstASTPtr & node, ConstASTSet & context)
{
    context.emplace(node);
    return visitNode(node, context);
}

Void SubExpressionVisitor::visitASTIdentifier(const ConstASTPtr & node, ConstASTSet & context)
{
    context.emplace(node);
    return Void{};
}

}
