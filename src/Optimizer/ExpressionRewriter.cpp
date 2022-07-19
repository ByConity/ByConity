#include <Optimizer/ExpressionRewriter.h>

namespace DB
{
ASTPtr ExpressionRewriter::rewrite(const ConstASTPtr & expression, ConstASTMap & expression_map)
{
    ExpressionRewriterVisitor visitor;
    return ASTVisitorUtil::accept(expression->clone(), visitor, expression_map);
}

ASTPtr ExpressionRewriterVisitor::visitNode(ASTPtr & node, ConstASTMap & expression_map)
{
    auto result = SimpleExpressionRewriter::visitNode(node, expression_map);
    if (expression_map.contains(result))
        return expression_map[result]->clone();
    return result;
}
}
