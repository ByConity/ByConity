#include <Optimizer/ExpressionRewriter.h>
#include <Optimizer/ExpressionUtils.h>

namespace DB
{

Void FunctionExtractorVisitor::visitNode(const ConstASTPtr & node, std::set<String> & context)
{
    for (ConstASTPtr child : node->children)
    {
        ASTVisitorUtil::accept(child, *this, context);
    }
    return Void{};
}

Void FunctionExtractorVisitor::visitASTFunction(const ConstASTPtr & node, std::set<String> & context)
{
    auto & func = node->as<ASTFunction &>();
    context.emplace(func.name);
    return visitNode(node, context);
}

}
