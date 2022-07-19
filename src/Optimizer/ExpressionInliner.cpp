#include <Optimizer/ExpressionInliner.h>

namespace DB
{
ASTPtr ExpressionInliner::inlineSymbols(const ConstASTPtr & predicate, const Assignments & mapping)
{
    InlinerVisitor visitor;
    return ASTVisitorUtil::accept(predicate->clone(), visitor, mapping);
}

ASTPtr InlinerVisitor::visitASTIdentifier(ASTPtr & node, const Assignments & context)
{
    auto & identifier = node->as<ASTIdentifier &>();
    if (context.count(identifier.name()))
        return context.at(identifier.name())->clone();
    return node;
}

}
