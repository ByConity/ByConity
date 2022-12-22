#include <Optimizer/ExpressionExtractor.h>
#include <Optimizer/SymbolsExtractor.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{
std::set<std::string> SymbolsExtractor::extract(ConstASTPtr node)
{
    static SymbolVisitor visitor;
    std::set<std::string> context;
    ASTVisitorUtil::accept(node, visitor, context);
    return context;
}

std::set<std::string> SymbolsExtractor::extract(PlanNodePtr & node)
{
    std::vector<ConstASTPtr> expressions;
    for (ConstASTPtr expr : ExpressionExtractor::extract(node))
    {
        expressions.emplace_back(expr);
    }
    return extract(expressions);
}

std::set<std::string> SymbolsExtractor::extract(std::vector<ConstASTPtr> & nodes)
{
    static SymbolVisitor visitor;
    std::set<std::string> context;
    for (auto & node : nodes)
    {
        ASTVisitorUtil::accept(node, visitor, context);
    }
    return context;
}

Void SymbolVisitor::visitNode(const ConstASTPtr & node, std::set<std::string> & context)
{
    for (ConstASTPtr child : node->children)
    {
        ASTVisitorUtil::accept(child, *this, context);
    }
    return Void{};
}

Void SymbolVisitor::visitASTIdentifier(const ConstASTPtr & node, std::set<std::string> & context)
{
    auto & identifier = node->as<ASTIdentifier &>();
    context.emplace(identifier.name());
    return Void{};
}

}
