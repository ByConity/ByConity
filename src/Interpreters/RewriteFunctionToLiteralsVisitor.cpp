#include <Interpreters/RewriteFunctionToLiteralsVisitor.h>
#include <Interpreters/evaluateConstantExpression.h>



namespace DB
{
void RewriteFunctionToLiteralsMatcher::visit(ASTPtr & ast, Data & /*data*/)
{
    if (auto * func = ast->as<ASTFunction>())
        if (auto literal = func->toLiteral())
            ast = std::move(literal);
}
}
