#include <Interpreters/RewriteFunctionToLiteralsVisitor.h>
#include <Interpreters/evaluateConstantExpression.h>



namespace DB
{
/// TODO @canh: more complexed rules can be apply for ast folding here
void RewriteFunctionToLiteralsMatcher::visit(ASTPtr & ast, Data & /*data*/)
{
    if (auto * func = ast->as<ASTFunction>())
    {
        if (auto literal = func->toLiteral())
        {
            literal->setAlias(func->tryGetAlias());
            ast = std::move(literal);
        }
    }
}
}
