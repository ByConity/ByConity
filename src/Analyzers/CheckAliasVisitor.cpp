#include <Analyzers/CheckAliasVisitor.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/formatAST.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

void CheckAliasVisitor::visit(ASTPtr & ast)
{
    bool is_alias_allowed = isSelectItem() || isTableExpressionElement();

    if (!is_alias_allowed && !ast->tryGetAlias().empty())
        throw Exception("Illegal alias found in ANSI mode: " + serializeAST(*ast), ErrorCodes::SYNTAX_ERROR);

    stack.push_back(ast);

    for (auto & child: ast->children)
        visit(child);

    stack.pop_back();
}

bool CheckAliasVisitor::isSelectItem()
{
    if (stack.size() > 1)
        if (auto * select_query = stack[stack.size() - 2]->as<ASTSelectQuery>())
            return select_query->select() == stack.back();

    return false;
}

bool CheckAliasVisitor::isTableExpressionElement()
{
    return !stack.empty() && stack.back()->as<ASTTableExpression>();
}

}
