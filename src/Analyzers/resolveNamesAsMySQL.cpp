#include <Analyzers/resolveNamesAsMySQL.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int MULTIPLE_EXPRESSIONS_FOR_ALIAS;
}

namespace
{
    // expressions with defined name
    class NamedExpressions
    {
    public:
        void insert(String name, ASTPtr expression)
        {
            index_by_name.emplace(std::move(name), std::move(expression));
        }
        bool rewriteName(const String & name, ASTPtr & ast, const ASTPtr & root_expression);

    private:
        std::unordered_multimap<String, ASTPtr> index_by_name;
    };

    bool NamedExpressions::rewriteName(const String & name, ASTPtr & ast, const ASTPtr & root_expression)
    {
        auto range = index_by_name.equal_range(name);

        if (range.first == range.second)
            return false;

        if (std::next(range.first, 1) != range.second)
            throw Exception(
                ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS,
                "{} is ambiguous in expression {}",
                backQuoteIfNeed(name),
                root_expression->formatForErrorMessage());

        ast = range.first->second->clone();
        return true;
    }

    // a prioritized stack of named expression
    class NamedExpressionsStack
    {
    public:
        explicit NamedExpressionsStack(std::vector<NamedExpressions> levels_) : levels(std::move(levels_))
        {
        }
        void rewriteName(ASTPtr & ast, const ASTPtr & root_expression);
        void rewriteChildren(ASTPtr & ast, const ASTPtr & root_expression)
        {
            for (auto & child : ast->children)
                rewriteName(child, root_expression);
        }

    private:
        std::vector<NamedExpressions> levels;
    };

    void NamedExpressionsStack::rewriteName(ASTPtr & ast, const ASTPtr & root_expression)
    {
        if (auto * iden = ast->as<ASTIdentifier>())
        {
            if (iden->isShort())
            {
                auto name = iden->shortName();

                for (auto it = levels.rbegin(); it != levels.rend(); ++it)
                    if (it->rewriteName(name, ast, root_expression))
                        break;
            }
        }
        else if (const auto * func = ast->as<ASTFunction>(); func
                 && !AggregateUtils::isAggregateFunction(*func) /* prefer source column under aggregate function */
                 && func->name != "lambda")
            rewriteChildren(ast, root_expression);
        else if (ast->as<ASTExpressionList>())
            rewriteChildren(ast, root_expression);
    }

    void collectNamedExpressions(const ASTPtr & expression, NamedExpressions & named_expressions)
    {
        if (const auto * expr_list = expression->as<ASTExpressionList>())
        {
            for (const auto & child : expr_list->children)
                collectNamedExpressions(child, named_expressions);
            return;
        }

        if (auto alias = expression->tryGetAlias(); !alias.empty())
            named_expressions.insert(alias, expression);
        else if (const auto * iden = expression->as<ASTIdentifier>())
            named_expressions.insert(iden->shortName(), expression);
    }
}

void resolveNamesInHavingAsMySQL(ASTSelectQuery & select_query)
{
    NamedExpressions group_by_named_expressions;
    NamedExpressions select_named_expressions;

    if (auto group_by = select_query.groupBy())
        collectNamedExpressions(group_by, group_by_named_expressions);

    if (auto select = select_query.select())
        collectNamedExpressions(select, select_named_expressions);

    // names in GROUP BY has higher priority
    NamedExpressionsStack named_expressions_stack{{select_named_expressions, group_by_named_expressions}};
    named_expressions_stack.rewriteName(select_query.refHaving(), select_query.refHaving());
}

}
