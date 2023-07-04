#include <Parsers/ParserUpdateQuery.h>
#include <Parsers/ASTUpdateQuery.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserSetQuery.h>


namespace DB
{

bool ParserUpdateQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto query = std::make_shared<ASTUpdateQuery>();
    node = query;

    ParserKeyword s_update("UPDATE");
    ParserKeyword s_set("SET");
    ParserKeyword s_where("WHERE");
    ParserKeyword s_order_by("ORDER BY");
    ParserKeyword s_limit("LIMIT");
    ParserKeyword s_settings("SETTINGS");

    ParserList parser_assignment_list(std::make_unique<ParserAssignment>(dt), std::make_unique<ParserToken>(TokenType::Comma), false);
    ParserExpression exp_list(dt);
    ParserOrderByExpressionList order_list(dt);
    ParserExpressionWithOptionalAlias exp_elem(false, dt);

    if (!s_update.ignore(pos, expected))
        return false;

    if (!parseDatabaseAndTableName(pos, expected, query->database, query->table))
        return false;

    if (!s_set.ignore(pos, expected))
        return false;

    if (!parser_assignment_list.parse(pos, query->assignment_list, expected))
        return false;

    if (!s_where.ignore(pos, expected))
        return false;

    if (!exp_list.parse(pos, query->where_condition, expected))
        return false;

    if (s_order_by.ignore(pos, expected))
    {
        if (!order_list.parse(pos, query->order_by_expr, expected))
            return false;
    }

    // LIMIT length | LIMIT offset, length
    if (s_limit.ignore(pos, expected))
    {
        ParserToken s_comma(TokenType::Comma);
        ASTPtr limit_length;

        if (!exp_elem.parse(pos, limit_length, expected))
            return false;
        
        if (s_comma.ignore(pos, expected))
        {
            query->limit_offset = limit_length;
            if (!exp_elem.parse(pos, query->limit_value, expected))
                return false;
        }
        else
            query->limit_value = std::move(limit_length);
    }

    if (s_settings.ignore(pos, expected))
    {
        ParserSetQuery parser_settings(true);

        if (!parser_settings.parse(pos, query->settings_ast, expected))
            return false;
    }

    return true;
}


}
