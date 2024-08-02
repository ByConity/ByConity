#include <Parsers/ParserUpdateQuery.h>
#include <Parsers/ASTUpdateQuery.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/formatAST.h>
#include <Poco/Logger.h>
#include "Parsers/ASTSerDerHelper.h"


namespace DB
{

/// To get more info of the grammar, see ASTUpdateQuery.h .
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

    ParserList parser_assignment_list(std::make_unique<ParserAssignmentWithAlias>(dt), std::make_unique<ParserToken>(TokenType::Comma), false);
    ParserExpression exp_list(dt);
    ParserOrderByExpressionList order_list(dt);
    ParserExpressionWithOptionalAlias exp_elem(false, dt);

    if (!s_update.ignore(pos, expected))
        return false;

    auto pos_before = pos;
    /// extract the target table
    if (!parseDatabaseAndTableName(pos, expected, query->database, query->table))
        return false;

    if (s_set.ignore(pos, expected))
    {
        /// If the target table is followed by the word 'SET', it means it's a UPDATE SINGLE TABLE query, and all tables are extracted.
        query->single_table = true;
    }
    else
    {
        /// For UPDATE JOIN, we already extract the target table and store the info to query->database and query->table.
        /// To parse the whole `tables` correctly, we need to retrace tokens from the beginning of target table. 
        query->single_table = false;
        pos = pos_before;

        /// extract all tables
        if (!ParserTablesInSelectQuery(dt).parse(pos, query->tables, expected))
            return false;

        if (!s_set.ignore(pos, expected))
            return false;
    }

    if (!parser_assignment_list.parse(pos, query->assignment_list, expected))
        return false;

    if (s_where.ignore(pos, expected))
    {
        if (!exp_list.parse(pos, query->where_condition, expected))
            return false;
    }

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

    if (query->tables)
        query->children.push_back(query->tables);
    query->children.push_back(query->assignment_list);
    if (query->where_condition)
        query->children.push_back(query->where_condition);
    if (query->order_by_expr)
        query->children.push_back(query->order_by_expr);
    if (query->limit_value)
        query->children.push_back(query->limit_value);
    if (query->limit_offset)
        query->children.push_back(query->limit_offset);
    if (query->settings_ast)
        query->children.push_back(query->settings_ast);

    return true;
}


}
