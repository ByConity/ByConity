#include <Parsers/ParserDropWarehouseQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTDropWarehouseQuery.h>

namespace DB
{

bool ParserDropWarehouseQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_if_exists("IF EXISTS");

    if (!ParserKeyword{"DROP WAREHOUSE"}.ignore(pos, expected))
        return false;

    bool if_exists = false;
    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    ASTPtr group_name_ast;
    if (!ParserIdentifier{}.parse(pos, group_name_ast, expected))
        return false;
    String group_name = getIdentifierName(group_name_ast);


    auto query = std::make_shared<ASTDropWarehouseQuery>();
    query->name = std::move(group_name);
    query->if_exists = if_exists;
    node = query;
    return true;
}

}
