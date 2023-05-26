#include <Parsers/ParserSwitchQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTSwitchQuery.h>


namespace DB
{

bool ParserSwitchQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_switch("Switch");
    ParserIdentifier name_p;

    if (!s_switch.ignore(pos, expected))
        return false;

    ASTPtr catalog;
    if (!name_p.parse(pos, catalog, expected))
        return false;

    auto query = std::make_shared<ASTSwitchQuery>();
    tryGetIdentifierNameInto(catalog, query->catalog);
    node = query;

    return true;
}

}
