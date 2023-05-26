#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserUseQuery.h>
#include "Parsers/formatTenantDatabaseName.h"

namespace DB
{

bool ParserUseQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_use("USE");
    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier name_p;

    if (!s_use.ignore(pos, expected))
        return false;

    ASTPtr database;
    ASTPtr catalog;
    if (!name_p.parse(pos, database, expected))
        return false;
    if (s_dot.ignore(pos, expected))
    {
        catalog = database;
        if (!name_p.parse(pos, database, expected))
            return false;
    }
    else if (auto current_catalog = getCurrentCatalog(); !current_catalog.empty())
    {
        catalog = std::make_shared<ASTIdentifier>(current_catalog);
    }

    tryAppendCatalogName(catalog, database);
    tryRewriteCnchDatabaseName(database, pos.getContext());

    auto query = std::make_shared<ASTUseQuery>();
    tryGetIdentifierNameInto(database, query->database);
    node = query;

    return true;
}

}
