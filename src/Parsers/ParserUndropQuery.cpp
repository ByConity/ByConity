#include <Parsers/ParserUndropQuery.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTUndropQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <IO/ReadHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int LOGICAL_ERROR;
}

bool ParserUndropQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_undrop("UNDROP");
    ParserKeyword s_database("DATABASE");
    ParserKeyword s_table("TABLE");
    ParserKeyword s_uuid("WITH UUID");
    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier name_p;

    if (!s_undrop.ignore(pos, expected))
        return false;

    ASTPtr database;
    ASTPtr table;
    ASTPtr uuid;

    if (s_database.ignore(pos, expected))
    {
        if (!name_p.parse(pos, database, expected))
            return false;
    }
    else if (s_table.ignore(pos, expected))
    {
        if (!name_p.parse(pos, table, expected))
            return false;

        if (s_dot.ignore(pos, expected))
        {
            database = table;
            if (!name_p.parse(pos, table, expected))
                return false;
        }
    }
    else
    {
        return false;
    }

    if (s_uuid.ignore(pos, expected))
    {
        if (!name_p.parse(pos, uuid, expected))
            return false;
    }

    auto query = std::make_shared<ASTUndropQuery>();
    node = query;
    tryGetIdentifierNameInto(database, query->database);
    tryGetIdentifierNameInto(table, query->table);
    if (uuid)
        query->uuid = stringToUUID(getIdentifierName(uuid));

    return true;
}

}
