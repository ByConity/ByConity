#include <IO/Operators.h>
#include <Parsers/ParserShowWarehousesQuery.h>
#include <Parsers/ASTShowWarehousesQuery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>

namespace DB
{

bool ParserShowWarehousesQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_like("LIKE");
    if (!ParserKeyword{"SHOW WAREHOUSES"}.ignore(pos, expected))
        return false;
    ParserStringLiteral like_p;
    ASTPtr like;

    if (s_like.ignore(pos, expected))
    {
        if (!like_p.parse(pos, like, expected))
            return false;
    }


    auto query = std::make_shared<ASTShowWarehousesQuery>();
    if (like)
       query->like = safeGet<const String &>(like->as<ASTLiteral &>().value);
    node = query;
    return true;
}

}
