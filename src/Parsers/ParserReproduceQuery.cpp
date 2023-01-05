#include <Parsers/ASTReproduceQuery.h>
#include <Parsers/ParserReproduceQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
namespace DB
{
bool ParserReproduceQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_reproduce("REPRODUCE");
    if (!s_reproduce.ignore(pos, expected))
    {
        return false;
    }
    auto query = std::make_shared<ASTReproduceQuery>();
    const char * begin = pos->begin;
    query->reproduce_path = String(begin);
    while (!pos->isEnd())
        ++pos;
    node = query;
    return true;
}

}
