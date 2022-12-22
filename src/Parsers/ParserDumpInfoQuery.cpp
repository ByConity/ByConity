#include <Parsers/ASTDumpInfoQuery.h>
#include <Parsers/ParserDumpInfoQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>
namespace DB
{
bool ParserDumpInfoQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_dump("DUMP");
    if (!s_dump.ignore(pos, expected))
    {
        return false;
    }
    auto query = std::make_shared<ASTDumpInfoQuery>();
    ParserSelectWithUnionQuery select_p(dt);
    ASTPtr sub_query;
    if(select_p.parse(pos, sub_query, expected))
    {
        WriteBufferFromOwnString buf;
        formatAST(*sub_query, buf, false, false);
        query->dump_string = buf.str();
        query->children.emplace_back(sub_query);
        query->dump_query = std::move(sub_query);
    }
    else
        return false;
    node = std::move(query);
    return true;
}

}
