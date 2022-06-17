#include <Parsers/ASTDumpInfoQuery.h>
#include <Parsers/ParserDumpInfoQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
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
    ParserQuery parser(end, dt);
    String error_message = "";
    const char * begin = pos->begin;
    query->dump_string = String(begin);
    ASTPtr res = tryParseQuery(parser, begin, end, error_message, false, "", false, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
//    ASTPtr res = tryParseQuery(parser, pos, end, error_message, false, query_description, allow_multi_statements, max_query_size, max_parser_depth);

    while (!pos->isEnd())
        ++pos;
    query->dump_query = res;
    if (res)
    {
        query->children.emplace_back(res);
    }
    query->syntax_error = error_message;
    node = query;
    return true;
}

}
