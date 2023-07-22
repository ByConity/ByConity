#include <Parsers/ASTDropFunctionQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserDropFunctionQuery.h>

#include <Functions/UserDefined/ReservedNames.h>

namespace DB
{

bool ParserDropFunctionQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_drop("DROP");
    ParserKeyword s_local{"LOCAL"};
    ParserKeyword s_function("FUNCTION");
    ParserKeyword s_if_exists("IF EXISTS");
    ParserKeyword s_on("ON");
    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier function_name_p;

    String cluster_str;
    bool if_exists = false;
    bool is_local = false;
    ASTPtr database_name;
    ASTPtr function_name;

    if (!s_drop.ignore(pos, expected))
        return false;

    if (s_local.ignore(pos, expected))
        is_local = true;

    if (!s_function.ignore(pos, expected))
        return false;

    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    if (!function_name_p.parse(pos, function_name, expected))
        return false;

    if (s_dot.ignore(pos, expected))
    {
        database_name = function_name;
        if (!function_name_p.parse(pos, function_name, expected))
            return false;
    }

    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    auto drop_function_query = std::make_shared<ASTDropFunctionQuery>();
    drop_function_query->if_exists = if_exists;
    drop_function_query->cnch_local = is_local;
    drop_function_query->cluster = std::move(cluster_str);

    node = drop_function_query;

    drop_function_query->function_name = function_name->as<ASTIdentifier &>().name();
    if (database_name)
        drop_function_query->database_name = database_name->as<ASTIdentifier &>().name();
    else if (isReservedName(drop_function_query->function_name.c_str()))
        return false;

    return true;
}

}
