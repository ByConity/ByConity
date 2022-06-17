#include <IO/Operators.h>
#include <Parsers/ParserDropWorkerGroupQuery.h>

#include <Parsers/ASTDropWorkerGroupQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>

namespace DB
{
bool ParserDropWorkerGroupQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_drop("DROP");
    ParserKeyword s_worker_group("WORKER GROUP");
    ParserKeyword s_if_exists("IF EXISTS");

    if (!s_drop.ignore(pos, expected))
        return false;

    if (!s_worker_group.ignore(pos, expected))
        return false;

    bool if_exists = false;
    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    ASTPtr worker_group_id_ast;
    if (!ParserIdentifier{}.parse(pos, worker_group_id_ast, expected))
        return false;

    /// construct ast
    auto query = std::make_shared<ASTDropWorkerGroupQuery>();
    query->if_exists = if_exists;
    query->worker_group_id = getIdentifierName(worker_group_id_ast);

    node = query;
    return true;
}

}
