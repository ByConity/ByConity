#include <Parsers/ParserCreateWorkerGroupQuery.h>

#include <Parsers/ASTCreateWorkerGroupQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserSetQuery.h>

namespace DB
{
bool ParserCreateWorkerGroupQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create("CREATE");
    ParserKeyword s_worker_group("WORKER GROUP");
    ParserKeyword s_if_not_exists("IF NOT EXISTS");
    ParserKeyword s_in("IN");
    ParserKeyword s_settings("SETTINGS");
    ParserSetQuery settings_p(/* parse_only_internals_ = */ true);

    if (!s_create.ignore(pos, expected))
        return false;

    if (!s_worker_group.ignore(pos, expected))
        return false;

    bool if_not_exists = false;
    if (s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    ASTPtr worker_group_id_ast;
    if (!ParserIdentifier{}.parse(pos, worker_group_id_ast, expected))
        return false;

    ASTPtr vw_name_ast;
    if (s_in.ignore(pos, expected))
    {
        if (!ParserIdentifier{}.parse(pos, vw_name_ast, expected))
            return false;
    }

    ASTPtr settings;
    if (s_settings.ignore(pos, expected))
    {
        if (!settings_p.parse(pos, settings, expected))
            return false;
    }

    /// construct ast
    auto query = std::make_shared<ASTCreateWorkerGroupQuery>();
    query->if_not_exists = if_not_exists;
    query->worker_group_id = getIdentifierName(worker_group_id_ast);
    if (vw_name_ast)
        query->vw_name = getIdentifierName(vw_name_ast);
    query->set(query->settings, settings);

    node = query;
    return true;
}

}
