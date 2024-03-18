#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTPreparedStatement.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserPreparedStatement.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserSetQuery.h>

namespace DB
{
bool ParserCreatePreparedStatementQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create("CREATE");
    ParserKeyword s_permanent("PERMANENT");
    ParserKeyword s_prepared_statement("PREPARED STATEMENT");
    ParserKeyword s_if_not_exists("IF NOT EXISTS");
    ParserKeyword s_or_replace("OR REPLACE");
    ParserKeyword s_on("ON");
    ParserKeyword s_as("AS");

    bool if_not_exists = false;
    bool or_replace = false;
    bool is_permanent = false;
    if (!s_create.ignore(pos, expected))
        return false;

    if (s_permanent.ignore(pos, expected))
        is_permanent = true;

    if (!s_prepared_statement.ignore(pos, expected))
        return false;

    if (s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;
    else if (s_or_replace.ignore(pos, expected))
        or_replace = true;

    ParserCompoundIdentifier name_p;
    ASTPtr identifier;
    if (!name_p.parse(pos, identifier, expected))
        return false;

    String cluster_str;
    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }


    if (!s_as.parse(pos, identifier, expected))
        return false;

    ASTPtr query;
    ParserSelectWithUnionQuery select_p(dt);
    if (!select_p.parse(pos, query, expected))
        return false;

    auto prepare = std::make_shared<ASTCreatePreparedStatementQuery>();

    tryGetIdentifierNameInto(identifier, prepare->name);
    prepare->cluster = std::move(cluster_str);
    prepare->is_permanent = is_permanent;
    prepare->if_not_exists = if_not_exists;
    prepare->or_replace = or_replace;
    prepare->query = query;
    prepare->children.push_back(prepare->query);

    node = prepare;
    return true;
}

bool ParserExecutePreparedStatementQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_execute("EXECUTE PREPARED STATEMENT");
    ParserKeyword s_using("USING");

    if (!s_execute.ignore(pos, expected))
        return false;

    ParserCompoundIdentifier name_p;
    ASTPtr identifier;

    if (!name_p.parse(pos, identifier, expected))
        return false;

    ASTPtr settings;

    if (s_using.ignore(pos, expected))
    {
        ParserSetQuery parser_settings(true);
        if (!parser_settings.parse(pos, settings, expected))
            return false;
    }
    else
    {
        settings = std::make_shared<ASTSetQuery>();
    }

    auto execute = std::make_shared<ASTExecutePreparedStatementQuery>();

    tryGetIdentifierNameInto(identifier, execute->name);

    execute->values = settings;
    node = execute;
    return true;
}

bool ParserShowPreparedStatementQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_show("SHOW");
    ParserKeyword s_create("CREATE");
    ParserKeyword s_explain("EXPLAIN");
    ParserKeyword s_prepared_statement("PREPARED STATEMENT");
    ParserKeyword s_prepared_statements("PREPARED STATEMENTS");
    ParserCompoundIdentifier name_p;
    ASTPtr identifier;

    bool create = false;
    bool explain = false;
    if (s_show.ignore(pos, expected))
    {
        if (s_create.ignore(pos, expected))
        {
            create = true;
            if (!s_prepared_statement.ignore(pos, expected))
                return false;
            if (!name_p.parse(pos, identifier, expected))
                return false;
        }
        else if (s_prepared_statements.ignore(pos, expected))
        {
        }
        else
            return false;
    }
    else if (s_explain.ignore(pos, expected))
    {
        explain = true;
        if (!s_prepared_statement.ignore(pos, expected))
            return false;
        if (!name_p.parse(pos, identifier, expected))
            return false;
    }
    else
        return false;

    auto show_prepare = std::make_shared<ASTShowPreparedStatementQuery>();
    if (identifier)
        tryGetIdentifierNameInto(identifier, show_prepare->name);
    show_prepare->show_create = create;
    show_prepare->show_explain = explain;
    node = show_prepare;
    return true;
}

bool ParserDropPreparedStatementQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_drop("DROP");
    ParserKeyword s_if_exists("IF EXISTS");
    ParserKeyword s_on("ON");
    ParserKeyword s_prepare("PREPARED STATEMENT");

    if (!s_drop.ignore(pos, expected) || !s_prepare.ignore(pos, expected))
        return false;

    bool if_exists = false;
    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    ParserCompoundIdentifier name_p;
    ASTPtr identifier;
    if (!name_p.parse(pos, identifier, expected))
        return false;

    String cluster_str;
    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    auto drop = std::make_shared<ASTDropPreparedStatementQuery>();
    tryGetIdentifierNameInto(identifier, drop->name);
    drop->cluster = std::move(cluster_str);
    drop->if_exists = if_exists;
    node = drop;
    return true;
}

}
