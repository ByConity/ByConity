#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTDropQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ParserDropQuery.h>
#include <Parsers/ParserPartition.h>


namespace DB
{

namespace
{

bool parseDropQuery(IParser::Pos & pos, ASTPtr & node, Expected & expected, const ASTDropQuery::Kind kind, const ParserSettingsImpl & dt)
{
    ParserKeyword s_temporary("TEMPORARY");
    ParserKeyword s_table("TABLE");
    ParserKeyword s_dictionary("DICTIONARY");
    ParserKeyword s_snapshot("SNAPSHOT");
    ParserKeyword s_view("VIEW");
    ParserKeyword s_catalog("EXTERNAL CATALOG");
    ParserKeyword s_database("DATABASE");
    ParserToken s_dot(TokenType::Dot);
    ParserKeyword s_if_exists("IF EXISTS");
    ParserIdentifier name_p;
    ParserKeyword s_permanently("PERMANENTLY");
    ParserKeyword s_no_delay("NO DELAY");
    ParserKeyword s_sync("SYNC");
    ParserKeyword s_schema("SCHEMA");

    ParserKeyword s_partition_where("PARTITION WHERE");
    ParserExpression parser_partition_predicate(dt);
    ParserKeyword s_partition("PARTITION");

    ASTPtr catalog;
    ASTPtr database;
    ASTPtr table;
    String cluster_str;
    bool if_exists = false;
    bool temporary = false;
    bool is_dictionary = false;
    bool is_snapshot = false;
    bool is_view = false;
    bool no_delay = false;
    bool permanently = false;

    if (s_catalog.ignore(pos, expected))
    {
        if (s_if_exists.ignore(pos, expected))
            if_exists = true;

        if (!name_p.parse(pos, catalog, expected))
            return false;
        tryRewriteHiveCatalogName(catalog, pos.getContext());
    }
    else if (s_database.ignore(pos, expected) || s_schema.ignore(pos, expected))
    {
        if (s_if_exists.ignore(pos, expected))
            if_exists = true;

        if (!name_p.parse(pos, database, expected))
            return false;
        tryRewriteCnchDatabaseName(database, pos.getContext());
    }
    else
    {
        if (s_view.ignore(pos, expected))
            is_view = true;
        else if (s_dictionary.ignore(pos, expected))
            is_dictionary = true;
        else if (s_snapshot.ignore(pos, expected))
            is_snapshot = true;
        else if (s_temporary.ignore(pos, expected))
            temporary = true;

        /// for TRUNCATE queries TABLE keyword is assumed as default and can be skipped
        if (!is_view && !is_dictionary && !is_snapshot && (!s_table.ignore(pos, expected) && kind != ASTDropQuery::Kind::Truncate))
        {
            return false;
        }

        if (s_if_exists.ignore(pos, expected))
            if_exists = true;

        if (!name_p.parse(pos, table, expected))
            return false;

        if (s_dot.ignore(pos, expected))
        {
            database = table;
            tryRewriteCnchDatabaseName(database, pos.getContext());
            if (!name_p.parse(pos, table, expected))
                return false;
        }
    }

    /// common for tables / dictionaries / databases
    if (!is_snapshot && ParserKeyword{"ON"}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    if (kind == ASTDropQuery::Kind::Detach && s_permanently.ignore(pos, expected))
        permanently = true;

    /// actually for TRUNCATE NO DELAY / SYNC means nothing
    if (s_no_delay.ignore(pos, expected) || s_sync.ignore(pos, expected))
        no_delay = true;

    auto query = std::make_shared<ASTDropQuery>();
    node = query;

    if (s_partition_where.ignore(pos, expected))
    {
        if (!parser_partition_predicate.parse(pos, query->partition_predicate, expected))
            return false;
    }
    else if (s_partition.ignore(pos, expected))
    {
        if (!ParserList{std::make_unique<ParserLiteral>(), std::make_unique<ParserToken>(TokenType::Comma), false}.parse(
                pos, query->partition, expected))
            return false;
    }

    query->kind = kind;
    query->if_exists = if_exists;
    query->temporary = temporary;
    query->is_dictionary = is_dictionary;
    query->is_snapshot = is_snapshot;
    query->is_view = is_view;
    query->no_delay = no_delay;
    query->permanently = permanently;

    tryGetIdentifierNameInto(catalog, query->catalog);
    tryGetIdentifierNameInto(database, query->database);
    tryGetIdentifierNameInto(table, query->table);

    query->cluster = cluster_str;

    return true;
}

}

bool ParserDropQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_drop("DROP");
    ParserKeyword s_detach("DETACH");
    ParserKeyword s_truncate("TRUNCATE");

    if (s_drop.ignore(pos, expected))
        return parseDropQuery(pos, node, expected, ASTDropQuery::Kind::Drop, dt);
    else if (s_detach.ignore(pos, expected))
        return parseDropQuery(pos, node, expected, ASTDropQuery::Kind::Detach, dt);
    else if (s_truncate.ignore(pos, expected))
        return parseDropQuery(pos, node, expected, ASTDropQuery::Kind::Truncate, dt);
    else
        return false;
}

}
