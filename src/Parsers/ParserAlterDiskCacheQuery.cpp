#include <Parsers/ParserAlterDiskCacheQuery.h>

#include <Parsers/ASTAlterDiskCacheQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserPartition.h>

namespace DB
{

bool ParserAlterDiskCacheQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_alter_disk_cache("ALTER DISK CACHE");
    ParserKeyword s_preload("PRELOAD TABLE");
    ParserKeyword s_drop("DROP TABLE");
    ParserKeyword s_partition("PARTITION");
    ParserKeyword s_sync("SYNC");
    ParserKeyword s_async("ASYNC");
    ParserKeyword s_settings("SETTINGS");
    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier p_identifier;
    ParserPartition p_partition;
    ParserSetQuery settings_p(true);

    ASTPtr table;
    ASTPtr database;
    ASTAlterDiskCacheQuery::Type query_type;

    if (!s_alter_disk_cache.ignore(pos, expected))
        return false;

    if (s_preload.ignore(pos, expected))
    {
        query_type = ASTAlterDiskCacheQuery::Type::PRELOAD;
    }
    else if (s_drop.ignore(pos, expected))
    {
        query_type = ASTAlterDiskCacheQuery::Type::DROP;
    }
    else
        return false;

    if (!p_identifier.parse(pos, database, expected))
        return false;

    auto query = std::make_shared<ASTAlterDiskCacheQuery>();
    query->type = query_type;
    if (s_dot.ignore(pos))
    {
        if (!p_identifier.parse(pos, table, expected))
            return false;

        tryGetIdentifierNameInto(database, query->database);
        tryGetIdentifierNameInto(table, query->table);
    }
    else
    {
        table = database;
        tryGetIdentifierNameInto(table, query->table);
    }

    if (s_partition.ignore(pos, expected))
    {
        if (!p_partition.parse(pos, query->partition, expected))
            return false;
    }

    if (s_sync.ignore(pos, expected))
        query->sync = true;
    else if (s_async.ignore(pos, expected))
        query->sync = false;
    else
        query->sync = true; // default sync

    if (s_settings.ignore(pos, expected))
    {
        if (!settings_p.parse(pos, query->settings_ast, expected))
            return false;
    }

    node = query;
    return true;
}

}
