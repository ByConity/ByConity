#include <Parsers/ParserAlterDiskCacheQuery.h>

#include <Parsers/ASTAlterDiskCacheQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/parseDatabaseAndTableName.h>

namespace DB
{

bool ParserAlterDiskCacheQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_alter_disk_cache("ALTER DISK CACHE");
    ParserKeyword s_preload("PRELOAD TABLE");
    ParserKeyword s_drop("DROP TABLE");
    ParserKeyword s_manifest("MANIFEST");
    ParserKeyword s_version("VERSION");
    ParserKeyword s_partition("PARTITION");
    ParserKeyword s_sync("SYNC");
    ParserKeyword s_async("ASYNC");
    ParserKeyword s_settings("SETTINGS");
    ParserToken s_dot(TokenType::Dot);
    ParserStringLiteral p_literal;
    ParserPartition p_partition;
    ParserSetQuery settings_p(true);

    ASTAlterDiskCacheQuery::Action action;

    if (!s_alter_disk_cache.ignore(pos, expected))
        return false;

    if (s_preload.ignore(pos, expected))
    {
        action = ASTAlterDiskCacheQuery::Action::PRELOAD;
    }
    else if (s_drop.ignore(pos, expected))
    {
        action = ASTAlterDiskCacheQuery::Action::DROP;
    }
    else
        return false;

    auto query = std::make_shared<ASTAlterDiskCacheQuery>();
    query->action = action;

    if (!parseDatabaseAndTableName(pos, expected, query->database, query->table))
        return false;

    // Parse drop manifest disk cache: `alter disk cache drop db.table manifest [version xxxx]`
    if (action == ASTAlterDiskCacheQuery::Action::DROP && s_manifest.ignore(pos, expected))
    {
        query->type = ASTAlterDiskCacheQuery::Type::MANIFEST;
        if (s_version.ignore(pos, expected))
            p_literal.parse(pos, query->version, expected);
    }
    else if (s_partition.ignore(pos, expected))
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
