#include <Parsers/ParserRefreshQuery.h>
#include <Parsers/ASTRefreshQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/ParserPartition.h>


namespace DB
{
    bool ParserRefreshQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
    {
        auto query = std::make_shared<ASTRefreshQuery>();
        node = query;

        ParserKeyword s_refresh_view("REFRESH MATERIALIZED VIEW");
        ParserKeyword s_partition("PARTITION");
        ParserKeyword s_sync("SYNC");
        ParserKeyword s_async("ASYNC");
        ParserPartition parser_partition(dt);

        if (!s_refresh_view.ignore(pos, expected))
            return false;

        if (!parseDatabaseAndTableName(pos, expected, query->database, query->table))
            return false;

        if (s_partition.ignore(pos, expected))
        {
            if (!parser_partition.parse(pos, query->partition, expected))
                return false;
        }

        if (s_sync.ignore(pos, expected))
            query->async = false;
        else if (s_async.ignore(pos, expected))
            query->async = true;

        return true;
    }
}
