#pragma once

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTStatsQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/parseDatabaseAndTableName.h>

namespace DB
{

bool parseStatsQueryKind(IParser::Pos & pos, Expected & expected, StatsQueryKind & kind);

/** Query like this:
  * (SHOW | DROP) (STATS | TABLE_STATS | COLUMN_STATS) (ALL | [db_name.]table_name) [AT COLUMN column_name] [ON CLUSTER cluster]
  *
  * or:
  * CREATE (STATS | TABLE_STATS | COLUMN_STATS) (ALL | [db_name.]table_name) [AT COLUMN column_name]
  *                                                                          [ON CLUSTER cluster]
  *                                                                          [PARTITION partition | PARTITION ID 'partition_id']
  *                                                                          [WITH NUM BUCKETS|TOPN|SAMPLES]
  */
template <typename ParserName, typename QueryAstClass, typename QueryInfo>
class ParserStatsQueryBase : public IParserBase
{
public:
    [[nodiscard]] const char * getName() const override { return ParserName::Name; }

protected:
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        ParserKeyword s_query_prefix(QueryInfo::QueryPrefix);
        ParserKeyword s_all("ALL");
        ParserKeyword s_on("ON");
        ParserKeyword s_at_column("AT COLUMN");
        ParserIdentifier p_column_name;

        auto query = std::make_shared<QueryAstClass>();

        if (!s_query_prefix.ignore(pos, expected))
            return false;

        if (!parseStatsQueryKind(pos, expected, query->kind))
            return false;

        if constexpr (std::is_same_v<QueryInfo, CreateStatsQueryInfo>)
        {
            // IF NOT EXISTS is valid only for create
            ParserKeyword s_if_not_exists("IF NOT EXISTS");
            if (s_if_not_exists.ignore(pos, expected))
                query->if_not_exists = true;
        }

        query->target_all = s_all.ignore(pos, expected);

        if (!query->target_all)
        {
            if (!parseDatabaseAndTableName(pos, expected, query->database, query->table))
                return false;
        }

        if (s_at_column.ignore(pos, expected))
        {
            ASTPtr column_name;

            if (!p_column_name.parse(pos, column_name, expected))
                return false;

            query->column = getIdentifierName(column_name);
        }

        if (s_on.ignore(pos, expected))
        {
            if (!ASTQueryWithOnCluster::parse(pos, query->cluster, expected))
                return false;
        }

        if (!parseSuffix(pos, expected, *query))
            return false;

        node = query;
        return true;
    }

    virtual bool parseSuffix(Pos &, Expected &, IAST &) { return true; }
};

struct CreateStatsParserName
{
    static constexpr auto Name = "Create stats query";
};

struct ShowStatsParserName
{
    static constexpr auto Name = "Show stats query";
};

struct DropStatsParserName
{
    static constexpr auto Name = "Drop stats query";
};

using ParserShowStatsQuery = ParserStatsQueryBase<ShowStatsParserName, ASTShowStatsQuery, ShowStatsQueryInfo>;
using ParserDropStatsQuery = ParserStatsQueryBase<DropStatsParserName, ASTDropStatsQuery, DropStatsQueryInfo>;

class ParserCreateStatsQuery : public ParserStatsQueryBase<CreateStatsParserName, ASTCreateStatsQuery, CreateStatsQueryInfo>
{
protected:
    bool parseSuffix(Pos &, Expected &, IAST &) override;
};

}
