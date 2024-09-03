#pragma once

#include <Parsers/ASTAutoStatsQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTStatsQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/parseDatabaseAndTableName.h>

namespace DB
{

class ParserAutoStatsQuery : public IParserBase
{
public:
    [[nodiscard]] const char * getName() const override
    {
        return "Auto Stats Query";
    }
    using QueryPrefix = ASTAutoStatsQuery::QueryPrefix;

private:
    static bool parsePrefix(Pos & pos, QueryPrefix & query_prefix, Expected & expected)
    {
        const std::vector<std::pair<QueryPrefix, const char *>> keywords = {
            {QueryPrefix::Alter, "ALTER"},
            {QueryPrefix::Create, "CREATE"},
            {QueryPrefix::Drop, "DROP"},
            {QueryPrefix::Show, "SHOW"},
        };

        for (const auto & [prefix, str] : keywords)
        {
            ParserKeyword s_key(str);
            if (s_key.ignore(pos, expected))
            {
                query_prefix = prefix;
                return true;
            }
        }
        return false;
    }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        ParserKeyword s_all_databases("ALL_DATABASES");
        ParserKeyword s_auto_stats("AUTO STATS");
        ParserKeyword s_with("WITH");
        ParserToken s_comma(TokenType::Comma);

        auto query = std::make_shared<ASTAutoStatsQuery>();

        QueryPrefix query_prefix{};
        if (!parsePrefix(pos, query_prefix, expected))
            return false;

        query->prefix = query_prefix;

        if (!s_auto_stats.ignore(pos, expected))
            return false;

        if (query_prefix != QueryPrefix::Alter)
        {
            bool all_db = s_all_databases.ignore(pos, expected);
            if (!all_db)
            {
                if (!parseDatabaseAndTableNameOrAsterisks(
                        pos, expected, query->database, query->any_database, query->table, query->any_table))
                    return false;
            }
            else
            {
                query->any_database = query->any_table = true;
            }
        }

        if (query_prefix == QueryPrefix::Drop || query_prefix == QueryPrefix::Show)
        {
            // DROP and SHOW don't support WITH clause
        }
        else if (s_with.ignore(pos, expected))
        {
            // for create and alter
            SettingsChanges changes;
            while (true)
            {
                if (!changes.empty() && !s_comma.ignore(pos))
                    break;
                changes.push_back(SettingChange{});
                if (!ParserSetQuery::parseNameValuePair(changes.back(), pos, expected))
                    return false;
            }
            query->settings_changes_opt = changes;
        }
        else if (query_prefix == QueryPrefix::Alter)
        {
            // ALTER must have WITH caluse
            return false;
        }

        node = query;
        return true;
    }
};

}
