#pragma once
#include <optional>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Statistics/StatsTableIdentifier.h>


namespace DB::Statistics
{

// use any_database, any_table, database, table in query
// to construct visited tables

struct StatisticsScope
{
    std::optional<String> database; // nullopt for all
    std::optional<String> table; // nullopt for all
};

std::vector<StatsTableIdentifier> getTablesFromScope(ContextPtr context, const StatisticsScope & scope);

template <typename QueryType>
StatisticsScope scopeFromAST(ContextPtr context, const QueryType * query)
{
    if (query->any_database)
        return StatisticsScope{};
    auto database = context->resolveDatabase(query->database);
    if (query->any_table)
        return StatisticsScope{database, std::nullopt};
    auto table = query->table;
    return StatisticsScope{database, table};
}

template <typename QueryType>
inline auto getTablesFromAST(ContextPtr context, const QueryType * query)
{
    auto scope = scopeFromAST(context, query); 
    return getTablesFromScope(context, scope);
}

}
