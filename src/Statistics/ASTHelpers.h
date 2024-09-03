#pragma once
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTStatsQuery.h>
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/StatsTableIdentifier.h>
#include <Storages/StorageMaterializedView.h>

namespace DB::Statistics
{

// use any_database, any_table, database, table in query
// to construct visited tables
template <typename QueryType>
inline auto getTablesFromAST(ContextPtr context, const QueryType * query)
{
    std::vector<StatsTableIdentifier> tables;
    auto catalog = createCatalogAdaptor(context);
    if (query->any_database)
    {
        for (const auto & db : DatabaseCatalog::instance().getDatabases(context))
        {
            auto new_tables = catalog->getAllTablesID(db.first);
            tables.insert(tables.end(), new_tables.begin(), new_tables.end());
        }
    }
    else
    {
        auto db = context->resolveDatabase(query->database);
        if (query->any_table)
        {
            tables = catalog->getAllTablesID(db);
        }
        else
        {
            auto table_info_opt = catalog->getTableIdByName(db, query->table);
            if (!table_info_opt)
            {
                auto msg = "Unknown Table (" + query->table + ") in database (" + db + ")";
                throw Exception(msg, ErrorCodes::UNKNOWN_TABLE);
            }
            tables.emplace_back(table_info_opt.value());
        }
    }

    // ensure table is unique
    std::unordered_set<StatsTableIdentifier> table_set;
    std::vector<StatsTableIdentifier> result;
    // show materialized view as target table
    for (auto table : tables)
    {
        auto storage = catalog->getStorageByTableId(table);
        if (const auto * mv = dynamic_cast<const StorageMaterializedView *>(storage.get()))
        {
            auto table_opt = catalog->getTableIdByName(mv->getTargetDatabaseName(), mv->getTargetTableName());
            if (!table_opt.has_value())
            {
                auto err_msg = fmt::format(
                    FMT_STRING("mv {}.{} has invalid target table {}.{}"),
                    mv->getDatabaseName(),
                    mv->getTableName(),
                    mv->getTargetDatabaseName(),
                    mv->getTargetTableName());
                LOG_WARNING(&Poco::Logger::get("ShowStats"), err_msg);
                continue;
            }
            table = table_opt.value();
        }
        if (table_set.count(table))
        {
            continue;
        }
        table_set.insert(table);
        result.emplace_back(table);
    }
    return result;
}
}
