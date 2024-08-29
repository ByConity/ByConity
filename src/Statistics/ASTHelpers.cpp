#include <Statistics/ASTHelpers.h>

#include <Access/ContextAccess.h>
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/StatsTableIdentifier.h>
#include <Storages/StorageMaterializedView.h>

namespace DB::Statistics
{
std::vector<StatsTableIdentifier> getTablesFromScope(ContextPtr context, const StatisticsScope & scope)
{
    std::vector<StatsTableIdentifier> tables;
    auto catalog = createCatalogAdaptor(context);

    if (!scope.database)
    {
        const auto access = context->getAccess();
        const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_DATABASES);
        const String tenant_id = context->getTenantId();
        for (const auto & [database_name, db] : DatabaseCatalog::instance().getDatabases(context))
        {
            String database_strip_tenantid = database_name;
            if (!tenant_id.empty())
            {
                if (startsWith(database_name, tenant_id + "."))
                    database_strip_tenantid = getOriginalDatabaseName(database_name, tenant_id);
                // Will skip database of other tenants and default user (without tenantid prefix)
                else if (database_name.find('.') != std::string::npos || !DatabaseCatalog::isDefaultVisibleSystemDatabase(database_name))
                    continue;
            }

            if (check_access_for_databases && !access->isGranted(AccessType::SHOW_DATABASES, database_name))
                continue;

            if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
                continue; /// We don't want to show the internal database for temporary tables in system.databases

            auto new_tables = catalog->getAllTablesID(database_name);
            tables.insert(tables.end(), new_tables.begin(), new_tables.end());
        }
    }
    else
    {
        auto db = context->resolveDatabase(scope.database.value());
        if (!scope.table)
        {
            tables = catalog->getAllTablesID(db);
        }
        else
        {
            auto table = scope.table.value();
            auto table_info_opt = catalog->getTableIdByName(db, table);
            if (!table_info_opt)
            {
                auto msg = "Unknown Table (" + table + ") in database (" + db + ")";
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
                LOG_WARNING(getLogger("ShowStats"), err_msg);
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
