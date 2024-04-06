#include <Databases/IDatabase.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <Interpreters/Context.h>
#include <Access/ContextAccess.h>
#include <Storages/System/StorageSystemDatabases.h>
#include <Parsers/formatTenantDatabaseName.h>


namespace DB
{

NamesAndTypesList StorageSystemDatabases::getNamesAndTypes()
{
    return {
        {"name", std::make_shared<DataTypeString>()},
        {"engine", std::make_shared<DataTypeString>()},
        {"data_path", std::make_shared<DataTypeString>()},
        {"metadata_path", std::make_shared<DataTypeString>()},
        {"uuid", std::make_shared<DataTypeUUID>()},
    };
}

void StorageSystemDatabases::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_DATABASES);

    const String tenant_id = context->getTenantId();
    const auto databases = DatabaseCatalog::instance().getDatabases(context);
    for (const auto & [database_name, database] : databases)
    {
        String database_strip_tenantid = database_name;
        if (!tenant_id.empty())
        {
            if (startsWith(database_name, tenant_id + "."))
                database_strip_tenantid = getOriginalDatabaseName(database_name, tenant_id);
            // Will skip database of other tenants and default user (without tenantid prefix)
            else if (database_name.find(".") != std::string::npos || !DatabaseCatalog::isDefaultVisibleSystemDatabase(database_name))
                continue;
        }

        if (check_access_for_databases && !access->isGranted(AccessType::SHOW_DATABASES, database_name))
            continue;

        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
            continue; /// We don't want to show the internal database for temporary tables in system.databases

        res_columns[0]->insert(database_strip_tenantid);
        res_columns[1]->insert(database->getEngineName());
        res_columns[2]->insert(context->getPath() + database->getDataPath());
        res_columns[3]->insert(database->getMetadataPath());
        res_columns[4]->insert(database->getUUID());
   }
}

}
