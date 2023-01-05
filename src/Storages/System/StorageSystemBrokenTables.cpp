#include <Storages/System/StorageSystemBrokenTables.h>
#include <Interpreters/Context.h>
#include <Access/ContextAccess.h>
#include <DataTypes/DataTypeString.h>

namespace DB
{


NamesAndTypesList StorageSystemBrokenTables::getNamesAndTypes()
{
    return {
        {"database", std::make_shared<DataTypeString>()},
        {"name", std::make_shared<DataTypeString>()},
        {"error", std::make_shared<DataTypeString>()},
    };
}

void StorageSystemBrokenTables::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_DATABASES);

    const auto databases = DatabaseCatalog::instance().getDatabases();

    for (const auto & [database_name, database] : databases)
    {
        if (check_access_for_databases && !access->isGranted(AccessType::SHOW_DATABASES, database_name))
            continue;
        
        auto tables = database->getBrokenTables();
        for (const auto & [broken_table, error_msg] : tables)
        {
            res_columns[0]->insert(database_name);
            res_columns[1]->insert(broken_table);
            res_columns[2]->insert(error_msg);
        }
    }
}

}
