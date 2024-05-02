#include <DataTypes/DataTypeString.h>
#include <Storages/StorageFactory.h>
#include <Storages/System/StorageSystemCnchFilesystemLock.h>
#include <DataTypes/DataTypesNumber.h>
#include <Catalog/Catalog.h>
#include <Interpreters/Context.h>
#include <Storages/System/TenantController.h>


namespace DB
{
NamesAndTypesList StorageSystemCnchFilesystemLock::getNamesAndTypes()
{
    return
        {
            {"directory", std::make_shared<DataTypeString>()},
            {"database", std::make_shared<DataTypeString>()},
            {"table", std::make_shared<DataTypeString>()},
            {"txn_id", std::make_shared<DataTypeUInt64>()}
        };
}

void StorageSystemCnchFilesystemLock::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    DISABLE_VISIT_FOR_TENANTS();
    /// get all filesys lock record from kv
    auto lock_records = context->getCnchCatalog()->getAllFilesysLock();
    std::for_each(lock_records.begin(), lock_records.end(), [&res_columns](auto & record)
    {
        res_columns[0]->insert(record.directory());
        res_columns[1]->insert(record.database());
        res_columns[2]->insert(record.table());
        res_columns[3]->insert(record.txn_id());
    });
}

}
