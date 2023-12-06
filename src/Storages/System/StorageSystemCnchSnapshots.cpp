#include <Catalog/Catalog.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <Protos/RPCHelpers.h>
#include <Storages/System/StorageSystemCnchSnapshots.h>
#include <Transaction/TxnTimestamp.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

NamesAndTypesList StorageSystemCnchSnapshots::getNamesAndTypes()
{
    return {
        {"database", std::make_shared<DataTypeString>()},
        {"name", std::make_shared<DataTypeString>()},
        {"table_uuid", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUUID>())},
        {"ttl_in_days", std::make_shared<DataTypeUInt32>()},
        {"version", std::make_shared<DataTypeUInt64>()},
        {"creation_time", std::make_shared<DataTypeDateTime>()},
    };
}

NamesAndAliases StorageSystemCnchSnapshots::getNamesAndAliases()
{
    return
    {
        {"expire_time", {std::make_shared<DataTypeDateTime>()}, "creation_time + interval ttl_in_days day"},
    };
}

void StorageSystemCnchSnapshots::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & ) const
{
    auto cnch_catalog = context->tryGetCnchCatalog();
    if (context->getServerType() != ServerType::cnch_server || !cnch_catalog)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "system.cnch_snapshots can only be accessed from cnch server");

    std::vector<std::pair<String, UUID>> dbname_and_uuid;
    for (const auto & db : cnch_catalog->getAllDataBases())
    {
        if (!db.has_uuid()) continue;
        dbname_and_uuid.emplace_back(db.name(), RPCHelpers::createUUID(db.uuid()));
    }

    for (const auto & [db_name, db_uuid] : dbname_and_uuid)
    {
        for (const auto & snapshot : cnch_catalog->getAllSnapshots(db_uuid))
        {
            size_t col_num = 0;
            res_columns[col_num++]->insert(db_name);
            res_columns[col_num++]->insert(snapshot->name());
            if (snapshot->has_table_uuid())
                res_columns[col_num++]->insert(RPCHelpers::createUUID(snapshot->table_uuid()));
            else
                res_columns[col_num++]->insertDefault();
            res_columns[col_num++]->insert(snapshot->ttl_in_days());
            res_columns[col_num++]->insert(snapshot->commit_time());
            res_columns[col_num++]->insert(TxnTimestamp(snapshot->commit_time()).toSecond());
        }
    }
}

}
