#include <Backups/BackupStatus.h>
#include <Catalog/Catalog.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Protos/RPCHelpers.h>
#include <Storages/System/StorageSystemCnchBackups.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

NamesAndTypesList StorageSystemCnchBackups::getNamesAndTypes()
{
    return {
        {"id", std::make_shared<DataTypeString>()},
        {"create_time", std::make_shared<DataTypeDateTime>()},
        {"status", std::make_shared<DataTypeEnum8>(getBackupStatusEnumValues())},
        {"command", std::make_shared<DataTypeString>()},
        {"server_address", std::make_shared<DataTypeString>()},
        {"enable_auto_recover", std::make_shared<DataTypeUInt8>()},
        {"progress", std::make_shared<DataTypeString>()},
        {"finished_tables", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"reschedule_times", std::make_shared<DataTypeUInt64>()},
        {"last_error", std::make_shared<DataTypeString>()},
        {"total_backup_size", std::make_shared<DataTypeUInt64>()},
        {"end_time", std::make_shared<DataTypeDateTime>()},
    };
}

NamesAndAliases StorageSystemCnchBackups::getNamesAndAliases()
{
    return {};
}

void StorageSystemCnchBackups::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    auto cnch_catalog = context->tryGetCnchCatalog();
    if (context->getServerType() != ServerType::cnch_server || !cnch_catalog)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "system.cnch_Backups can only be accessed from cnch server");

    for (const auto & backup_task : cnch_catalog->getAllBackupJobs())
    {
        size_t col_num = 0;
        res_columns[col_num++]->insert(backup_task->id());
        res_columns[col_num++]->insert(TxnTimestamp(backup_task->create_time()).toSecond());
        res_columns[col_num++]->insert(backup_task->status());
        res_columns[col_num++]->insert(backup_task->serialized_ast());
        res_columns[col_num++]->insert(backup_task->server_address());
        res_columns[col_num++]->insert(backup_task->enable_auto_recover());
        res_columns[col_num++]->insert(backup_task->progress());
        Array finished_tables;
        finished_tables.reserve(backup_task->finished_tables_size());
        for (const auto & table : backup_task->finished_tables())
            finished_tables.emplace_back(table);
        res_columns[col_num++]->insert(finished_tables);
        res_columns[col_num++]->insert(backup_task->reschedule_times());
        res_columns[col_num++]->insert(backup_task->last_error());
        res_columns[col_num++]->insert(backup_task->total_backup_size());
        res_columns[col_num++]->insert(TxnTimestamp(backup_task->end_time()).toSecond());
    }
}

}
