#include <Storages/System/StorageSystemBGThreads.h>

#include <CloudServices/CnchBGThreadsMap.h>
#include <CloudServices/ICnchBGThread.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>

namespace DB
{
NamesAndTypesList StorageSystemBGThreads::getNamesAndTypes()
{
    return {
        {"type", std::make_shared<DataTypeString>()},
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"uuid", std::make_shared<DataTypeUUID>()},
        {"status", std::make_shared<DataTypeString>()},
        {"startup_time", std::make_shared<DataTypeDateTime>()},
        {"last_wakeup_interval", std::make_shared<DataTypeInt64>()},
        {"last_wakeup_time", std::make_shared<DataTypeDateTime>()},
        {"num_wakeup", std::make_shared<DataTypeUInt64>()},
    };
}

void StorageSystemBGThreads::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    for (auto i = CnchBGThreadType::ServerMinType; i <= CnchBGThreadType::ServerMaxType; i = CnchBGThreadType(size_t(i) + 1))
    {
        for (auto && [_, t] : context->getCnchBGThreadsMap(i)->getAll())
        {
            size_t c = 0;
            res_columns[c++]->insert(toString(t->getType()));
            auto storage_id = t->getStorageID();
            res_columns[c++]->insert(storage_id.database_name);
            res_columns[c++]->insert(storage_id.table_name);
            res_columns[c++]->insert(storage_id.uuid);
            res_columns[c++]->insert(toString(t->getThreadStatus()));
            res_columns[c++]->insert(t->getStartupTime());
            res_columns[c++]->insert(t->getLastWakeupInterval());
            res_columns[c++]->insert(t->getLastWakeupTime());
            res_columns[c++]->insert(t->getNumWakeup());
        }
    }
}

}
