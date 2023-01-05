#include <Storages/System/StorageSystemGlobalGCManager.h>
#include <Core/NamesAndTypes.h>
#include <Columns/IColumn.h>
#include <Interpreters/Context.h>
#include <Storages/SelectQueryInfo.h>
#include <DataTypes/DataTypeUUID.h>
#include <CloudServices/CnchServerClient.h>
#include <Common/HostWithPorts.h>


namespace DB
{
    NamesAndTypesList StorageSystemGlobalGCManager::getNamesAndTypes()
    {
        return
        {
            {"deleting_uuid", std::make_shared<DataTypeUUID>()}
        };
    }

    void StorageSystemGlobalGCManager::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
    {
        if (context->getServerType() != ServerType::cnch_server)
            return;
        UInt16 port = context->getRPCPort();
        CnchServerClient client{createHostPortString(getLoopbackIPFromEnv(), port)};
        std::set<UUID> deleting_uuid = client.getDeletingTablesInGlobalGC();
        std::for_each(deleting_uuid.begin(), deleting_uuid.end(),
            [& res_columns] (const UUID & uuid) {
                res_columns[0]->insert(uuid);
            });
    }
} // end namespace DB
