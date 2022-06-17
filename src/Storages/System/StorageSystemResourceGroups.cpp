#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <ResourceGroup/IResourceGroupManager.h>
#include <Storages/System/StorageSystemResourceGroups.h>

namespace DB
{
NamesAndTypesList StorageSystemResourceGroups::getNamesAndTypes()
{
    return {
        {"name", std::make_shared<DataTypeString>()},
        {"parent_resource_group", std::make_shared<DataTypeString>()},
        {"enable", std::make_shared<DataTypeUInt8>()},
        {"can_run_more", std::make_shared<DataTypeUInt8>()},
        {"can_queue_more", std::make_shared<DataTypeUInt8>()},
        {"priority", std::make_shared<DataTypeInt32>()},

        {"soft_max_memory_usage", std::make_shared<DataTypeInt64>()},
        {"cached_memory_usage", std::make_shared<DataTypeInt64>()},

        {"max_concurrent_queries", std::make_shared<DataTypeInt32>()},
        {"running_queries", std::make_shared<DataTypeInt32>()},

        {"max_queued", std::make_shared<DataTypeInt32>()},
        {"queued_queries", std::make_shared<DataTypeInt32>()},
        {"max_queued_waiting_ms", std::make_shared<DataTypeInt32>()},

        {"cpu_shares", std::make_shared<DataTypeInt32>()},

        {"last_used", std::make_shared<DataTypeDateTime>()},
        {"in_use", std::make_shared<DataTypeUInt8>()},

        {"queued_time_total_ms", std::make_shared<DataTypeUInt64>()},
        {"running_time_total_ms", std::make_shared<DataTypeUInt64>()},
    };
}


void StorageSystemResourceGroups::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    DB::ResourceGroupInfoVec infos = context->tryGetResourceGroupManager()->getInfoVec();

    for (const auto & info : infos)
    {
        size_t i = 0;
        res_columns[i++]->insert(info.name);
        res_columns[i++]->insert(info.parent_resource_group);

        res_columns[i++]->insert(context->tryGetResourceGroupManager()->isInUse());

        res_columns[i++]->insert(info.can_run_more);
        res_columns[i++]->insert(info.can_queue_more);
        res_columns[i++]->insert(info.priority);

        res_columns[i++]->insert(info.soft_max_memory_usage);
        res_columns[i++]->insert(info.cached_memory_usage_bytes);

        res_columns[i++]->insert(info.max_concurrent_queries);
        res_columns[i++]->insert(info.running_queries);

        res_columns[i++]->insert(info.max_queued);
        res_columns[i++]->insert(info.queued_queries);
        res_columns[i++]->insert(info.max_queued_waiting_ms);

        res_columns[i++]->insert(info.cpu_shares);
        
        if (info.last_used > 0)
            res_columns[i++]->insert(info.last_used / 1000);
        else
        {
            LOG_WARNING(&Poco::Logger::get("StorageSystemResourceGroups"), "last_used unset. This should not happen");
            res_columns[i++]->insert(info.last_used);
        }
        res_columns[i++]->insert(info.in_use);

        res_columns[i++]->insert(info.queued_time_total_ms);
        res_columns[i++]->insert(info.running_time_total_ms);
    }
}

}
