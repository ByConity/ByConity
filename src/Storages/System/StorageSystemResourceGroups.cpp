#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/ResourceGroupManager.h>
#include <Storages/System/StorageSystemResourceGroups.h>

namespace DB
{
NamesAndTypesList StorageSystemResourceGroups::getNamesAndTypes()
{
    return {
        {"name", std::make_shared<DataTypeString>()},

        {"enable", std::make_shared<DataTypeUInt8>()},

        {"can_run_more", std::make_shared<DataTypeUInt8>()},
        {"can_queue_more", std::make_shared<DataTypeUInt8>()},

        {"soft_max_memory_usage", std::make_shared<DataTypeInt64>()},
        {"cached_memory_usage", std::make_shared<DataTypeInt64>()},

        {"max_concurrent_queries", std::make_shared<DataTypeInt32>()},
        {"running_queries", std::make_shared<DataTypeInt32>()},

        {"max_queued", std::make_shared<DataTypeInt32>()},
        {"queued_queries", std::make_shared<DataTypeInt32>()},

        {"priority", std::make_shared<DataTypeInt32>()},

        {"parent_resource_group", std::make_shared<DataTypeString>()},
    };
}


void StorageSystemResourceGroups::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    ResourceGroupManager::Info infos = context->getResourceGroupManager().getInfo();

    for (const auto & info : infos)
    {
        size_t i = 0;
        res_columns[i++]->insert(info.name);

        res_columns[i++]->insert(context->getResourceGroupManager().isInUse());

        res_columns[i++]->insert(info.can_run_more);
        res_columns[i++]->insert(info.can_queue_more);

        res_columns[i++]->insert(info.soft_max_memory_usage);
        res_columns[i++]->insert(info.cachedMemoryUsageBytes);

        res_columns[i++]->insert(info.max_concurrent_queries);
        res_columns[i++]->insert(info.runningQueries);

        res_columns[i++]->insert(info.max_queued);
        res_columns[i++]->insert(info.queuedQueries);

        res_columns[i++]->insert(info.priority);

        res_columns[i++]->insert(info.parent_resource_group);
    }
}

}
