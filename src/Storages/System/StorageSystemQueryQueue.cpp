#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Interpreters/Context.h>
#include <Interpreters/QueueManager.h>
#include <Storages/System/StorageSystemQueryQueue.h>

namespace DB
{

NamesAndTypesList StorageSystemQueryQueue::getNamesAndTypes()
{
    return {
        {"query_id", std::make_shared<DataTypeString>()},
        {"virtual_warehouse", std::make_shared<DataTypeString>()},
        {"worker_group", std::make_shared<DataTypeString>()},
        {"enqueue_time", std::make_shared<DataTypeDateTime>()},
        {"queue_duration_ms", std::make_shared<DataTypeUInt64>()},
    };
}

void StorageSystemQueryQueue::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    context->getQueueManager()->getQueueInfo([&](const QueueInfoPtr & queue_info_ptr) {
        size_t i = 0;
        res_columns[i++]->insert(queue_info_ptr->query_id);
        res_columns[i++]->insert(queue_info_ptr->vw_name);
        res_columns[i++]->insert(queue_info_ptr->wg_name);
        res_columns[i++]->insert(queue_info_ptr->enqueue_time);
        res_columns[i++]->insert(queue_info_ptr->sw.elapsedMilliseconds());
    });
}

}
