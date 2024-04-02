#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/QueueManager.h>
#include <Interpreters/VirtualWarehousePool.h>
#include <Interpreters/VirtualWarehouseQueue.h>
#include <Storages/System/StorageSystemVirtualWarehouseQueryQueue.h>

namespace DB
{

NamesAndTypesList StorageSystemVirtualWarehouseQueryQueue::getNamesAndTypes()
{
    return {
        {"virtual_warehouse", std::make_shared<DataTypeString>()},
        {"queue_name", std::make_shared<DataTypeString>()},
        {"max_concurrency", std::make_shared<DataTypeUInt64>()},
        {"query_queue_size", std::make_shared<DataTypeUInt64>()},
        {"rule_name", std::make_shared<DataTypeString>()},
        {"databases", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"tables", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"user", std::make_shared<DataTypeString>()},
        {"ip", std::make_shared<DataTypeString>()},
        {"query_id", std::make_shared<DataTypeString>()},
        {"fingerprint", std::make_shared<DataTypeString>()}};
}

void StorageSystemVirtualWarehouseQueryQueue::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    auto all_vw = context->getVirtualWarehousePool().getAll();
    for (auto iter = all_vw.begin(); iter != all_vw.end(); iter++)
    {
        iter->second->getQueueManager().fillSystemTable([&](VirtualWarehouseQueue & queue) {

            const auto rules = queue.getRules();
            if (rules.empty())
            {
                size_t i = 0;
                res_columns[i++]->insert(iter->first);
                res_columns[i++]->insert(queue.queueName());
                res_columns[i++]->insert(queue.maxConcurrency());
                res_columns[i++]->insert(queue.queryQueueSize());
                DB::Array tmp;
                res_columns[i++]->insert("");
                res_columns[i++]->insert(tmp);
                res_columns[i++]->insert(tmp);
                res_columns[i++]->insert("");
                res_columns[i++]->insert("");
                res_columns[i++]->insert("");
                res_columns[i++]->insert("");
            }
            else
            {
                for (const auto & rule : rules)
                {
                    size_t i = 0;
                    res_columns[i++]->insert(iter->first);
                    res_columns[i++]->insert(queue.queueName());
                    res_columns[i++]->insert(queue.maxConcurrency());
                    res_columns[i++]->insert(queue.queryQueueSize());
                    res_columns[i++]->insert(rule.rule_name);
                    DB::Array tmp;
                    tmp.reserve(rule.databases.size());
                    for (const auto & database : rule.databases)
                    {
                        tmp.emplace_back(database); 
                    }
                    res_columns[i++]->insert(tmp);

                    tmp.clear();
                    for (const auto & table : rule.tables)
                    {
                        tmp.emplace_back(table); 
                    }
                    res_columns[i++]->insert(tmp);
                    res_columns[i++]->insert(rule.user);
                    res_columns[i++]->insert(rule.ip);
                    res_columns[i++]->insert(rule.query_id);
                    res_columns[i++]->insert(rule.fingerprint);
                }
            }
            

        });
    }
}

}
