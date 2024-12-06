#include <Catalog/CatalogBackgroundTask.h>
#include <Catalog/MetastoreProxy.h>
#include <Catalog/LargeKVHandler.h>
#include <MergeTreeCommon/CnchServerLeader.h>


namespace DB
{

namespace Catalog
{

CatalogBackgroundTask::CatalogBackgroundTask(
    const ContextPtr & context_,
    const std::shared_ptr<IMetaStore> & metastore_,
    const String & name_space_)
    : context(context_),
      metastore(metastore_),
      name_space(name_space_)
{
    task_holder = context->getSchedulePool().createTask(
        "CatalogBGTask",
        [this](){
            execute();
        }
    );

    task_holder->activate();
    // wait for server startup
    task_holder->scheduleAfter(30*1000);
}

CatalogBackgroundTask::~CatalogBackgroundTask()
{
    try
    {
        task_holder->deactivate();
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

void CatalogBackgroundTask::execute()
{
    // only server can perform catalog bg task
    if (context->getServerType() != ServerType::cnch_server)
        return;

    LOG_DEBUG(log, "Try execute catalog bg task.");
    try
    {
        cleanStaleLargeKV();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Exception happens while executing catalog bg task.");
    }

    // execute every 1 hour.
    task_holder->scheduleAfter(60*60*1000);
}

void CatalogBackgroundTask::cleanStaleLargeKV()
{
    // only leader can execute clean job
    if (!context->getCnchServerLeader()->isLeader())
        return;

    // scan large kv records
    std::unordered_map<String, String> uuid_to_key;
    String large_kv_reference_prefix = MetastoreProxy::largeKVReferencePrefix(name_space);
    auto it = metastore->getByPrefix(large_kv_reference_prefix);

    while (it->next())
    {
        String uuid = it->key().substr(large_kv_reference_prefix.size());
        uuid_to_key.emplace(uuid, it->value());
    }

    // check for each large KV if still been referenced by stored key
    for (const auto & [uuid, key] : uuid_to_key)
    {
        String value;
        metastore->get(key, value);
        if (!value.empty())
        {
            Protos::DataModelLargeKVMeta large_kv_model;
            if (tryParseLargeKVMetaModel(value, large_kv_model) && large_kv_model.uuid() == uuid)
                continue;
        }

        // remove large KV because it is not been referenced by original key
        BatchCommitRequest batch_write;
        BatchCommitResponse resp;

        auto large_kv_it = metastore->getByPrefix(MetastoreProxy::largeKVDataPrefix(name_space, uuid));
        while (large_kv_it->next())
            batch_write.AddDelete(large_kv_it->key());

        batch_write.AddDelete(MetastoreProxy::largeKVReferenceKey(name_space, uuid));

        try
        {
            metastore->batchWrite(batch_write, resp);
            LOG_DEBUG(log, "Removed large KV(uuid: {}) from metastore.", uuid);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Error occurs while removing large kv.");
        }
    }
}

}

}
