#include <algorithm>
#include <string>
#include <CloudServices/CnchObjectColumnSchemaAssembleThread.h>
#include <DataTypes/ObjectUtils.h>
#include <Interpreters/Context.h>
#include <Storages/CnchStorageCache.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/PartCacheManager.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/StorageSnapshot.h>
#include <Transaction/TxnTimestamp.h>
#include <fmt/format.h>

namespace DB
{
CnchObjectColumnSchemaAssembleThread::CnchObjectColumnSchemaAssembleThread(ContextPtr context_, const StorageID & id_)
    : ICnchBGThread(context_, CnchBGThreadType::ObjectSchemaAssemble, id_)
{
}

void CnchObjectColumnSchemaAssembleThread::runImpl()
{
    try
    {
        auto storage = getStorageFromCatalog();
        auto table_uuid = storage->getStorageUUID();
        auto & table = checkAndGetCnchTable(storage);
        auto storage_settings = table.getSettings();
        auto database_name = storage->getDatabaseName();
        auto table_name = storage->getTableName();

        if (storage->is_dropped)
        {
            LOG_DEBUG(log, "Table was dropped, wait for removing...");
            scheduled_task->scheduleAfter(10 * 1000);
            return;
        }

        if (storage->supportsDynamicSubcolumns() && storage->getInMemoryMetadataPtr()->hasDynamicSubcolumns())
        {
            LOG_INFO(log, "{}.{} Start assemble partial schemas", database_name, table_name);
            auto catalog = getContext()->getCnchCatalog();
            auto [current_topology_version, current_topology] = getContext()->getCnchTopologyMaster()->getCurrentTopologyVersion();

            // Step 1:scan object partial schema and drop uncommitted partial schemas
            auto old_assembled_schema = catalog->tryGetTableObjectAssembledSchema(table_uuid);
            auto partial_schemas
                = catalog->tryGetTableObjectPartialSchemas(table_uuid, storage_settings->json_partial_schema_assemble_batch_size);
            LOG_DEBUG(log, "{}.{} Before assemble. Assembled schema :{}", database_name, table_name, old_assembled_schema.toString());

            if (partial_schemas.empty())
            {
                LOG_INFO(log, "{}.{} no need to refresh dynamic object column schema.", database_name, table_name);
                scheduled_task->scheduleAfter(50 * 1000);
                return;
            }

            std::vector<TxnTimestamp> unfiltered_partial_schema_txnids;
            unfiltered_partial_schema_txnids.reserve(partial_schemas.size());
            for (const auto & [txn_id, partial_schema] : partial_schemas)
            {
                LOG_DEBUG(
                    log,
                    "{}.{} Before assemble. Partial schema :[{}->{}]",
                    database_name,
                    table_name,
                    txn_id.toString(),
                    partial_schema.toString());

                unfiltered_partial_schema_txnids.emplace_back(txn_id);
            }

            auto committed_partial_schema_txnids = catalog->filterUncommittedObjectPartialSchemas(unfiltered_partial_schema_txnids);

            // Step 2:assemble partial schema to assembled schema
            std::vector<String> partial_schema_txn_ids_for_print;
            std::vector<ColumnsDescription> schemas_ready_to_assemble;
            partial_schema_txn_ids_for_print.reserve(committed_partial_schema_txnids.size());
            schemas_ready_to_assemble.reserve(committed_partial_schema_txnids.size() + 1);
            for (auto & txn_id : committed_partial_schema_txnids)
            {
                auto partial_schema = partial_schemas[txn_id];
                schemas_ready_to_assemble.emplace_back(partial_schema);
                partial_schema_txn_ids_for_print.emplace_back(txn_id.toString());
            }
            schemas_ready_to_assemble.emplace_back(old_assembled_schema);
            auto new_assembled_schema = DB::getConcreteObjectColumns(
                schemas_ready_to_assemble.begin(),
                schemas_ready_to_assemble.end(),
                storage->getInMemoryMetadataPtr()->getColumns(),
                [](const auto & schema) { return schema; });

            // Step 3:update assembled schema and delete partial schema in meta store
            // TODO:@lianwenlong consider purge fail and check lease
            auto cas_put_result = catalog->resetObjectAssembledSchemaAndPurgePartialSchemas(
                table_uuid, old_assembled_schema, new_assembled_schema, committed_partial_schema_txnids);

            LOG_DEBUG(
                log,
                "{}.{} After assemble.Assembled schema :{} and deleted txn ids:{}, result:{}",
                database_name,
                table_name,
                new_assembled_schema.toString(),
                fmt::join(partial_schema_txn_ids_for_print, ","),
                std::to_string(cas_put_result));

            if (cas_put_result)
            {
                // Step 4:update assembled schema and delete partial schema in storage cache
                if (auto cache_manager = getContext()->getPartCacheManager())
                {
                    if (auto storage_in_cache = cache_manager->getStorageFromCache(table_uuid, current_topology_version, *getContext()))
                    {
                        auto & table_in_cache = checkAndGetCnchTable(storage_in_cache);
                        table_in_cache.refreshAssembledSchema(new_assembled_schema, committed_partial_schema_txnids);
                    }
                }

                // Step5: clean partial schema status in meta store including
                catalog->batchDeleteObjectPartialSchemaStatus(committed_partial_schema_txnids);
                // Step6: @TODO:lianwenlong rollback aborted partial schema from meta store and storage cache
            }

            LOG_INFO(
                log, "{}.{} Finish assemble partial schemas with result:{}", database_name, table_name, std::to_string(cas_put_result));
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    scheduled_task->scheduleAfter(50 * 1000);
}

} //namespace DB end
