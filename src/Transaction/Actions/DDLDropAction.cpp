#include "DDLDropAction.h"

#include <Catalog/Catalog.h>
// #include <Interpreters/CnchSystemLog.h>
#include <Parsers/ASTDropQuery.h>
// #include <Storages/Kafka/StorageCnchKafka.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Storages/StorageCnchMergeTree.h>
// #include <MergeTreeCommon/MemoryBufferManager.h>
// #include <DaemonManager/DaemonManagerClient.h>
#include <CloudServices/CnchServerClient.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <Common/ErrorCodes.h>

namespace DB
{

namespace ErrorCodes
{
    extern const ErrorCode SYNTAX_ERROR;
}

void DDLDropAction::executeV1(TxnTimestamp commit_time)
{
    Catalog::CatalogPtr cnch_catalog = getContext()->getCnchCatalog();

    if (!params.database.empty() && params.table.empty())
    {
        // drop database
        if (params.kind == ASTDropQuery::Kind::Drop)
        {
            for (auto & table : tables)
                updateTsCache(table->getStorageUUID(), commit_time);
            cnch_catalog->dropDatabase(params.database, params.prev_version, txn_id, commit_time); /*mark database deleted, we just remove database now.*/
        }
        else if (params.kind == ASTDropQuery::Kind::Truncate)
        {
            throw Exception("Unable to truncate database.", ErrorCodes::SYNTAX_ERROR);
        }
    }
    else if (tables.size()==1 && tables[0] && !params.database.empty() && !params.table.empty())
    {
        auto table = tables[0];

        // drop table
        if (params.kind == ASTDropQuery::Kind::Drop)
        {
            // if (auto storage = dynamic_cast<StorageCnchMergeTree *>(table.get()))
            // {
            //     // Drop preallocated tables
            //     if (!storage->isOnDemandMode())
            //         storage->dropPreallocatedTables(getContext()->;
            //     else if (isQueryMetricsTable(storage->getDatabaseName(), storage->getTableName()))
            //         storage->sendDropLocalQuery(getContext()->;

            //     // Destroy memory buffer if any
            //     if (storage->settings.cnch_enable_memory_buffer)
            //     {
            //         auto daemon_manager = getContext()->getDaemonManagerClient();
            //         if (!daemon_manager)
            //             throw Exception("No DaemonManager client available.", ErrorCodes::LOGICAL_ERROR);

            //         LOG_DEBUG(&Logger::get("DDLDropAction"),
            //                   "Destroying memory buffer for storage: " << storage->getStorageID().getNameForLogs());
            //         daemon_manager->controlDaemonJob(
            //                 storage->getStorageID(), CnchBGThreadType::MemoryBuffer, Protos::ControlDaemonJobReq::Drop);
            //     }
            // }
            // else if (auto kafka_table = dynamic_cast<StorageCnchKafka *>(table.get()))
            // {
            //     kafka_table->drop();
            // }

            updateTsCache(table->getStorageUUID(), commit_time);

            // Mark delete
            cnch_catalog->dropTable(table, params.prev_version, txn_id, commit_time);
        }
        else if (params.kind == ASTDropQuery::Kind::Detach)
        {
            updateTsCache(table->getStorageUUID(), commit_time);
            cnch_catalog->detachTable(params.database, params.table, commit_time);
        }
        else if (params.kind == ASTDropQuery::Kind::Truncate)
        {
            throw Exception("Logical error: shouldn't be here", ErrorCodes::LOGICAL_ERROR);
        }
    }
}

void DDLDropAction::updateTsCache(const UUID & uuid, const TxnTimestamp & commit_time)
{
    auto & ts_cache_manager = getContext()->getCnchTransactionCoordinator().getTsCacheManager();
    auto table_guard = ts_cache_manager.getTimestampCacheTableGuard(uuid);
    auto & ts_cache = ts_cache_manager.getTimestampCacheUnlocked(uuid);
    ts_cache->insertOrAssign(UUIDHelpers::UUIDToString(uuid), commit_time);
}

}
