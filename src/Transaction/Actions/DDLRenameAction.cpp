#include "DDLRenameAction.h"

#include <Catalog/Catalog.h>
// #include <DaemonManager/DaemonManagerClient.h>
#include <Parsers/ASTDropQuery.h>
// #include <Storages/Kafka/StorageCnchKafka.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
// #include <Storages/StorageCnchMergeTree.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <Storages/StorageCnchMergeTree.h>

namespace DB
{

void DDLRenameAction::renameTablePrefix(TxnTimestamp commit_time)
{
    Catalog::CatalogPtr cnch_catalog = context.getCnchCatalog();
    // auto daemon_manager = context.getDaemonManagerClient();
    // if (!daemon_manager)
    //     throw Exception("No DaemonManager client available.", ErrorCodes::LOGICAL_ERROR);

    auto storage = cnch_catalog->tryGetTable(context, params.table_params.from_database, params.table_params.from_table, commit_time);

    if (const auto * cnch_table = dynamic_cast<const StorageCnchMergeTree *>(storage.get()))
    {
        is_cnch_merge_tree = true;
        // if (cnch_table->settings.cnch_enable_memory_buffer)
        // {
        //     LOG_DEBUG(&Logger::get("DDLRenameAction"),
        //             "Destroying memory buffer before renaming table: " << cnch_table->getStorageID().getFullTableName());

        //     daemon_manager->controlDaemonJob(cnch_table->getStorageID(), CnchBGThreadType::MemoryBuffer, Protos::ControlDaemonJobReq::Drop);
        // }
    }
    // else if (auto kafka_table = dynamic_cast<const StorageCnchKafka *>(storage.get()))
    // {
    //     is_cnch_kafka = true;
    //     if (kafka_table->tableIsActive())
    //     {
    //         LOG_DEBUG(&Logger::get("DDLRenameAction"),
    //                   "Stop consume before renaming table: " << kafka_table->getStorageID().getFullTableName());
    //         daemon_manager->controlDaemonJob(kafka_table->getStorageID(), CnchBGThreadType::Consumer,
    //                                          Protos::ControlDaemonJobReq::Stop);
    //     }
    // }
    else
        throw Exception("Only CnchMergeTree are supported to rename now", ErrorCodes::LOGICAL_ERROR);
}

void DDLRenameAction::executeV1(TxnTimestamp commit_time)
{
    Catalog::CatalogPtr cnch_catalog = context.getCnchCatalog();

    if (params.type == RenameActionParams::Type::RENAME_TABLE)
    {
        renameTablePrefix(commit_time);

        updateTsCache(params.table_params.from_table_uuid, commit_time);
        cnch_catalog->renameTable(params.table_params.from_database, params.table_params.from_table,
                                  params.table_params.to_database, params.table_params.to_table, txn_id, commit_time);

        renameTableSuffix(commit_time);
    }
    else
    {
        for (auto & uuid : params.db_params.uuids)
            updateTsCache(uuid, commit_time);

        cnch_catalog->renameDatabase(params.db_params.from_database, params.db_params.to_database, txn_id, commit_time);
    }
}

void DDLRenameAction::renameTableSuffix(TxnTimestamp commit_time)
{
    Catalog::CatalogPtr cnch_catalog = context.getCnchCatalog();
    // auto daemon_manager = context.getDaemonManagerClient();
    // if (!daemon_manager)
    //     throw Exception("No DaemonManager client available.", ErrorCodes::LOGICAL_ERROR);

    auto storage = cnch_catalog->tryGetTable(context, params.table_params.to_database, params.table_params.to_table, commit_time);
    if (is_cnch_merge_tree)
    {
        const auto * cnch_table = dynamic_cast<const StorageCnchMergeTree *>(storage.get());
        if (!cnch_table)
            throw Exception("Can not get cnch table after renaming", ErrorCodes::LOGICAL_ERROR);

        // if (cnch_table->settings.cnch_enable_memory_buffer)
        // {
        //     LOG_DEBUG(&Poco::Logger::get("DDLRenameAction"),
        //             "Starting memory buffer after renaming table: " << cnch_table->getStorageID().getFullTableName());

        //     daemon_manager->controlDaemonJob(cnch_table->getStorageID(), CnchBGThreadType::MemoryBuffer, Protos::ControlDaemonJobReq::Start);
        // }
    }
    // else if (is_cnch_kafka)
    // {
    //     auto kafka_table = dynamic_cast<const StorageCnchKafka *>(storage.get());
    //     if (!kafka_table)
    //         throw Exception("Can not get cnch-kafka table after renaming", ErrorCodes::LOGICAL_ERROR);

    //     if (kafka_table->tableIsActive())
    //     {
    //         LOG_DEBUG(&Logger::get("DDLRenameAction"),
    //                   "Starting Consume after renaming table: " << kafka_table->getStorageID().getFullTableName());
    //         daemon_manager->controlDaemonJob(kafka_table->getStorageID(), CnchBGThreadType::Consumer,
    //                                          Protos::ControlDaemonJobReq::Start);
    //     }
    // }
}

void DDLRenameAction::updateTsCache(const UUID & uuid, const TxnTimestamp & commit_time)
{
    auto & ts_cache_manager = context.getCnchTransactionCoordinator().getTsCacheManager();
    auto table_guard = ts_cache_manager.getTimestampCacheTableGuard(uuid);
    auto & ts_cache = ts_cache_manager.getTimestampCacheUnlocked(uuid);
    ts_cache->insertOrAssign(UUIDHelpers::UUIDToString(uuid), commit_time);
}
}
