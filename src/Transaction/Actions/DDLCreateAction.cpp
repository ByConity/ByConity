#include "DDLCreateAction.h"

#include <Catalog/Catalog.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>

namespace DB
{

void DDLCreateAction::executeV1(TxnTimestamp commit_time)
{
    Catalog::CatalogPtr cnch_catalog = global_context.getCnchCatalog();

    if (!params.database.empty() && params.table.empty())
    {
        /// create database
        assert(!params.attach);
        cnch_catalog->createDatabase(params.database, params.uuid, txn_id, commit_time);
    }
    else
    {
        /// create table
        updateTsCache(params.uuid, commit_time);
        if (params.attach)
            cnch_catalog->attachTable(params.database, params.table, commit_time);
        else
            cnch_catalog->createTable(StorageID{params.database, params.table, params.uuid}, params.statement, "", txn_id, commit_time);
    }
}

void DDLCreateAction::updateTsCache(const UUID & uuid, const TxnTimestamp & commit_time)
{
    auto & ts_cache_manager = global_context.getCnchTransactionCoordinator().getTsCacheManager();
    auto table_guard = ts_cache_manager.getTimestampCacheTableGuard(uuid);
    auto & ts_cache = ts_cache_manager.getTimestampCacheUnlocked(uuid);
    ts_cache->insertOrAssign(UUIDHelpers::UUIDToString(uuid), commit_time);
}

void DDLCreateAction::abort() {}

}
