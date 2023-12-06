#include <Interpreters/InterpreterCreateSnapshotQuery.h>

#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Transaction/Actions/DDLCreateAction.h>
#include <Transaction/IntentLock.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CNCH_SNAPSHOT_ALREADY_EXISTS;
    extern const int CNCH_TRANSACTION_NOT_INITIALIZED;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_TABLE;
}

BlockIO InterpreterCreateSnapshotQuery::execute()
{
    ASTCreateSnapshotQuery & query = query_ptr->as<ASTCreateSnapshotQuery &>();

    auto current_context = getContext();
    /// TODO: add snapshot access type
    current_context->checkAccess(AccessType::CREATE);

    auto current_database = current_context->getCurrentDatabase();
    if (query.database.empty())
        query.database = current_database;
    if (query.to_table_id && query.to_table_id.database_name.empty())
        query.to_table_id.database_name = current_database;

    if (query.to_table_id && query.database != query.to_table_id.database_name)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot bind snapshot to table in another database");

    DatabasePtr database = DatabaseCatalog::instance().getDatabase(query.database, current_context);
    if (database->getEngineName() != "Cnch")
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot create snapshot under database of type '{}'", database->getEngineName());

    if (query.to_table_id)
    {
        auto table = database->tryGetTable(query.to_table_id.table_name, current_context);
        if (!table)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "No table {} found", query.to_table_id.table_name);
        auto * cnch_table = dynamic_cast<StorageCnchMergeTree *>(table.get());
        if (!cnch_table)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot bind snapshot to table of type '{}'", table->getName());
        query.to_table_id.uuid = cnch_table->getStorageUUID();
    }

    if (database->getUUID() == UUIDHelpers::Nil)
    {
        /// TODO: add UUID for db
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Require UUID for DB");
    }

    auto txn = current_context->getCurrentTransaction();
    if (!txn)
        throw Exception("Cnch transaction is not initialized", ErrorCodes::CNCH_TRANSACTION_NOT_INITIALIZED);

    IntentLockPtr db_lock;
    IntentLockPtr ss_lock;
    if (current_context->getSettingsRef().bypass_ddl_db_lock)
    {
        ss_lock = txn->createIntentLock(IntentLock::SS_LOCK_PREFIX, query.database, query.table);
        ss_lock->lock();
    }
    else
    {
        db_lock = txn->createIntentLock(IntentLock::DB_LOCK_PREFIX, query.database);
        ss_lock = txn->createIntentLock(IntentLock::SS_LOCK_PREFIX, query.database, query.table);
        std::lock(*db_lock, *ss_lock);
    }

    if (auto snapshot = current_context->getCnchCatalog()->tryGetSnapshot(database->getUUID(), query.table))
    {
        if (query.if_not_exists)
            return {};
        throw Exception(
            ErrorCodes::CNCH_SNAPSHOT_ALREADY_EXISTS,
            "Snapshot {}.{} already exists",
            backQuoteIfNeed(query.database),
            backQuoteIfNeed(query.table));
    }

    CreateActionParams params = CreateSnapshotParams{database->getUUID(), query.table, query.ttl_in_days, query.to_table_id.uuid};
    auto action = txn->createAction<DDLCreateAction>(std::move(params));
    txn->appendAction(std::move(action));
    txn->commitV1();

    return {};
}

} // namespace DB
