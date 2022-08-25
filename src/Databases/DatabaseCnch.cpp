#include <Databases/DatabaseCnch.h>

#include <Catalog/Catalog.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Transaction/Actions/DDLCreateAction.h>
#include <Transaction/Actions/DDLDropAction.h>
#include <Transaction/ICnchTransaction.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CNCH_TRANSACTION_NOT_INITIALIZED;
    extern const int SYSTEM_ERROR;
}

class CnchDatabaseTablesSnapshotIterator final : public DatabaseTablesSnapshotIterator
{
public:
    using DatabaseTablesSnapshotIterator::DatabaseTablesSnapshotIterator;
    UUID uuid() const override { return table()->getStorageID().uuid; }
};

DatabaseCnch::DatabaseCnch(const String & name_, UUID uuid, ContextPtr local_context)
    : IDatabase(name_)
    , WithContext(local_context->getGlobalContext())
    , db_uuid(uuid)
    , log(&Poco::Logger::get("DatabaseCnch (" + name_ + ")"))
{
    LOG_DEBUG(log, "Create database {} in query {}", database_name, local_context->getCurrentQueryId());
}

void DatabaseCnch::createTable(ContextPtr local_context, const String & table_name, const StoragePtr & table, const ASTPtr & query)
{
    LOG_DEBUG(log, "Create table {} in query {}", table_name, local_context->getCurrentQueryId());
    if (!query->as<ASTCreateQuery>())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query is not create query");
    auto create_query = query->as<ASTCreateQuery &>();
    String statement = serializeAST(*query);
    auto txn = local_context->getCurrentTransaction();

    if (!txn)
        throw Exception("Cnch transaction is not initialized", ErrorCodes::CNCH_TRANSACTION_NOT_INITIALIZED);

    CreateActionParams params = {getDatabaseName(), table_name, table->getStorageUUID(), statement, create_query.attach};
    auto create_table = txn->createAction<DDLCreateAction>(std::move(params));
    txn->appendAction(std::move(create_table));
    txn->commitV1();
    LOG_TRACE(log, "Successfully create table {} in query {}", table_name, local_context->getCurrentQueryId());
}

void DatabaseCnch::dropTable(ContextPtr local_context, const String & table_name, bool no_delay)
{
    LOG_DEBUG(log, "Drop table {} in query {}, no delay {}", table_name, local_context->getCurrentQueryId(), no_delay);

    auto txn = local_context->getCurrentTransaction();

    if (!txn)
        throw Exception("Cnch transaction is not initialized", ErrorCodes::CNCH_TRANSACTION_NOT_INITIALIZED);
    //auto table_lock = txn->createIntentLock({database_name, table_name});
    //table_lock->lock();

    StoragePtr storage = local_context->getCnchCatalog()->getTable(*local_context, getDatabaseName(), table_name, TxnTimestamp::maxTS());
    if (!storage)
        throw Exception("Can't get storage for table " + table_name, ErrorCodes::SYSTEM_ERROR);

    DropActionParams params{getDatabaseName(), table_name, storage->commit_time, ASTDropQuery::Kind::Drop};
    auto drop_action = txn->createAction<DDLDropAction>(std::move(params), std::vector{std::move(storage)});
    txn->appendAction(std::move(drop_action));
    txn->commitV1();
}

void DatabaseCnch::drop(ContextPtr local_context)
{
    LOG_DEBUG(log, "Drop database {} in query {}", database_name, local_context->getCurrentQueryId());

    auto txn = local_context->getCurrentTransaction();

    if (!txn)
        throw Exception("Cnch transaction is not initialized", ErrorCodes::CNCH_TRANSACTION_NOT_INITIALIZED);

    DropActionParams params{getDatabaseName(), "", commit_time, ASTDropQuery::Kind::Drop};
    auto drop_action = txn->createAction<DDLDropAction>(std::move(params));
    txn->appendAction(std::move(drop_action));
    txn->commitV1();
}

void DatabaseCnch::detachTablePermanently(ContextPtr local_context, const String & name)
{
    TransactionCnchPtr txn = local_context->getCurrentTransaction();

    if (!txn)
        throw Exception("Cnch transaction is not initialized", ErrorCodes::CNCH_TRANSACTION_NOT_INITIALIZED);

    auto table = tryGetTable(name, local_context);

    if (!table)
        throw Exception("Table " + name + "." + name + " doesn't exists.", ErrorCodes::UNKNOWN_TABLE);

    /// detach table action
    DropActionParams params{getDatabaseName(), name, table->commit_time, ASTDropQuery::Kind::Detach};
    auto detach_action = txn->createAction<DDLDropAction>(std::move(params), std::vector{table});
    txn->appendAction(std::move(detach_action));
    txn->commitV1();
}

ASTPtr DatabaseCnch::getCreateDatabaseQuery() const
{
    auto settings = getContext()->getSettingsRef();
    String query = "CREATE DATABASE " + backQuoteIfNeed(getDatabaseName()) + " ENGINE = Cnch";
    ParserCreateQuery parser(ParserSettings::valueOf(settings.dialect_type));
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "", 0, settings.max_parser_depth);
    return ast;
}

bool DatabaseCnch::isTableExist(const String & name, ContextPtr local_context) const
{
    return local_context->getCnchCatalog()->isTableExists(getDatabaseName(), name, local_context->getCurrentTransactionID().toUInt64());
}

StoragePtr DatabaseCnch::tryGetTable(const String & name, ContextPtr local_context) const
{
    try
    {
        auto res = tryGetTableImpl(name, local_context);
        if (res && !res->is_detached && !res->is_dropped)
            return res;
    }
    catch (Exception & e)
    {
        if (e.code() != ErrorCodes::UNKNOWN_TABLE)
            throw e;
    }
    return nullptr;
}

DatabaseTablesIteratorPtr DatabaseCnch::getTablesIterator(ContextPtr local_context, const FilterByNameFunction & filter_by_table_name)
{
    Tables tables;
    Strings names = local_context->getCnchCatalog()->getTablesInDB(getDatabaseName());
    std::for_each(names.begin(), names.end(), [this, &local_context, &tables](const String & name) {
        StoragePtr storage = tryGetTableImpl(name, local_context);
        if (!storage)
            throw Exception("Can't get storage for table " + name, ErrorCodes::UNKNOWN_TABLE);
        if (storage->is_detached || storage->is_dropped)
            return;
        /// debug
        StorageID storage_id = storage->getStorageID();
        LOG_DEBUG(
            log,
            "UUID {} database {} table {}",
            UUIDHelpers::UUIDToString(storage_id.uuid),
            storage_id.getDatabaseName(),
            storage_id.getTableName());
        /// end debug
        tables.try_emplace(name, std::move(storage));
    });

    if (!filter_by_table_name)
        return std::make_unique<CnchDatabaseTablesSnapshotIterator>(std::move(tables), database_name);

    Tables filtered_tables;
    for (const auto & [table_name, storage] : tables)
        if (filter_by_table_name(table_name))
            filtered_tables.emplace(table_name, storage);

    return std::make_unique<CnchDatabaseTablesSnapshotIterator>(std::move(filtered_tables), database_name);
}

bool DatabaseCnch::empty() const
{
    Strings tables = getContext()->getCnchCatalog()->getTablesInDB(getDatabaseName());
    return tables.empty();
}

ASTPtr DatabaseCnch::getCreateTableQueryImpl(const String & name, ContextPtr local_context, bool throw_on_error) const
{
    StoragePtr storage{};
    if (throw_on_error)
    {
        storage = getContext()->getCnchCatalog()->getTable(
            *local_context, getDatabaseName(), name, local_context->getCurrentTransactionID().toUInt64());
    }
    else
    {
        storage = getContext()->getCnchCatalog()->tryGetTable(
            *local_context, getDatabaseName(), name, local_context->getCurrentTransactionID().toUInt64());
    }

    if (!storage)
        return {};

    String create_table_query = storage->getCreateTableSql();
    ParserCreateQuery p_create_query;
    ASTPtr ast{};
    try
    {
        ast = parseQuery(
            p_create_query,
            create_table_query,
            local_context->getSettingsRef().max_query_size,
            local_context->getSettingsRef().max_parser_depth);
    }
    catch (...)
    {
        if (throw_on_error)
            throw;
        else
            LOG_DEBUG(
                log,
                "Fail to parseQuery for table {} in datase {} query id {}, create query {}",
                name,
                getDatabaseName(),
                local_context->getCurrentQueryId(),
                create_table_query);
    }

    return ast;
}

void DatabaseCnch::createEntryInCnchCatalog(ContextPtr local_context) const
{
    auto txn = local_context->getCurrentTransaction();
    if (!txn)
        throw Exception("Cnch transaction is not initialized", ErrorCodes::CNCH_TRANSACTION_NOT_INITIALIZED);

    CreateActionParams params = {getDatabaseName(), "", getUUID(), ""};
    auto create_db = txn->createAction<DDLCreateAction>(std::move(params));
    txn->appendAction(std::move(create_db));
    txn->commitV1();
}

StoragePtr DatabaseCnch::tryGetTableImpl(const String & name, ContextPtr local_context) const
{
    return getContext()->getCnchCatalog()->getTable(
        *local_context, getDatabaseName(), name, local_context->getCurrentTransactionID().toUInt64());
}

}
