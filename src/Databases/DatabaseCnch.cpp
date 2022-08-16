#include <Databases/DatabaseCnch.h>

#include <Catalog/Catalog.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>
#include <Transaction/Actions/DDLCreateAction.h>
#include <Transaction/Actions/DDLDropAction.h>
#include <Transaction/ICnchTransaction.h>
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
    : IDatabase(name_), WithContext(local_context->getGlobalContext())
    , db_uuid(uuid)
    , log(&Poco::Logger::get("DatabaseCnch (" + name_ + ")"))
{
    LOG_DEBUG(log, "Create database {} in query {}", database_name, local_context->getCurrentQueryId());
}

void DatabaseCnch::createTable(
    ContextPtr local_context,
    const String & table_name,
    const StoragePtr & table,
    const ASTPtr & query)
{
    LOG_DEBUG(log, "Create table {} in query {}", table_name, local_context->getCurrentQueryId());

    String statement = serializeAST(*query);
    auto txn = local_context->getCurrentTransaction();

    if (!txn)
        throw Exception("Cnch transaction is not initialized", ErrorCodes::CNCH_TRANSACTION_NOT_INITIALIZED);

    CreateActionParams params = {getDatabaseName(), table_name, table->getStorageUUID(), statement};
    auto create_table = txn->createAction<DDLCreateAction>(std::move(params));
    txn->appendAction(std::move(create_table));
    txn->commitV1();
    LOG_TRACE(log, "Successfully create table {} in query {}", table_name, local_context->getCurrentQueryId());
}

void DatabaseCnch::dropTable(
    ContextPtr local_context,
    const String & table_name,
    bool no_delay)
{
    LOG_DEBUG(log, "Drop table {} in query {}, no delay {}"
         , table_name, local_context->getCurrentQueryId(), no_delay);

    auto txn = local_context->getCurrentTransaction();

    if (!txn)
        throw Exception("Cnch transaction is not initialized", ErrorCodes::CNCH_TRANSACTION_NOT_INITIALIZED);
    //auto table_lock = txn->createIntentLock({database_name, table_name});
    //table_lock->lock();

    StoragePtr storage = local_context->getCnchCatalog()->getTable(*local_context, getDatabaseName(), table_name, TxnTimestamp::maxTS());
    if (!storage)
        throw Exception("Can't get storage for table " + table_name, ErrorCodes::SYSTEM_ERROR);

    DropActionParams params{getDatabaseName(), table_name, storage->commit_time, ASTDropQuery::Kind::Drop};
    DDLDropActionPtr drop_action = std::make_shared<DDLDropAction>(local_context, txn->getTransactionID(), std::move(params), std::vector{std::move(storage)});
    txn->appendAction(std::move(drop_action));
    txn->commitV1();
}

void DatabaseCnch::drop(ContextPtr local_context)
{
    LOG_DEBUG(log, "Drop database {} in query {}", database_name , local_context->getCurrentQueryId());

    auto txn = local_context->getCurrentTransaction();

    if (!txn)
        throw Exception("Cnch transaction is not initialized", ErrorCodes::CNCH_TRANSACTION_NOT_INITIALIZED);

    DropActionParams params{getDatabaseName(), "", commit_time, ASTDropQuery::Kind::Drop};
    DDLDropActionPtr drop_action = std::make_shared<DDLDropAction>(local_context, txn->getTransactionID(), std::move(params));
    txn->appendAction(std::move(drop_action));
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
    /// TODO update timestamp
    return local_context->getCnchCatalog()->isTableExists(getDatabaseName(), name, TxnTimestamp::maxTS());
}

StoragePtr DatabaseCnch::tryGetTable(const String & name, ContextPtr local_context) const
{
    /// TODO update timestamp
    StoragePtr res{nullptr};
    try
    {
        res = local_context->getCnchCatalog()->getTable(*local_context, getDatabaseName(), name, TxnTimestamp::maxTS());
    }
    catch (Exception & e)
    {
        if (e.code() != ErrorCodes::UNKNOWN_TABLE)
            throw e;
    }
    return res;
}

DatabaseTablesIteratorPtr DatabaseCnch::getTablesIterator(ContextPtr local_context, const FilterByNameFunction & filter_by_table_name)
{
    Tables tables;
    Strings names = local_context->getCnchCatalog()->getTablesInDB(getDatabaseName());
    std::transform(names.begin(), names.end(), std::inserter(tables, tables.end()),
        [this, & local_context] (const String & name)
        {
            StoragePtr storage = tryGetTable(name, local_context);
            if (!storage)
                throw Exception("Can't get storage for table " + name, ErrorCodes::UNKNOWN_TABLE);

            /// debug
            StorageID storage_id = storage->getStorageID();
            LOG_DEBUG(log, "uuid {} database {} table {}", UUIDHelpers::UUIDToString(storage_id.uuid), storage_id.getDatabaseName(), storage_id.getTableName());

            /// end debug

            return std::make_pair(name, std::move(storage));
        }
    );

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
        storage = getContext()->getCnchCatalog()->getTable(*local_context, getDatabaseName(), name, TxnTimestamp::maxTS());
    }
    else
    {
        storage = getContext()->getCnchCatalog()->tryGetTable(*local_context, getDatabaseName(), name, TxnTimestamp::maxTS());
    }

    if (!storage)
        return {};

    String create_table_query = storage->getCreateTableSql();
    ParserCreateQuery p_create_query;
    ASTPtr ast{};
    try
    {
        ast = parseQuery(p_create_query, create_table_query, local_context->getSettingsRef().max_query_size
            , local_context->getSettingsRef().max_parser_depth);
    }
    catch (...)
    {
        if (throw_on_error)
            throw;
        else
            LOG_DEBUG(log, "Fail to parseQuery for table {} in datase {} query id {}, create query {}",
                name , getDatabaseName(), local_context->getCurrentQueryId(), create_table_query);
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

}
