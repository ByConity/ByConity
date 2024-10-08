/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <mutex>
#include <Databases/DatabaseCnch.h>

#include <Catalog/Catalog.h>
#include <Catalog/CatalogFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/VirtualWarehousePool.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Transaction/Actions/DDLCreateAction.h>
#include <Transaction/Actions/DDLDropAction.h>
#include <Transaction/ICnchTransaction.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/Exception.h>
#include <Common/Status.h>
#include <common/logger_useful.h>
#include <common/scope_guard.h>
#include <Transaction/Actions/DDLRenameAction.h>
#include <Storages/StorageMaterializedView.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CNCH_TRANSACTION_NOT_INITIALIZED;
    extern const int SYSTEM_ERROR;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

String getObjectDefinitionFromCreateQueryForCnch(const ASTPtr & query)
{
    ASTPtr query_clone = query->clone();
    auto & create = query_clone->as<ASTCreateQuery &>();

    /// We remove everything that is not needed for ATTACH from the query.
    create.attach = false;
    create.as_database.clear();
    create.as_table.clear();
    create.if_not_exists = false;
    create.is_populate = false;
    create.replace_view = false;
    create.replace_table = false;
    create.create_or_replace = false;

    /// For views it is necessary to save the SELECT query itself, for the rest - on the contrary
    if (!create.isView() && !create.is_materialized_view)
        create.select = nullptr;

    create.format = nullptr;
    create.out_file = nullptr;

    WriteBufferFromOwnString statement_buf;
    formatAST(create, statement_buf, false);
    writeChar('\n', statement_buf);
    return statement_buf.str();
}

void checkCreateIsAllowedInCnch(ContextPtr local_context, const ASTPtr & query)
{
    auto * create_query = query->as<ASTCreateQuery>();
    if (!create_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query is not create query");

    // Disable create table as function for cnch database first.
    // Todo: add proper support for this new feature
    if (!create_query->storage && create_query->as_table_function)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "create table as table function is not supported under cnch database");

    if (create_query->is_dictionary || create_query->isView())
        return;

    if (!create_query->storage->engine)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Table engine is unknown, you should specific engine to Cnch/MySQL");

    static constexpr auto allowed_engine = {"Cnch", "MySQL"};

    if (!std::any_of(allowed_engine.begin(), allowed_engine.end(), [&](auto & engine) { return startsWith(create_query->storage->engine->name, engine); }))
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Cnch database only support creating Cnch/MySQL tables");

    if (auto * settings = create_query->storage->settings)
    {
        for (const auto & change : settings->changes)
        {
            if (change.name.find("cnch_vw") == 0)
            {
                auto vw_name = change.value.get<String>();
                if (vw_name == "vw_default" || vw_name == "vw_write")
                    continue;

                /// Will throw VIRTUAL_WAREHOUSE_NOT_FOUND if vw not found.
                local_context->getVirtualWarehousePool().get(vw_name);
            }
        }
    }
}

}

using TablesMeta = std::map<String, Protos::DataModelTable>;

class CnchDatabaseTablesSnapshotIterator final : public IDatabaseTablesIterator
{
private:
    ContextPtr context;
    TablesMeta tables;
    TablesMeta::iterator it;
    mutable std::list<StoragePtr> storages{nullptr};
public:
    CnchDatabaseTablesSnapshotIterator(const ContextPtr & context_, const TablesMeta & tables_, const String & database_name_)
    : IDatabaseTablesIterator(database_name_), context(context_), tables(tables_), it(tables.begin())
    {
    }

    void next() override
    {
        ++it;
        storages.emplace_back(nullptr);
    }

    bool isValid() const override { return it != tables.end(); }

    const String & name() const override { return it->first; }

    /// NOTE: may returns nullptr, users should handle this.
    const StoragePtr & table() const override
    {
        if (storages.back())
            return storages.back();

        StoragePtr current_storage;
        try
        {
            current_storage = Catalog::CatalogFactory::getTableByDataModel(context, &(it->second));
            storages.pop_back();
            storages.push_back(current_storage);
        }
        catch (...)
        {
        }
        return storages.back();
    }

    UUID uuid() const override
    {
        const StoragePtr & storage = table();
        if (storage)
            return storage->getStorageID().uuid;
        else
            return UUIDHelpers::Nil;
    }
};

// The lightweight iterator only contains informations about tables StorageID. It is used when no need to
// construct storage object.
using TablesID = std::vector<Protos::TableIdentifier>;
class CnchDatabaseTablesLightWeightIterator final : public IDatabaseTablesIterator
{
private:
    TablesID tables;
    TablesID::iterator it;

public:
    CnchDatabaseTablesLightWeightIterator(const TablesID & tables_, const String & database_name_)
    : IDatabaseTablesIterator(database_name_), tables(tables_), it(tables.begin())
    {
    }

    void next() override { it++; }

    bool isValid() const override { return it != tables.end(); }

    const String & name() const override { return it->name(); }

    const StoragePtr & table() const override
    {
        throw Exception("Shouldn't call table() in light weight iterator.", ErrorCodes::NOT_IMPLEMENTED);
    }

    UUID uuid() const override { return UUIDHelpers::toUUID(it->uuid()); }
};

DatabaseCnch::DatabaseCnch(const String & name_, UUID uuid, ContextPtr local_context)
    : IDatabase(name_)
    , WithContext(local_context->getGlobalContext())
    , log(getLogger("DatabaseCnch (" + name_ + ")"))
    , db_uuid(uuid)
{
    LOG_DEBUG(log, "Create database {} in query {}", database_name, local_context->getCurrentQueryId());
}

DatabaseCnch::DatabaseCnch(const String & name_, UUID uuid, const String & logger, ContextPtr local_context)
    : IDatabase(name_)
    , WithContext(local_context->getGlobalContext())
    , log(getLogger(logger))
    , db_uuid(uuid)
{
    LOG_DEBUG(log, "Create database {} in query {}", database_name, local_context->getCurrentQueryId());
}

void DatabaseCnch::createTable(ContextPtr local_context, const String & table_name, const StoragePtr & table, const ASTPtr & query)
{
    LOG_DEBUG(log, "Create table {} in query {}", table_name, local_context->getCurrentQueryId());
    auto txn = local_context->getCurrentTransaction();
    if (!txn)
        throw Exception("Cnch transaction is not initialized", ErrorCodes::CNCH_TRANSACTION_NOT_INITIALIZED);

    checkCreateIsAllowedInCnch(local_context, query);

    bool attach = query->as<ASTCreateQuery&>().attach;
    String statement = getObjectDefinitionFromCreateQueryForCnch(query);

    /// Cnch table should not throw exceptions during creating StoragePtr. Otherwise, it will cause problems when
    /// atempting to drop the table later.
    /// Cnch Hive table catch exception during table creation and throws exception in startup()
    table->startup();

    CreateActionParams params;
    if (table->isDictionary())
        params = CreateDictionaryParams{table->getStorageID(), statement, attach};
    else
        params = CreateTableParams{getUUID(), table->getStorageID(), statement, attach};

    auto create_table = txn->createAction<DDLCreateAction>(std::move(params));
    txn->appendAction(std::move(create_table));
    txn->commitV1();
    LOG_TRACE(log, "Successfully create table {} in query {}", table_name, local_context->getCurrentQueryId());
    if (table->isDictionary())
    {
        local_context->getExternalDictionariesLoader().reloadConfig("CnchCatalogRepository");
        LOG_TRACE(log, "Successfully add dictionary config for {}", table_name, local_context->getCurrentQueryId());
    }
}

void DatabaseCnch::dropTable(ContextPtr local_context, const String & table_name, bool no_delay)
{
    LOG_DEBUG(log, "Drop table {} in query {}, no delay {}", table_name, local_context->getCurrentQueryId(), no_delay);

    auto txn = local_context->getCurrentTransaction();
    if (!txn)
        throw Exception("Cnch transaction is not initialized", ErrorCodes::CNCH_TRANSACTION_NOT_INITIALIZED);

    DropActionParams params;
    bool is_dictionary = false;
    StoragePtr storage = local_context->getCnchCatalog()->tryGetTable(*local_context, getDatabaseName(), table_name, TxnTimestamp::maxTS());
    if (storage)
    {
        params = DropTableParams{storage, storage->commit_time, /*is_detach*/ false, getUUID()};
    }
    else if (local_context->getCnchCatalog()->isDictionaryExists(getDatabaseName(), table_name))
    {
        is_dictionary = true;
        params = DropDictionaryParams{getDatabaseName(), table_name, /*is_detach*/ false};
    }
    else
        throw Exception("Can't get storage for table " + table_name, ErrorCodes::SYSTEM_ERROR);

    auto drop_action = txn->createAction<DDLDropAction>(std::move(params));
    txn->appendAction(std::move(drop_action));
    txn->commitV1();

    // commitV1 should throw exception if commit failed
    if (auto * mv = dynamic_cast<StorageMaterializedView *>(storage.get()))
    {
        if (mv->async())
            local_context->getCnchCatalog()->cleanMvMeta(UUIDHelpers::UUIDToString(mv->getStorageID().uuid));
    }

    if (is_dictionary)
        local_context->getExternalDictionariesLoader().reloadConfig("CnchCatalogRepository");

    std::lock_guard wr{cache_mutex};
    if (cache.contains(table_name))
        cache.erase(table_name);
}

void DatabaseCnch::drop(ContextPtr local_context)
{
    LOG_DEBUG(log, "Drop database {} in query {}", database_name, local_context->getCurrentQueryId());

    auto txn = local_context->getCurrentTransaction();

    if (!txn)
        throw Exception("Cnch transaction is not initialized", ErrorCodes::CNCH_TRANSACTION_NOT_INITIALIZED);

    // get the lock of tables in current database
    std::vector<IntentLockPtr> locks;

    for (auto iterator = getTablesIteratorLightweight(getContext(), [](const String &) { return true; }); iterator->isValid(); iterator->next())
    {
        locks.emplace_back(txn->createIntentLock(IntentLock::TB_LOCK_PREFIX, database_name, iterator->name()));
    }

    for (const auto & lock : locks)
        lock->lock();

    DropActionParams params = DropDatabaseParams{getDatabaseName(), commit_time};
    auto drop_action = txn->createAction<DDLDropAction>(std::move(params));
    txn->appendAction(std::move(drop_action));
    txn->commitV1();
}

void DatabaseCnch::detachTablePermanently(ContextPtr local_context, const String & table_name)
{
    TransactionCnchPtr txn = local_context->getCurrentTransaction();

    if (!txn)
        throw Exception("Cnch transaction is not initialized", ErrorCodes::CNCH_TRANSACTION_NOT_INITIALIZED);

    /// detach table action
    DropActionParams params;
    bool is_dictionary = false;
    StoragePtr storage = local_context->getCnchCatalog()->tryGetTable(*local_context, getDatabaseName(), table_name, TxnTimestamp::maxTS());
    if (storage)
    {
        params = DropTableParams{storage, storage->commit_time, /*is_detach*/ true, getUUID()};
    }
    else if (local_context->getCnchCatalog()->isDictionaryExists(getDatabaseName(), table_name))
    {
        is_dictionary = true;
        params = DropDictionaryParams{getDatabaseName(), table_name, /*is_detach*/ true};
    }
    else
        throw Exception("Can't get storage for table " + table_name, ErrorCodes::SYSTEM_ERROR);

    auto detach_action = txn->createAction<DDLDropAction>(std::move(params));
    txn->appendAction(std::move(detach_action));
    txn->commitV1();

    if (is_dictionary)
        local_context->getExternalDictionariesLoader().reloadConfig("CnchCatalogRepository");

    std::lock_guard wr{cache_mutex};
    if (cache.contains(table_name))
        cache.erase(table_name);
}

ASTPtr DatabaseCnch::getCreateDatabaseQuery() const
{
    auto settings = getContext()->getSettingsRef();
    String query = "CREATE DATABASE " + backQuoteIfNeed(getDatabaseName())
        + (db_uuid != UUIDHelpers::Nil ? (" UUID " + quoteString(UUIDHelpers::UUIDToString(db_uuid))) : "") + " ENGINE = Cnch";
    ParserCreateQuery parser(ParserSettings::valueOf(settings));
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "", 0, settings.max_parser_depth);
    return ast;
}

bool DatabaseCnch::isTableExist(const String & name, ContextPtr local_context) const
{
    return (local_context->getCnchCatalog()->isTableExists(getDatabaseName(), name, local_context->getCurrentTransactionID().toUInt64()))
        || (local_context->getCnchCatalog()->isDictionaryExists(getDatabaseName(), name));
}

StoragePtr DatabaseCnch::tryGetTable(const String & name, ContextPtr local_context) const
{
    return tryGetTable(name, local_context, false);
}

StoragePtr DatabaseCnch::tryGetTable(const String & name, ContextPtr local_context, bool ignore_status) const
{
    try
    {
        {
            std::shared_lock rd{cache_mutex};
            auto it = cache.find(name);
            if (it != cache.end())
                return it->second;
        }
        auto res = tryGetTableImpl(name, local_context);
        if (res)
        {
            if (!res->is_detached && !res->is_dropped)
            {
                std::lock_guard wr{cache_mutex};
                cache.emplace(name, res);
                return res;
            }
            else if (ignore_status)
            {
                return res;
            }
        }
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::UNKNOWN_TABLE)
            throw;
    }
    return nullptr;
}

DatabaseTablesIteratorPtr DatabaseCnch::getTablesIterator(ContextPtr local_context, const FilterByNameFunction & filter_by_table_name)
{
    TablesMeta tables;
    std::vector<Protos::DataModelTable> table_models = local_context->getCnchCatalog()->getAllTables(database_name);

    for (auto & table_meta : table_models)
    {
        if (Status::isVisible(table_meta.status()))
            tables.emplace(table_meta.name(), std::move(table_meta));
    }

    if (!filter_by_table_name)
        return std::make_unique<CnchDatabaseTablesSnapshotIterator>(local_context, std::move(tables), database_name);

    TablesMeta filtered_tables;
    for (const auto & [table_name, table_meta] : tables)
        if (filter_by_table_name(table_name))
            filtered_tables.emplace(table_name, table_meta);

    return std::make_unique<CnchDatabaseTablesSnapshotIterator>(local_context, std::move(filtered_tables), database_name);
}

DatabaseTablesIteratorPtr DatabaseCnch::getTablesIteratorWithCommonSnapshot(ContextPtr local_context, const std::vector<Protos::DataModelTable> & snapshot)
{
    TablesMeta tables;
    for (const auto & table_meta : snapshot)
    {
        if (table_meta.database()==database_name && Status::isVisible(table_meta.status()))
            tables.emplace(table_meta.name(), table_meta);
    }
    return std::make_unique<CnchDatabaseTablesSnapshotIterator>(local_context, std::move(tables), database_name);
}

DatabaseTablesIteratorPtr DatabaseCnch::getTablesIteratorLightweight(ContextPtr local_context, const FilterByNameFunction & filter_by_table_name)
{
    TablesID tables;
    auto tables_id = local_context->getCnchCatalog()->getAllTablesID(database_name);

    for (auto & table_id : tables_id)
    {
        if (table_id->has_detached() && table_id->detached())
            continue;
        tables.push_back(std::move(*table_id));
    }

    if (!filter_by_table_name)
        return std::make_unique<CnchDatabaseTablesLightWeightIterator>(std::move(tables), database_name);

    TablesID filtered_tables;
    for (const auto & table_id : tables)
        if (filter_by_table_name(table_id.name()))
            filtered_tables.push_back(table_id);

    return std::make_unique<CnchDatabaseTablesLightWeightIterator>(std::move(filtered_tables), database_name);
}

bool DatabaseCnch::empty() const
{
    Strings tables = getContext()->getCnchCatalog()->getTablesInDB(getDatabaseName());
    return tables.empty();
}

ASTPtr DatabaseCnch::getCreateTableQueryImpl(const String & name, ContextPtr local_context, bool throw_on_error) const
{
    StoragePtr storage = getContext()->getCnchCatalog()->tryGetTable(
        *local_context, getDatabaseName(), name, local_context->getCurrentTransactionID().toUInt64());

    if (!storage)
    {
        try
        {
            return getContext()->getCnchCatalog()->getCreateDictionary(getDatabaseName(), name);
        }
        catch (...)
        {
            LOG_DEBUG(
                log,
                "Fail to try to get create query for dictionary {} in database {} query id {}",
                name,
                getDatabaseName(),
                local_context->getCurrentQueryId());
        }
    }

    if ((!storage) && throw_on_error)
        throw Exception("Table " + getDatabaseName() + "." + name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);

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
                "Fail to parseQuery for table {} in database {} query id {}, create query {}",
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

    CreateActionParams params = CreateDatabaseParams{getDatabaseName(), getUUID(), /*statement*/ "", getEngineName(), local_context->getSettingsRef().text_case_option};
    auto create_db = txn->createAction<DDLCreateAction>(std::move(params));
    txn->appendAction(std::move(create_db));
    txn->commitV1();
}

StoragePtr DatabaseCnch::tryGetTableImpl(const String & name, ContextPtr local_context) const
{
    TransactionCnchPtr cnch_txn = local_context->getCurrentTransaction();
    const TxnTimestamp & start_time = cnch_txn ? cnch_txn->getStartTime() : TxnTimestamp{local_context->getTimestamp()};

    StoragePtr storage_ptr = getContext()->getCnchCatalog()->tryGetTable(*local_context, getDatabaseName(), name, start_time);
    if (!storage_ptr)
        storage_ptr = getContext()->getCnchCatalog()->tryGetDictionary(database_name, name, local_context);

    return storage_ptr;
}

void DatabaseCnch::renameDatabase(ContextPtr local_context, const String & new_name)
{
    auto txn = local_context->getCurrentTransaction();
    RenameActionParams params = RenameDatabaseParams{getUUID(), database_name, new_name};
    auto rename = txn->createAction<DDLRenameAction>(std::move(params));
    txn->appendAction(std::move(rename));
    txn->commitV1();
}

void DatabaseCnch::renameTable(
    ContextPtr local_context,
    const String & table_name,
    IDatabase & to_database,
    const String & to_table_name,
    bool /*exchange*/,
    bool /*dictionary*/)
{
    if (to_database.getEngineName() != getEngineName())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Can only rename to database with {} engine, but got {}", getEngineName(), to_database.getEngineName());
    if (to_database.isTableExist(to_table_name, local_context))
        throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Table {}.{} already exists", to_database.getDatabaseName(), to_table_name);

    StoragePtr from_table = tryGetTableImpl(table_name, local_context);
    if (!from_table)
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist", database_name, table_name);

    std::vector<IntentLockPtr> locks;
    auto txn = local_context->getCurrentTransaction();
    if (std::make_pair(database_name, table_name)
        < std::make_pair(to_database.getDatabaseName(), to_table_name))
    {
        locks.push_back(txn->createIntentLock(IntentLock::TB_LOCK_PREFIX, database_name, from_table->getStorageID().table_name));
        locks.push_back(txn->createIntentLock(IntentLock::TB_LOCK_PREFIX, to_database.getDatabaseName(), to_table_name));
    }
    else
    {
        locks.push_back(txn->createIntentLock(IntentLock::TB_LOCK_PREFIX, to_database.getDatabaseName(), to_table_name));
        locks.push_back(txn->createIntentLock(IntentLock::TB_LOCK_PREFIX, database_name, from_table->getStorageID().table_name));
    }
    std::lock(*locks[0], *locks[1]);

    RenameActionParams params = RenameTableParams{database_name, from_table, to_database.getDatabaseName(), to_table_name, to_database.getUUID()};
    auto rename_table = txn->createAction<DDLRenameAction>(std::move(params));
    txn->appendAction(std::move(rename_table));
    /// Commit in InterpreterRenameQuery because we can rename multiple tables in a same transaction
    std::lock_guard wr{cache_mutex};
    if (cache.contains(table_name))
        cache.erase(table_name);
}

void DatabaseCnch::dropSnapshot(ContextPtr local_context, const String & snapshot_name)
{
    assertSupportSnapshot();
    auto txn = local_context->getCurrentTransaction();
    if (!txn)
        throw Exception("Cnch transaction is not initialized", ErrorCodes::CNCH_TRANSACTION_NOT_INITIALIZED);

    LOG_DEBUG(log, "Dropping snapshot {} in query {}", snapshot_name, local_context->getCurrentQueryId());

    DropActionParams params = DropSnapshotParams{snapshot_name, db_uuid};
    auto action = txn->createAction<DDLDropAction>(std::move(params));
    txn->appendAction(std::move(action));
    txn->commitV1();
}

SnapshotPtr DatabaseCnch::tryGetSnapshot(const String & snapshot_name) const
{
    assertSupportSnapshot();
    return getContext()->getCnchCatalog()->tryGetSnapshot(db_uuid, snapshot_name);
}

Snapshots DatabaseCnch::getAllSnapshots() const
{
    assertSupportSnapshot();
    return getContext()->getCnchCatalog()->getAllSnapshots(db_uuid);
}

Snapshots DatabaseCnch::getAllSnapshotsForStorage(UUID storage_uuid) const
{
    if (storage_uuid == UUIDHelpers::Nil)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "storage uuid can't be empty");
    assertSupportSnapshot();
    return getContext()->getCnchCatalog()->getAllSnapshots(db_uuid, &storage_uuid);
}
}
