/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Interpreters/loadMetadata.h>
#include <Storages/IStorage.h>
#include <Databases/IDatabase.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabaseOnDisk.h>
#include <Common/quoteString.h>
#include <Storages/StorageMemory.h>
#include <Storages/LiveView/TemporaryLiveViewCleaner.h>
#include <Core/BackgroundSchedulePool.h>
#include <Parsers/formatAST.h>
#include <IO/ReadHelpers.h>
#include <Poco/DirectoryIterator.h>
#include <Common/atomicRename.h>
#include <Common/CurrentMetrics.h>
#include <common/logger_useful.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <filesystem>
#include <memory>
#include <Common/filesystemHelpers.h>

#include <Catalog/Catalog.h>
#include <Catalog/CatalogFactory.h>
#include <CloudServices/CnchWorkerResource.h>
#include <Databases/DatabaseCnch.h>
#include <Common/Status.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/StorageCnchMergeTree.h>
#include <DataTypes/ObjectUtils.h>
#include <ExternalCatalog/IExternalCatalogMgr.h>
#include <Protos/RPCHelpers.h>
#include <Transaction/ICnchTransaction.h>
#include <Common/DefaultCatalogName.h>

#include <Common/Status.h>
#include <Core/SettingsEnums.h>
#include <Databases/DatabaseExternalHive.h>
#include <Interpreters/StorageID.h>
#include <Transaction/TxnTimestamp.h>
#include <Parsers/formatTenantDatabaseName.h>
#if !defined(ARCADIA_BUILD)
#    include <config_core.h>
#endif

#if USE_MYSQL
#    include <Databases/MySQL/MaterializeMySQLSyncThread.h>
#    include <Storages/StorageMaterializeMySQL.h>
#endif

#if USE_LIBPQXX
#    include <Storages/PostgreSQL/StorageMaterializedPostgreSQL.h>
#endif

namespace fs = std::filesystem;

namespace CurrentMetrics
{
    extern const Metric TablesToDropQueueSize;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_CATALOG;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_TABLE;
    extern const int TABLE_IS_DETACHED;
    extern const int TABLE_IS_DROPPED;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int DATABASE_ALREADY_EXISTS;
    extern const int DATABASE_NOT_EMPTY;
    extern const int DATABASE_ACCESS_DENIED;
    extern const int LOGICAL_ERROR;
}

TemporaryTableHolder::TemporaryTableHolder(ContextPtr context_, const TemporaryTableHolder::Creator & creator, const ASTPtr & query)
    : WithContext(context_->getGlobalContext())
{
    auto & database_catalog = DatabaseCatalog::instance();
    database_catalog.initializeAndLoadTemporaryDatabase();
    temporary_tables = database_catalog.getDatabaseForTemporaryTables().get();

    ASTPtr original_create;
    ASTCreateQuery * create = dynamic_cast<ASTCreateQuery *>(query.get());
    String global_name;
    if (create)
    {
        original_create = create->clone();
        if (create->uuid == UUIDHelpers::Nil)
            create->uuid = UUIDHelpers::generateV4();
        id = create->uuid;
        create->table = "_tmp_" + toString(id);
        global_name = create->table;
        create->database = DatabaseCatalog::TEMPORARY_DATABASE;
    }
    else
    {
        id = UUIDHelpers::generateV4();
        global_name = "_tmp_" + toString(id);
    }
    auto table_id = StorageID(DatabaseCatalog::TEMPORARY_DATABASE, global_name, id);
    auto table = creator(table_id);
    temporary_tables->createTable(getContext(), global_name, table, original_create);
    table->startup();
}


TemporaryTableHolder::TemporaryTableHolder(
    ContextPtr context_,
    const ColumnsDescription & columns,
    const ConstraintsDescription & constraints,
    const ForeignKeysDescription & foreign_keys_,
    const UniqueNotEnforcedDescription & unique_not_enforced_,
    const ASTPtr & query,
    bool create_for_global_subquery)
    : TemporaryTableHolder(
        context_,
        [&](const StorageID & table_id)
        {
            auto storage = StorageMemory::create(table_id, ColumnsDescription{columns}, ConstraintsDescription{constraints}, ForeignKeysDescription{foreign_keys_}, UniqueNotEnforcedDescription{unique_not_enforced_}, String{});

            if (create_for_global_subquery)
                storage->delayReadForGlobalSubqueries();

            return storage;
        },
        query)
{
}

TemporaryTableHolder::TemporaryTableHolder(TemporaryTableHolder && rhs)
        : WithContext(rhs.context), temporary_tables(rhs.temporary_tables), id(rhs.id)
{
    rhs.id = UUIDHelpers::Nil;
}

TemporaryTableHolder & TemporaryTableHolder::operator = (TemporaryTableHolder && rhs)
{
    id = rhs.id;
    rhs.id = UUIDHelpers::Nil;
    return *this;
}

TemporaryTableHolder::~TemporaryTableHolder()
{
    if (id != UUIDHelpers::Nil)
        temporary_tables->dropTable(getContext(), "_tmp_" + toString(id));
}

StorageID TemporaryTableHolder::getGlobalTableID() const
{
    return StorageID{DatabaseCatalog::TEMPORARY_DATABASE, "_tmp_" + toString(id), id};
}

StoragePtr TemporaryTableHolder::getTable() const
{
    auto table = temporary_tables->tryGetTable("_tmp_" + toString(id), getContext());
    if (!table)
        throw Exception("Temporary table " + getGlobalTableID().getNameForLogs() + " not found", ErrorCodes::LOGICAL_ERROR);
    return table;
}


void DatabaseCatalog::initializeAndLoadTemporaryDatabase()
{
    if (isDatabaseExist(TEMPORARY_DATABASE, getContext()))
        return;
    // coverity[store_truncates_time_t]
    drop_delay_sec = getContext()->getConfigRef().getInt("database_atomic_delay_before_drop_table_sec", default_drop_delay_sec);

    auto db_for_temporary_and_external_tables = std::make_shared<DatabaseMemory>(TEMPORARY_DATABASE, getContext());
    attachDatabase(TEMPORARY_DATABASE, db_for_temporary_and_external_tables);
}

void DatabaseCatalog::loadDatabases()
{
    loadMarkedAsDroppedTables();
    auto task_holder = getContext()->getSchedulePool().createTask("DatabaseCatalog", [this](){ this->dropTableDataTask(); });
    drop_task = std::make_unique<BackgroundSchedulePoolTaskHolder>(std::move(task_holder));
    (*drop_task)->activate();
    std::lock_guard lock{tables_marked_dropped_mutex};
    if (!tables_marked_dropped.empty())
        (*drop_task)->schedule();

    /// Another background thread which drops temporary LiveViews.
    /// We should start it after loadMarkedAsDroppedTables() to avoid race condition.
    TemporaryLiveViewCleaner::instance().startup();
}

void DatabaseCatalog::shutdownImpl()
{
    TemporaryLiveViewCleaner::shutdown();

    if (drop_task)
        (*drop_task)->deactivate();

    /** At this point, some tables may have threads that block our mutex.
      * To shutdown them correctly, we will copy the current list of tables,
      *  and ask them all to finish their work.
      * Then delete all objects with tables.
      */

    Databases current_databases;
    {
        std::lock_guard lock(databases_mutex);
        current_databases = databases;
    }

    /// We still hold "databases" (instead of std::move) for Buffer tables to flush data correctly.

    for (auto & database : current_databases)
        database.second->shutdown();

    tables_marked_dropped.clear();

    std::lock_guard lock(databases_mutex);
    assert(std::find_if(uuid_map.begin(), uuid_map.end(), [](const auto & elem)
    {
        /// Ensure that all UUID mappings are empty (i.e. all mappings contain nullptr instead of a pointer to storage)
        const auto & not_empty_mapping = [] (const auto & mapping)
        {
            auto & table = mapping.second.second;
            return table;
        };
        auto it = std::find_if(elem.map.begin(), elem.map.end(), not_empty_mapping);
        return it != elem.map.end();
    }) == uuid_map.end());
    databases.clear();
    db_uuid_map.clear();
    view_dependencies.clear();
}

DatabaseAndTable DatabaseCatalog::tryGetByUUID(const UUID & uuid, const ContextPtr & local_context) const
{
    if (preferCnchCatalog(*local_context) && local_context->getCurrentTransaction())
    {
        StoragePtr storage = getContext()->getCnchCatalog()->tryGetTableByUUID(*local_context, UUIDHelpers::UUIDToString(uuid), local_context->getCurrentTransactionID().toUInt64(), false);
        if (storage && !storage->is_dropped && !storage->is_detached)
        {
            DatabasePtr database_cnch = tryGetDatabaseCnch(storage->getDatabaseName(), local_context);
            if (!database_cnch)
                throw Exception("Database " + backQuoteIfNeed(storage->getDatabaseName()) + " is not exists as DatabaseCnch.", ErrorCodes::UNKNOWN_DATABASE);
            return std::make_pair(std::move(database_cnch), std::move(storage));
        }
    }

    assert(uuid != UUIDHelpers::Nil && getFirstLevelIdx(uuid) < uuid_map.size());
    const UUIDToStorageMapPart & map_part = uuid_map[getFirstLevelIdx(uuid)];
    std::lock_guard lock{map_part.mutex};
    auto it = map_part.map.find(uuid);
    if (it == map_part.map.end())
        return {};
    return it->second;
}


DatabaseAndTable DatabaseCatalog::getTableImpl(
    const StorageID & table_id,
    ContextPtr context_,
    std::optional<Exception> * exception) const
{
    if (!table_id)
    {
        if (exception)
            exception->emplace(ErrorCodes::UNKNOWN_TABLE, "Cannot find table: StorageID is empty");
        return {};
    }

    if (context_->getServerType() == ServerType::cnch_worker)
    {
        if (auto worker_resource = context_->tryGetCnchWorkerResource())
        {
            if (auto table = worker_resource->getTable(table_id))
            {
                LOG_INFO(log, "got table {} from worker resource", table_id.getNameForLogs());
                return {nullptr, table};
            }
        }
    }

    auto aeolus_check = [&table_id, &context_](const StoragePtr & storage)
    {
        // check aeolus table access before return required storage.
        if (context_->getServerType() != ServerType::cnch_server)
            return;

        if (!storage || storage->getName() == "MaterializedView")
            return;

        context_->checkAeolusTableAccess(table_id.database_name, table_id.table_name);
    };

    if (table_id.hasUUID() && table_id.database_name == TEMPORARY_DATABASE)
    {
        /// Shortcut for tables which have persistent UUID
        auto db_and_table = tryGetByUUID(table_id.uuid, context_);
        if (!db_and_table.first || !db_and_table.second)
        {
            assert(!db_and_table.first && !db_and_table.second);
            if (exception)
                exception->emplace(ErrorCodes::UNKNOWN_TABLE, "Table {} with uuid: {} doesn't exist", table_id.getNameForLogs(),
                    UUIDHelpers::UUIDToString(table_id.uuid));
            return {};
        }

#if USE_LIBPQXX
        if (!context_->isInternalQuery() && (db_and_table.first->getEngineName() == "MaterializedPostgreSQL"))
        {
            db_and_table.second = std::make_shared<StorageMaterializedPostgreSQL>(std::move(db_and_table.second), getContext());
        }
#endif

#if USE_MYSQL
        /// It's definitely not the best place for this logic, but behaviour must be consistent with DatabaseMaterializeMySQL::tryGetTable(...)
        if (db_and_table.first->getEngineName() == "CnchMaterializedMySQL" || db_and_table.first->getEngineName() == "CloudMaterializedMySQL")
        {
            if (!MaterializeMySQLSyncThread::isMySQLSyncThread() && !context_->getSettingsRef().force_manipulate_materialized_mysql_table)
               db_and_table.second = std::make_shared<StorageMaterializeMySQL>(std::move(db_and_table.second), db_and_table.first.get());
        }
#endif

        aeolus_check(db_and_table.second);
        return db_and_table;
    }


    if (table_id.database_name == TEMPORARY_DATABASE)
    {
        /// For temporary tables UUIDs are set in Context::resolveStorageID(...).
        /// If table_id has no UUID, then the name of database was specified by user and table_id was not resolved through context.
        /// Do not allow access to TEMPORARY_DATABASE because it contains all temporary tables of all contexts and users.
        if (exception)
            exception->emplace(ErrorCodes::DATABASE_ACCESS_DENIED, "Direct access to `{}` database is not allowed", String(TEMPORARY_DATABASE));
        return {};
    }

    /// when worker have cnch table is worker resource try to get table from cnch
    bool is_worker_cnch_table = context_->getServerType() == ServerType::cnch_worker && context_->tryGetCnchWorkerResource()
        && context_->tryGetCnchWorkerResource()->isCnchTableInWorker(table_id);
    DatabasePtr database{};
    if (preferCnchCatalog(*context_) || is_worker_cnch_table)
        database = tryGetDatabaseCnch(table_id.getDatabaseName(), context_);

    if (!database)
    {
        String tenant_db = formatTenantDatabaseName(table_id.getDatabaseName());
        std::lock_guard lock{databases_mutex};
        auto it = databases.find(tenant_db);
        if (databases.end() == it)
        {
            if (exception)
                exception->emplace(
                    ErrorCodes::UNKNOWN_DATABASE,
                    "Database {} doesn't exist when fetching {}",
                    backQuoteIfNeed(table_id.getDatabaseName()),
                    table_id.getNameForLogs());
            return {};
        }
        database = it->second;
    }

    StoragePtr table = database->tryGetTable(table_id.table_name, context_, true);
    if (exception)
    {
        if (!table)
            exception->emplace(ErrorCodes::UNKNOWN_TABLE, "Table {} doesn't exist in {}, database engine type: {}", table_id.getNameForLogs(), database->getDatabaseName(), database->getEngineName());
        else if (table->is_detached)
            exception->emplace(ErrorCodes::TABLE_IS_DETACHED, "Table {} is detached in {}, database engine type: {}", table_id.getNameForLogs(), database->getDatabaseName(), database->getEngineName());
        else if (table->is_dropped)
            exception->emplace(ErrorCodes::TABLE_IS_DROPPED, "Table {} is dropped in {}, database engine type: {}", table_id.getNameForLogs(), database->getDatabaseName(), database->getEngineName());
    }
    if (!table || table->is_detached || table->is_dropped)
    {
        table = nullptr;
        database = nullptr;
    }

    if (table && table->getInMemoryMetadataPtr()->hasDynamicSubcolumns())
    {
        if (auto cnch_table = std::dynamic_pointer_cast<StorageCnchMergeTree>(table))
            cnch_table->resetObjectColumns(context_);
    }

    aeolus_check(table);
    return {database, table};
}

void DatabaseCatalog::assertDatabaseExists(const String & database_name, ContextPtr local_context) const
{
    if (preferCnchCatalog(*local_context))
    {
        DatabasePtr database_cnch = tryGetDatabaseCnch(database_name, local_context);
        if (database_cnch)
            return;
    }

    {
        String tenant_db = formatTenantDatabaseName(database_name);
        std::lock_guard lock{databases_mutex};
        assertDatabaseExistsUnlocked(tenant_db);
    }
}

void DatabaseCatalog::assertDatabaseDoesntExist(const String & database_name, ContextPtr local_context) const
{
    {
        String tenant_db = formatTenantDatabaseName(database_name);
        std::lock_guard lock{databases_mutex};
        assertDatabaseDoesntExistUnlocked(tenant_db);
    }

    if (preferCnchCatalog(*local_context))
    {
        DatabasePtr database_cnch = tryGetDatabaseCnch(database_name);
        if (database_cnch)
            throw Exception("Database " + backQuoteIfNeed(database_name) + " already exists as DatabaseCnch.", ErrorCodes::DATABASE_ALREADY_EXISTS);
    }
}

void DatabaseCatalog::assertDatabaseExistsUnlocked(const String & database_name) const
{
    assert(!database_name.empty());
    if (databases.end() == databases.find(database_name))
        throw Exception("Database " + backQuoteIfNeed(database_name) + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
}


void DatabaseCatalog::assertDatabaseDoesntExistUnlocked(const String & database_name) const
{
    assert(!database_name.empty());
    if (databases.end() != databases.find(database_name))
        throw Exception("Database " + backQuoteIfNeed(database_name) + " already exists.", ErrorCodes::DATABASE_ALREADY_EXISTS);
}

void DatabaseCatalog::attachDatabase(const String & database_name, const DatabasePtr & database)
{
    // TEMPORARY_DATABASE is stored in DatabaseCatalog becase it is tiny
    if ((database->getEngineName() == "Cnch" || database->getEngineName() == "CnchMaterializedMySQL") && database_name != TEMPORARY_DATABASE)
        return;
    String tenant_db = formatTenantDatabaseName(database_name);
    std::lock_guard lock{databases_mutex};
    assertDatabaseDoesntExistUnlocked(tenant_db);
    databases.emplace(tenant_db, database);
    UUID db_uuid = database->getUUID();
    if (db_uuid != UUIDHelpers::Nil)
        db_uuid_map.emplace(db_uuid, database);
}


DatabasePtr DatabaseCatalog::detachDatabase(ContextPtr local_context, const String & database_name, bool drop, bool check_empty)
{
    if (database_name == TEMPORARY_DATABASE)
        throw Exception("Cannot detach database with temporary tables.", ErrorCodes::DATABASE_ACCESS_DENIED);

    DatabasePtr db{};

    if (preferCnchCatalog(*local_context))
        db = tryGetDatabaseCnch(database_name, local_context);

    String tenant_db = formatTenantDatabaseName(database_name);
    if (!db)
    {
        std::lock_guard lock{databases_mutex};
        assertDatabaseExistsUnlocked(tenant_db);
        db = databases.find(tenant_db)->second;
        db_uuid_map.erase(db->getUUID());
        databases.erase(tenant_db);
    }

    if (check_empty)
    {
        try
        {
            if (!db->empty())
                throw Exception("New table appeared in database being dropped or detached. Try again.",
                                ErrorCodes::DATABASE_NOT_EMPTY);
            if (!drop)
                db->assertCanBeDetached(false);
        }
        catch (...)
        {
            attachDatabase(tenant_db, db);
            throw;
        }
    }

    db->shutdown();

    if (drop)
    {
        /// Delete the database.
        db->drop(local_context);

        /// Old ClickHouse versions did not store database.sql files
        fs::path database_metadata_file = fs::path(getContext()->getPath()) / "metadata" / (escapeForFileName(tenant_db) + ".sql");
        if (fs::exists(database_metadata_file))
            fs::remove(database_metadata_file);
    }

    return db;
}

void DatabaseCatalog::updateDatabaseName(const String & old_name, const String & new_name)
{
    std::lock_guard lock{databases_mutex};
    assert(databases.find(new_name) == databases.end());
    auto it = databases.find(old_name);
    assert(it != databases.end());
    auto db = it->second;
    databases.erase(it);
    databases.emplace(new_name, db);
}

DatabasePtr DatabaseCatalog::getDatabase(const String & database_name) const
{
    String tenant_db = formatTenantDatabaseName(database_name);
    std::lock_guard lock{databases_mutex};
    assertDatabaseExistsUnlocked(tenant_db);
    return databases.find(tenant_db)->second;
}

DatabasePtr DatabaseCatalog::tryGetDatabase(const String & database_name, ContextPtr local_context) const
{
    assert(!database_name.empty());

    if (preferCnchCatalog(*local_context))
    {
        DatabasePtr database_cnch = tryGetDatabaseCnch(database_name, local_context);
        if (database_cnch)
            return database_cnch;
    }

    String tenant_db = formatTenantDatabaseName(database_name);
    std::lock_guard lock{databases_mutex};
    auto it = databases.find(tenant_db);
    if (it == databases.end())
        return {};
    return it->second;
}

DatabasePtr DatabaseCatalog::tryGetDatabase(const String & database_name) const
{
    assert(!database_name.empty());

    String tenant_db = formatTenantDatabaseName(database_name);
    std::lock_guard lock{databases_mutex};
    auto it = databases.find(tenant_db);
    if (it == databases.end())
        return {};
    return it->second;
}

DatabasePtr DatabaseCatalog::getDatabase(const UUID & uuid) const
{
    std::lock_guard lock{databases_mutex};
    auto it = db_uuid_map.find(uuid);
    if (it == db_uuid_map.end())
        throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database UUID {} does not exist", toString(uuid));
    return it->second;
}

DatabasePtr DatabaseCatalog::tryGetDatabase(const UUID & uuid) const
{
    assert(uuid != UUIDHelpers::Nil);
    std::lock_guard lock{databases_mutex};
    auto it = db_uuid_map.find(uuid);
    if (it == db_uuid_map.end())
        return {};
    return it->second;
}

bool DatabaseCatalog::isDatabaseExist(const String & database_name, ContextPtr local_context) const
{
    assert(!database_name.empty());

    if (preferCnchCatalog(*local_context))
    {
        DatabasePtr cnch_database = tryGetDatabaseCnch(database_name);
        if (cnch_database)
            return true;
    }

    String tenant_db = formatTenantDatabaseName(database_name);
    std::lock_guard lock{databases_mutex};
    return databases.end() != databases.find(tenant_db);
}

Databases DatabaseCatalog::getDatabases(ContextPtr local_context) const
{
    Databases res;
    {
        std::lock_guard lock{databases_mutex};
        res = databases;
    }

    if (preferCnchCatalog(*local_context))
    {
        Databases cnch_databases = getDatabaseCnchs();
        std::move(cnch_databases.begin(), cnch_databases.end(), std::inserter(res, res.end()));
    }
    return res;
}

Databases DatabaseCatalog::getNonCnchDatabases() const
{
    std::lock_guard lock{databases_mutex};
    return databases;
}

bool DatabaseCatalog::isTableExist(const DB::StorageID & table_id, ContextPtr context_) const
{
    if (table_id.hasUUID())
        return tryGetByUUID(table_id.uuid, context_).second != nullptr;

    DatabasePtr db;
    {
        String tenant_db = formatTenantDatabaseName(table_id.database_name);
        std::lock_guard lock{databases_mutex};
        auto iter = databases.find(tenant_db);
        if (iter != databases.end())
            db = iter->second;
    }
    return db && db->isTableExist(table_id.table_name, context_);
}

void DatabaseCatalog::assertTableDoesntExist(const StorageID & table_id, ContextPtr context_) const
{
    if (isTableExist(table_id, context_))
        throw Exception("Table " + table_id.getNameForLogs() + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
}

DatabasePtr DatabaseCatalog::getDatabaseForTemporaryTables() const
{
    return getDatabase(TEMPORARY_DATABASE);
}

DatabasePtr DatabaseCatalog::getSystemDatabase() const
{
    return getDatabase(SYSTEM_DATABASE);
}

void DatabaseCatalog::addUUIDMapping(const UUID & uuid)
{
    addUUIDMapping(uuid, nullptr, nullptr);
}

void DatabaseCatalog::addUUIDMapping(const UUID & uuid, const DatabasePtr & database, const StoragePtr & table)
{
    assert(uuid != UUIDHelpers::Nil && getFirstLevelIdx(uuid) < uuid_map.size());
    assert((database && table) || (!database && !table));
    UUIDToStorageMapPart & map_part = uuid_map[getFirstLevelIdx(uuid)];
    std::lock_guard lock{map_part.mutex};
    auto [it, inserted] = map_part.map.try_emplace(uuid, database, table);
    if (inserted)
        return;

    auto & prev_database = it->second.first;
    auto & prev_table = it->second.second;
    assert((prev_database && prev_table) || (!prev_database && !prev_table));

    if (!prev_table && table)
    {
        /// It's empty mapping, it was created to "lock" UUID and prevent collision. Just update it.
        prev_database = database;
        prev_table = table;
        return;
    }

    /// We are trying to replace existing mapping (prev_table != nullptr), it's logical error
    if (table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapping for table with UUID={} already exists", toString(uuid));
    /// Normally this should never happen, but it's possible when the same UUIDs are explicitly specified in different CREATE queries,
    /// so it's not LOGICAL_ERROR
    throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Mapping for table with UUID={} already exists. It happened due to UUID collision, "
                    "most likely because some not random UUIDs were manually specified in CREATE queries.", toString(uuid));
}

void DatabaseCatalog::removeUUIDMapping(const UUID & uuid)
{
    assert(uuid != UUIDHelpers::Nil && getFirstLevelIdx(uuid) < uuid_map.size());
    UUIDToStorageMapPart & map_part = uuid_map[getFirstLevelIdx(uuid)];
    std::lock_guard lock{map_part.mutex};
    auto it = map_part.map.find(uuid);
    if (it == map_part.map.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapping for table with UUID={} doesn't exist", toString(uuid));
    it->second = {};
}

void DatabaseCatalog::removeUUIDMappingFinally(const UUID & uuid)
{
    assert(uuid != UUIDHelpers::Nil && getFirstLevelIdx(uuid) < uuid_map.size());
    UUIDToStorageMapPart & map_part = uuid_map[getFirstLevelIdx(uuid)];
    std::lock_guard lock{map_part.mutex};
    if (!map_part.map.erase(uuid))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapping for table with UUID={} doesn't exist", toString(uuid));
}

void DatabaseCatalog::updateUUIDMapping(const UUID & uuid, DatabasePtr database, StoragePtr table)
{
    assert(uuid != UUIDHelpers::Nil && getFirstLevelIdx(uuid) < uuid_map.size());
    assert(database && table);
    UUIDToStorageMapPart & map_part = uuid_map[getFirstLevelIdx(uuid)];
    std::lock_guard lock{map_part.mutex};
    auto it = map_part.map.find(uuid);
    if (it == map_part.map.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapping for table with UUID={} doesn't exist", toString(uuid));
    auto & prev_database = it->second.first;
    auto & prev_table = it->second.second;
    assert(prev_database && prev_table);
    prev_database = std::move(database);
    prev_table = std::move(table);
}

std::unique_ptr<DatabaseCatalog> DatabaseCatalog::database_catalog;

DatabaseCatalog::DatabaseCatalog(ContextMutablePtr global_context_)
    : WithMutableContext(global_context_)
    , view_dependencies("DatabaseCatalog")
    , log(&Poco::Logger::get("DatabaseCatalog"))
    , use_cnch_catalog{global_context_->getServerType() == ServerType::cnch_server}
{
    TemporaryLiveViewCleaner::init(global_context_);
}

DatabaseCatalog & DatabaseCatalog::init(ContextMutablePtr global_context_)
{
    if (database_catalog)
    {
        throw Exception("Database catalog is initialized twice. This is a bug.",
            ErrorCodes::LOGICAL_ERROR);
    }

    database_catalog.reset(new DatabaseCatalog(global_context_));

    return *database_catalog;
}

DatabaseCatalog & DatabaseCatalog::instance()
{
    if (!database_catalog)
    {
        throw Exception("Database catalog is not initialized. This is a bug.",
            ErrorCodes::LOGICAL_ERROR);
    }

    return *database_catalog;
}

void DatabaseCatalog::shutdown()
{
    // The catalog might not be initialized yet by init(global_context). It can
    // happen if some exception was thrown on first steps of startup.
    if (database_catalog)
    {
        database_catalog->shutdownImpl();
    }
}

DatabasePtr DatabaseCatalog::getDatabase(const String & database_name, ContextPtr local_context) const
{
    String resolved_database = local_context->resolveDatabase(database_name);

    if (local_context->getServerType() == ServerType::cnch_worker)
    {
        if (auto worker_resource = local_context->tryGetCnchWorkerResource())
        {
            if (auto database = worker_resource->getDatabase(resolved_database))
                return database;
        }
    }

    if (preferCnchCatalog(*local_context))
    {
        DatabasePtr res = tryGetDatabaseCnch(database_name, local_context);
        if (res)
            return res;
    }


    return getDatabase(resolved_database);
}

void DatabaseCatalog::addDependency(const StorageID & from, const StorageID & where)
{
    String from_tenant_db = formatTenantDatabaseName(from.database_name);
    StorageID tenant_where = StorageID {formatTenantDatabaseName(where.database_name), where.table_name, where.uuid};

    std::lock_guard lock{databases_mutex};
    // FIXME when loading metadata storage may not know UUIDs of it's dependencies, because they are not loaded yet,
    // so UUID of `from` is not used here. (same for remove, get and update)
    view_dependencies.addDependency(StorageID{from_tenant_db, from.getTableName()}, tenant_where);
}

void DatabaseCatalog::removeDependency(const StorageID & from, const StorageID & where)
{
    String from_tenant_db = formatTenantDatabaseName(from.database_name);
    StorageID tenant_where = StorageID {formatTenantDatabaseName(where.database_name), where.table_name, where.uuid};

    std::lock_guard lock{databases_mutex};
    view_dependencies.removeDependency(StorageID{from_tenant_db, from.getTableName()}, tenant_where);
}

Dependencies DatabaseCatalog::getDependencies(const StorageID & from) const
{
    String from_tenant_db = formatTenantDatabaseName(from.database_name);
    std::lock_guard lock{databases_mutex};
    return view_dependencies.getDependencies(StorageID{from_tenant_db, from.getTableName()});
}

void DatabaseCatalog::updateDependency(const StorageID & old_from, const StorageID & old_where, const StorageID & new_from,
                                  const StorageID & new_where)
{
    StorageID tenant_old_where = StorageID {formatTenantDatabaseName(old_where.database_name), old_where.table_name, old_where.uuid};
    StorageID tenant_new_where = StorageID {formatTenantDatabaseName(new_where.database_name), new_where.table_name, new_where.uuid};
    std::lock_guard lock{databases_mutex};
    if (!old_from.empty())
        view_dependencies.removeDependency(StorageID{formatTenantDatabaseName(old_from.database_name), old_from.getTableName()}, tenant_old_where);
    if (!new_from.empty())
        view_dependencies.addDependency(StorageID{formatTenantDatabaseName(new_from.database_name), new_from.getTableName()}, tenant_new_where);
}

DDLGuardPtr DatabaseCatalog::getDDLGuard(const String & database, const String & table)
{
    String tenant_db = formatTenantDatabaseName(database);
    std::unique_lock lock(ddl_guards_mutex);
    auto db_guard_iter = ddl_guards.try_emplace(tenant_db).first;
    DatabaseGuard & db_guard = db_guard_iter->second;
    return std::make_unique<DDLGuard>(db_guard.first, db_guard.second, std::move(lock), table, tenant_db);
}

std::unique_lock<std::shared_mutex> DatabaseCatalog::getExclusiveDDLGuardForDatabase(const String & database)
{
    String tenant_db = formatTenantDatabaseName(database);
    DDLGuards::iterator db_guard_iter;
    {
        std::unique_lock lock(ddl_guards_mutex);
        db_guard_iter = ddl_guards.try_emplace(tenant_db).first;
        assert(db_guard_iter->second.first.count(""));
    }
    DatabaseGuard & db_guard = db_guard_iter->second;
    return std::unique_lock{db_guard.second};
}

bool DatabaseCatalog::isDictionaryExist(const StorageID & table_id) const
{
    auto storage = tryGetTable(table_id, getContext());
    bool storage_is_dictionary = storage && storage->isDictionary();

    return storage_is_dictionary;
}

StoragePtr DatabaseCatalog::getTable(const StorageID & table_id, ContextPtr local_context) const
{
    std::optional<Exception> exc;
    auto res = getTableImpl(table_id, local_context, &exc);
    if (!res.second)
        throw Exception(*exc);
    return res.second;
}

StoragePtr DatabaseCatalog::tryGetTable(const StorageID & table_id, ContextPtr local_context) const
{
    return getTableImpl(table_id, local_context, nullptr).second;
}

DatabaseAndTable DatabaseCatalog::getDatabaseAndTable(const StorageID & table_id, ContextPtr local_context) const
{
    std::optional<Exception> exc;
    auto res = getTableImpl(table_id, local_context, &exc);
    if (!res.second)
        throw Exception(*exc);
    return res;
}

DatabaseAndTable DatabaseCatalog::tryGetDatabaseAndTable(const StorageID & table_id, ContextPtr local_context) const
{
    return getTableImpl(table_id, local_context, nullptr);
}

void DatabaseCatalog::loadMarkedAsDroppedTables()
{
    /// /clickhouse_root/metadata_dropped/ contains files with metadata of tables,
    /// which where marked as dropped by Atomic databases.
    /// Data directories of such tables still exists in store/
    /// and metadata still exists in ZooKeeper for ReplicatedMergeTree tables.
    /// If server restarts before such tables was completely dropped,
    /// we should load them and enqueue cleanup to remove data from store/ and metadata from ZooKeeper

    std::map<String, StorageID> dropped_metadata;
    String path = getContext()->getPath() + "metadata_dropped/";

    if (!std::filesystem::exists(path))
    {
        return;
    }

    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator it(path); it != dir_end; ++it)
    {
        /// File name has the following format:
        /// database_name.table_name.uuid.sql

        /// Ignore unexpected files
        if (!it.name().ends_with(".sql"))
            continue;

        /// Process .sql files with metadata of tables which were marked as dropped
        StorageID dropped_id = StorageID::createEmpty();
        size_t dot_pos = it.name().find('.');
        if (dot_pos == std::string::npos)
            continue;
        dropped_id.database_name = unescapeForFileName(it.name().substr(0, dot_pos));

        size_t prev_dot_pos = dot_pos;
        dot_pos = it.name().find('.', prev_dot_pos + 1);
        if (dot_pos == std::string::npos)
            continue;
        dropped_id.table_name = unescapeForFileName(it.name().substr(prev_dot_pos + 1, dot_pos - prev_dot_pos - 1));

        prev_dot_pos = dot_pos;
        dot_pos = it.name().find('.', prev_dot_pos + 1);
        if (dot_pos == std::string::npos)
            continue;
        dropped_id.uuid = parse<UUID>(it.name().substr(prev_dot_pos + 1, dot_pos - prev_dot_pos - 1));

        String full_path = path + it.name();
        dropped_metadata.emplace(std::move(full_path), std::move(dropped_id));
    }

    LOG_INFO(log, "Found {} partially dropped tables. Will load them and retry removal.", dropped_metadata.size());

    ThreadPool pool;
    for (const auto & elem : dropped_metadata)
    {
        pool.scheduleOrThrowOnError([&]()
        {
            this->enqueueDroppedTableCleanup(elem.second, nullptr, elem.first);
        });
    }
    pool.wait();
}

String DatabaseCatalog::getPathForDroppedMetadata(const StorageID & table_id) const
{
    return getContext()->getPath() + "metadata_dropped/" +
           escapeForFileName(formatTenantDatabaseName(table_id.getDatabaseName())) + "." +
           escapeForFileName(table_id.getTableName()) + "." +
           toString(table_id.uuid) + ".sql";
}

void DatabaseCatalog::enqueueDroppedTableCleanup(StorageID table_id, StoragePtr table, String dropped_metadata_path, bool ignore_delay)
{
    assert(table_id.hasUUID());
    assert(!table || table->getStorageID().uuid == table_id.uuid);
    assert(dropped_metadata_path == getPathForDroppedMetadata(table_id));

    /// Table was removed from database. Enqueue removal of its data from disk.
    time_t drop_time;
    if (table)
    {
        drop_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        table->is_dropped = true;
    }
    else
    {
        /// Try load table from metadata to drop it correctly (e.g. remove metadata from zk or remove data from all volumes)
        LOG_INFO(log, "Trying load partially dropped table {} from {}", table_id.getNameForLogs(), dropped_metadata_path);
        ASTPtr ast = DatabaseOnDisk::parseQueryFromMetadata(
            log, getContext(), dropped_metadata_path, /*throw_on_error*/ false, /*remove_empty*/ false);
        auto * create = typeid_cast<ASTCreateQuery *>(ast.get());
        assert(!create || create->uuid == table_id.uuid);

        if (create)
        {
            String data_path = "store/" + getPathForUUID(table_id.uuid);
            create->database = table_id.database_name;
            create->table = table_id.table_name;
            try
            {
                table = createTableFromAST(*create, table_id.getDatabaseName(), data_path, getContext(), false).second;
                table->is_dropped = true;
            }
            catch (...)
            {
                tryLogCurrentException(log, "Cannot load partially dropped table " + table_id.getNameForLogs() +
                                            " from: " + dropped_metadata_path +
                                            ". Parsed query: " + serializeAST(*create) +
                                            ". Will remove metadata and " + data_path +
                                            ". Garbage may be left in ZooKeeper.");
            }
        }
        else
        {
            LOG_WARNING(log, "Cannot parse metadata of partially dropped table {} from {}. Will remove metadata file and data directory. Garbage may be left in /store directory and ZooKeeper.", table_id.getNameForLogs(), dropped_metadata_path);
        }

        addUUIDMapping(table_id.uuid);
        drop_time = FS::getModificationTime(dropped_metadata_path);
    }

    std::lock_guard lock(tables_marked_dropped_mutex);
    if (ignore_delay)
        tables_marked_dropped.push_front({table_id, table, dropped_metadata_path, drop_time});
    else
        tables_marked_dropped.push_back({table_id, table, dropped_metadata_path, drop_time + drop_delay_sec});
    tables_marked_dropped_ids.insert(table_id.uuid);
    CurrentMetrics::add(CurrentMetrics::TablesToDropQueueSize, 1);

    /// If list of dropped tables was empty, start a drop task.
    /// If ignore_delay is set, schedule drop task as soon as possible.
    if (drop_task && (tables_marked_dropped.size() == 1 || ignore_delay))
        (*drop_task)->schedule();
}

void DatabaseCatalog::dropTableDataTask()
{
    /// Background task that removes data of tables which were marked as dropped by Atomic databases.
    /// Table can be removed when it's not used by queries and drop_delay_sec elapsed since it was marked as dropped.

    bool need_reschedule = true;
    /// Default reschedule time for the case when we are waiting for reference count to become 1.
    size_t schedule_after_ms = reschedule_time_ms;
    TableMarkedAsDropped table;
    try
    {
        std::lock_guard lock(tables_marked_dropped_mutex);
        assert(!tables_marked_dropped.empty());
        time_t current_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        time_t min_drop_time = std::numeric_limits<time_t>::max();
        size_t tables_in_use_count = 0;
        auto it = std::find_if(tables_marked_dropped.begin(), tables_marked_dropped.end(), [&](const auto & elem)
        {
            bool not_in_use = !elem.table || elem.table.unique();
            bool old_enough = elem.drop_time <= current_time;
            min_drop_time = std::min(min_drop_time, elem.drop_time);
            tables_in_use_count += !not_in_use;
            return not_in_use && old_enough;
        });
        if (it != tables_marked_dropped.end())
        {
            table = std::move(*it);
            LOG_INFO(log, "Have {} tables in drop queue ({} of them are in use), will try drop {}",
                     tables_marked_dropped.size(), tables_in_use_count, table.table_id.getNameForLogs());
            tables_marked_dropped.erase(it);
            /// Schedule the task as soon as possible, while there are suitable tables to drop.
            schedule_after_ms = 0;
        }
        else if (current_time < min_drop_time)
        {
            /// We are waiting for drop_delay_sec to exceed, no sense to wakeup until min_drop_time.
            /// If new table is added to the queue with ignore_delay flag, schedule() is called to wakeup the task earlier.
            schedule_after_ms = (min_drop_time - current_time) * 1000;
            LOG_TRACE(log, "Not found any suitable tables to drop, still have {} tables in drop queue ({} of them are in use). "
                           "Will check again after {} seconds", tables_marked_dropped.size(), tables_in_use_count, min_drop_time - current_time);
        }
        need_reschedule = !tables_marked_dropped.empty();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    if (table.table_id)
    {

        try
        {
            dropTableFinally(table);
            std::lock_guard lock(tables_marked_dropped_mutex);
            [[maybe_unused]] auto removed = tables_marked_dropped_ids.erase(table.table_id.uuid);
            assert(removed);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Cannot drop table " + table.table_id.getNameForLogs() +
                                        ". Will retry later.");
            {
                table.drop_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()) + drop_error_cooldown_sec;
                std::lock_guard lock(tables_marked_dropped_mutex);
                tables_marked_dropped.emplace_back(std::move(table));
                /// If list of dropped tables was empty, schedule a task to retry deletion.
                if (tables_marked_dropped.size() == 1)
                {
                    need_reschedule = true;
                    schedule_after_ms = drop_error_cooldown_sec * 1000;
                }
            }
        }

        wait_table_finally_dropped.notify_all();
    }

    /// Do not schedule a task if there is no tables to drop
    if (need_reschedule)
        (*drop_task)->scheduleAfter(schedule_after_ms);
}

void DatabaseCatalog::dropTableFinally(const TableMarkedAsDropped & table)
{
    if (table.table)
    {
        table.table->drop();
    }

    /// Even if table is not loaded, try remove its data from disk.
    /// TODO remove data from all volumes
    fs::path data_path = fs::path(getContext()->getPath()) / "store" / getPathForUUID(table.table_id.uuid);
    if (fs::exists(data_path))
    {
        LOG_INFO(log, "Removing data directory {} of dropped table {}", data_path.string(), table.table_id.getNameForLogs());
        fs::remove_all(data_path);
    }

    LOG_INFO(log, "Removing metadata {} of dropped table {}", table.metadata_path, table.table_id.getNameForLogs());
    fs::remove(fs::path(table.metadata_path));

    removeUUIDMappingFinally(table.table_id.uuid);
    CurrentMetrics::sub(CurrentMetrics::TablesToDropQueueSize, 1);
}

String DatabaseCatalog::getPathForUUID(const UUID & uuid)
{
    const size_t uuid_prefix_len = 3;
    return toString(uuid).substr(0, uuid_prefix_len) + '/' + toString(uuid) + '/';
}

void DatabaseCatalog::waitTableFinallyDropped(const UUID & uuid)
{
    if (uuid == UUIDHelpers::Nil)
        return;

    LOG_DEBUG(log, "Waiting for table {} to be finally dropped", toString(uuid));
    std::unique_lock lock{tables_marked_dropped_mutex};
    wait_table_finally_dropped.wait(lock, [&]()
    {
        return tables_marked_dropped_ids.count(uuid) == 0;
    });
}

bool DatabaseCatalog::isDefaultVisibleSystemDatabase(const String & database_name)
{
    return database_name == SYSTEM_DATABASE
        || database_name == INFORMATION_SCHEMA
        || database_name == INFORMATION_SCHEMA_UPPERCASE
        || database_name == MYSQL
        || database_name == MYSQL_UPPERCASE;
}

static DatabasePtr
getDatabaseFromCnchOrHiveCatalog(const String & database_name, ContextPtr context, const TxnTimestamp & timestamp, bool enable_three_part)
{
    String tenant_db = formatTenantDatabaseName(database_name);
    std::optional<String> catalog_name = std::nullopt;
    std::optional<String> db_name = std::nullopt;
    if (enable_three_part)
    {
        std::tie(catalog_name, db_name) = getCatalogNameAndDatabaseName(tenant_db);
    }
    else
    {
        db_name = {database_name};
    }
    //get database from cnch catalog
    if (!catalog_name.has_value() || getOriginalDatabaseName(catalog_name.value()) == "cnch")
    {
        auto catalog = context->getCnchCatalog();
        return catalog->getDatabase(appendTenantIdOnly(db_name.value()), context, timestamp);
    }
    else
    {
        // get database from hive catalog
        return std::make_shared<DatabaseExternalHive>(catalog_name.value(), db_name.value(), context);
    }
}


DatabasePtr DatabaseCatalog::tryGetDatabaseCnch(const String & database_name) const
{
    DatabasePtr cnch_database = getDatabaseFromCnchOrHiveCatalog(
        database_name, getContext(), TxnTimestamp::maxTS(), getContext()->getSettingsRef().enable_three_part_identifier);
    return cnch_database;
}

DatabasePtr DatabaseCatalog::tryGetDatabaseCnch(const String & database_name, ContextPtr local_context) const
{
    String tenant_db = formatTenantDatabaseName(database_name);
    auto txn = local_context->getCurrentTransaction();
    DatabasePtr res;
    if (txn)
        res = txn->tryGetDatabaseViaCache(tenant_db);
    if (res)
        return res;
    res = getDatabaseFromCnchOrHiveCatalog(
        database_name,
        getContext(),
        txn ? txn->getStartTime() : TxnTimestamp::maxTS(),
        local_context->getSettingsRef().enable_three_part_identifier);
    if (res && txn)
        txn->addDatabaseIntoCache(res);
    return res;
}

DatabasePtr DatabaseCatalog::tryGetDatabaseCnch(const UUID & uuid) const
{
    Catalog::Catalog::DataModelDBs all_db_models =
        getContext()->getCnchCatalog()->getAllDataBases();
    auto it = std::find_if(all_db_models.begin(), all_db_models.end(),
        [uuid] (const Protos::DataModelDB & model)
        {
            return (uuid == RPCHelpers::createUUID(model.uuid()));
        }
    );

    if ((it != all_db_models.end()) && !Status::isDeleted(it->status()))
        return Catalog::CatalogFactory::getDatabaseByDataModel(*it, getContext());
    else
        return {};
}

Databases DatabaseCatalog::getDatabaseCnchs() const
{
    Databases res;
    Catalog::Catalog::DataModelDBs all_db_models =
        getContext()->getCnchCatalog()->getAllDataBases();
    std::for_each(
        all_db_models.begin(), all_db_models.end(),
        [& res, this] (const Protos::DataModelDB & model)
        {
            if (!Status::isDeleted(model.status()))
            {
                DatabasePtr database_cnch = Catalog::CatalogFactory::getDatabaseByDataModel(model, getContext());
                res.insert(std::make_pair(model.name(), std::move(database_cnch)));
            }
        }
    );

    return res;
}

DDLGuard::DDLGuard(Map & map_, std::shared_mutex & db_mutex_, std::unique_lock<std::mutex> guards_lock_, const String & elem, const String & database_name)
        : map(map_), db_mutex(db_mutex_), guards_lock(std::move(guards_lock_))
{
    it = map.emplace(elem, Entry{std::make_unique<std::mutex>(), 0}).first;
    ++it->second.counter;
    guards_lock.unlock();
    table_lock = std::unique_lock(*it->second.mutex);
    is_database_guard = elem.empty();
    if (!is_database_guard)
    {

        bool locked_database_for_read = db_mutex.try_lock_shared();
        if (!locked_database_for_read)
        {
            releaseTableLock();
            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database {} is currently dropped or renamed", database_name);
        }
    }
}

void DDLGuard::releaseTableLock() noexcept
{
    if (table_lock_removed)
        return;

    table_lock_removed = true;
    guards_lock.lock();
    UInt32 counter = --it->second.counter;
    table_lock.unlock();
    if (counter == 0)
        map.erase(it);
    guards_lock.unlock();
}

DDLGuard::~DDLGuard()
{
    if (!is_database_guard)
        db_mutex.unlock_shared();
    releaseTableLock();
}

inline bool DatabaseCatalog::preferCnchCatalog(const Context & local_context) const
{
    return use_cnch_catalog || local_context.getSettingsRef().prefer_cnch_catalog;
}
}
