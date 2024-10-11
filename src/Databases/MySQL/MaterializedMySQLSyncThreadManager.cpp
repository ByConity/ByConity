#include <Databases/MySQL/MaterializedMySQLSyncThreadManager.h>

#if USE_MYSQL

#include <Catalog/Catalog.h>
#include <Databases/MySQL/MaterializedMySQLCommon.h>
#include <DataStreams/CountingBlockOutputStream.h>
#include <DataStreams/copyData.h>
#include <Formats/MySQLBlockInputStream.h>
#include <IO/Operators.h>
#include <Interpreters/CnchSystemLog.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/StorageID.h>
#include <Parsers/queryToString.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_MYSQL_VARIABLE;
    extern const int ILLEGAL_COLUMN;
    extern const int MYSQL_EXCEPTION;
    extern const int UNEXPECTED_MATERIALIZED_MYSQL_STATE;
    extern const int SYNTAX_ERROR;
    extern const int UNSUPPORTED_MYSQL_TABLE;
    extern const int UNKNOWN_EXCEPTION;
}

static inline TransactionCnchPtr createTransactionForMySQL(const ContextPtr context)
{
    auto & txn_coordinator = context->getCnchTransactionCoordinator();
    auto txn = txn_coordinator.createTransaction();

    return txn;
}

static bool tryToExecuteQueryWithForwarding(const String & query_to_execute, ContextMutablePtr query_context,
                                            const String & database, const StorageID & storage_id, const String & comment, bool internal = true)
{
    try
    {
        if (!database.empty())
            query_context->setCurrentDatabase(database);

        /// Here we use internal forwarding as we have set txn each time
        if (query_context->getSettings().enable_auto_query_forwarding)
        {
            auto cnch_table_helper = CnchStorageCommonHelper(storage_id, database, storage_id.getTableName());
            if (cnch_table_helper.forwardQueryToServerIfNeeded(query_context, storage_id, query_to_execute, false))
                return true;
        }
        /// set enable_auto_query_forwarding to false as this executeQuery will be executed in local server
        query_context->setSetting("enable_auto_query_forwarding", false);
        executeQuery("/*" + comment + "*/ " + query_to_execute, query_context, internal);
        return true;
    }
    catch (...)
    {
        tryLogCurrentException(
            getLogger("MaterializeMySQLManager(" + database + ")"),
            "Query " + query_to_execute + " wasn't finished successfully");
        throw;
    }
}

static BlockIO tryToExecuteQuery(const String & query_to_execute, ContextMutablePtr query_context,
                                 const String & database, const String & comment, bool internal = true)
{
    try
    {
        if (!database.empty())
            query_context->setCurrentDatabase(database);

        return executeQuery("/*" + comment + "*/ " + query_to_execute, query_context, internal);
    }
    catch (...)
    {
        tryLogCurrentException(
            getLogger("MaterializeMySQLManager(" + database + ")"),
            "Query " + query_to_execute + " wasn't finished successfully");
        throw;
    }
}

static BlockIO tryToExecuteQueryWithTxn(const String & query_to_execute, ContextMutablePtr context,
                                        const String & database, const String & comment, bool internal = true)
{
    auto query_context = MaterializedMySQL::createQueryContext(context);
    auto txn = createTransactionForMySQL(context);
    SCOPE_EXIT({
        if (txn)
            context->getCnchTransactionCoordinator().finishTransaction(txn);
    });

    query_context->setCurrentTransaction(txn);
    return tryToExecuteQuery(query_to_execute, query_context, database, comment, internal);
}

static bool checkTableExistence(const mysqlxx::PoolWithFailover::Entry & connection, const std::string & database, const std::string & table, const Settings & global_settings)
{
    Block header{{std::make_shared<DataTypeString>(), "table_name"}};
    String query = "SELECT TABLE_NAME AS table_name FROM INFORMATION_SCHEMA.TABLES  WHERE TABLE_TYPE != 'VIEW' AND TABLE_SCHEMA = "
        + quoteString(database) + " and TABLE_NAME = " + quoteString(table);

    StreamSettings mysql_input_stream_settings(global_settings);
    MySQLBlockInputStream input(connection, query, header, mysql_input_stream_settings);

    while (Block block = input.read())
    {
        if (block.rows() >= 1)
            return true;
    }

    return false;
}

static inline void cleanOutdatedTables(const String & database_name, ContextPtr context)
{
    String cleaning_table_name;
    try
    {
        auto ddl_guard = DatabaseCatalog::instance().getDDLGuard(database_name, "");
        const DatabasePtr & clean_database = DatabaseCatalog::instance().getDatabase(database_name, context);

        for (auto iterator = clean_database->getTablesIterator(context); iterator->isValid(); iterator->next())
        {
            String comment = "Materialize MySQL step 1: execute MySQL DDL for dump data";
            cleaning_table_name = backQuoteIfNeed(database_name) + "." + backQuoteIfNeed(iterator->name());

            auto query_context = MaterializedMySQL::createQueryContext(context);
            auto txn = createTransactionForMySQL(context);
            SCOPE_EXIT({
                if (txn)
                    context->getCnchTransactionCoordinator().finishTransaction(txn);
            });

            query_context->setCurrentTransaction(txn);
            tryToExecuteQuery(" DROP TABLE " + cleaning_table_name, query_context, database_name, comment);
        }
    }
    catch (Exception & exception)
    {
        exception.addMessage("While executing " + (cleaning_table_name.empty() ? "cleanOutdatedTables" : cleaning_table_name));
        throw;
    }
}

static inline BlockOutputStreamPtr
getTableOutput(const String & database_name, const String & table_name, ContextMutablePtr query_context, bool insert_delete_flag_column = false)
{
    const StoragePtr & storage = DatabaseCatalog::instance().getTable(StorageID(database_name, table_name), query_context);

    WriteBufferFromOwnString insert_columns_str;
    auto storage_metadata = storage->getInMemoryMetadataPtr();
    const ColumnsDescription & storage_columns = storage_metadata->getColumns();
    const NamesAndTypesList & insert_columns_names = storage_columns.getOrdinary();


    for (auto iterator = insert_columns_names.begin(); iterator != insert_columns_names.end(); ++iterator)
    {
        if (iterator != insert_columns_names.begin())
            insert_columns_str << ", ";

        insert_columns_str << backQuoteIfNeed(iterator->name);
    }
    if (insert_delete_flag_column)
    {
        insert_columns_str << ", " << backQuoteIfNeed(StorageInMemoryMetadata::DELETE_FLAG_COLUMN_NAME);
    }


    String comment = "Materialize MySQL step 1: execute dump data";
    BlockIO res = tryToExecuteQuery("INSERT INTO " + backQuoteIfNeed(table_name) + "(" + insert_columns_str.str() + ")" + " VALUES",
        query_context, database_name, comment);

    if (!res.out)
        throw Exception("LOGICAL ERROR: It is a bug.", ErrorCodes::LOGICAL_ERROR);

    return res.out;
}

static void createTable(const String & database_name, const String & query_str, const ContextPtr context)
{
    auto query_context = MaterializedMySQL::createQueryContext(context);
    auto txn = createTransactionForMySQL(context);
    SCOPE_EXIT({
        if (txn)
            context->getCnchTransactionCoordinator().finishTransaction(txn);
    });

    query_context->setCurrentTransaction(txn);

    String comment = "Materialize MySQL step 1: execute MySQL DDL for dump data";
    tryToExecuteQuery(query_str, query_context, database_name, comment); /// create table.
}

static inline void dumpDataForTables(
    mysqlxx::Pool::Entry & connection, const std::unordered_map<String, String> & need_dumping_tables,
    const String & query_prefix, const String & database_name, const String & mysql_database_name,
    ContextPtr context, NameSet & unsupported_tables_local)
{
    auto iterator = need_dumping_tables.begin();
    for (; iterator != need_dumping_tables.end() /*&& !is_cancelled()*/; ++iterator)
    {
        LOG_DEBUG(getLogger("DumpDataForMySQLTable"), "Try to create and dump data for table {} with sql: {}", iterator->first, iterator->second);
        try
        {
            /// 1. create table
            createTable(database_name, query_prefix + " " + iterator->second, context);

            /// 2. dump all data from mysql
            auto query_context = MaterializedMySQL::createQueryContext(context);
            auto txn = createTransactionForMySQL(context);
            SCOPE_EXIT({
                if (txn)
                    context->getCnchTransactionCoordinator().finishTransaction(txn);
            });

            query_context->setCurrentTransaction(txn);
            const auto & table_name = iterator->first;

            auto out = std::make_shared<CountingBlockOutputStream>(getTableOutput(database_name, table_name, query_context));
            StreamSettings mysql_input_stream_settings(context->getSettingsRef());
            MySQLBlockInputStream input(
                connection, "SELECT * FROM " + backQuoteIfNeed(mysql_database_name) + "." + backQuoteIfNeed(table_name),
                out->getHeader(), mysql_input_stream_settings);

            Stopwatch watch;
            copyData(input, *out);
            const Progress & progress = out->getProgress();
            LOG_INFO(getLogger("MaterializeMySQLSyncThread(" + database_name + ")"),
                "Materialize MySQL step 1: dump {}, {} rows, {} in {} sec., {} rows/sec., {}/sec."
                , table_name, formatReadableQuantity(progress.written_rows), formatReadableSizeWithBinarySuffix(progress.written_bytes)
                , watch.elapsedSeconds(), formatReadableQuantity(static_cast<size_t>(progress.written_rows / watch.elapsedSeconds()))
                , formatReadableSizeWithBinarySuffix(static_cast<size_t>(progress.written_bytes / watch.elapsedSeconds())));
        }
        catch (Exception & exception)
        {
            exception.addMessage("While executing dump MySQL " + mysql_database_name + "." + iterator->first);
            if (exception.code() == ErrorCodes::UNSUPPORTED_MYSQL_TABLE || exception.code() == ErrorCodes::NOT_IMPLEMENTED || exception.code() == ErrorCodes::SYNTAX_ERROR || exception.code() == ErrorCodes::ILLEGAL_COLUMN)
            {
                LOG_WARNING(
                        getLogger("MaterializeMySQLManager(" + database_name + ")"),
                        "Skip unsupported MySQL table while trying to execute: [" + iterator->second + "]; "
                            + "due to: " + getCurrentExceptionMessage(true));
                if (!unsupported_tables_local.count(iterator->first))
                    unsupported_tables_local.emplace(iterator->first);
                exception.addMessage("Skip unsupported table: {}", iterator->first);
                continue;
            }
            throw;
        }
    }
}
/// End of some functions to sync MySQL for the first time

MaterializedMySQLSyncThreadManager::MaterializedMySQLSyncThreadManager(ContextPtr context_, const StorageID & storage_id_)
    : ICnchBGThread(context_, CnchBGThreadType::MaterializedMySQL, storage_id_)
{
    database_ptr = DatabaseCatalog::instance().getDatabase(storage_id.database_name, getContext());

    materialized_mysql_ptr = dynamic_cast<DatabaseCnchMaterializedMySQL*>(database_ptr.get());
    if (!materialized_mysql_ptr)
        throw Exception("CnchMaterializedMySQL is expected, but got " + database_ptr->getEngineName(), ErrorCodes::LOGICAL_ERROR);

    auto mysql_info = materialized_mysql_ptr->getMysqlDatabaseInfo();
    auto mysql_pool = mysqlxx::Pool(mysql_info.mysql_database_name, mysql_info.mysql_host_name, mysql_info.mysql_user_name, mysql_info.mysql_password, mysql_info.mysql_port);
    MySQLClient client(mysql_info.mysql_host_name, mysql_info.mysql_port, mysql_info.mysql_user_name, mysql_info.mysql_password);
    mysql_component = std::make_shared<MySQLComponent>(std::move(mysql_pool), std::move(client));
    resync_table_task = getContext()->getSchedulePool().createTask("MaterializeMySQLResyncTableTask", [this]{ runResyncTableTask(); });
    if (auto materialize_mysql_log = getContext()->getCloudMaterializedMySQLLog())
    {
        auto current_log = MaterializedMySQL::createMaterializeMySQLLog(MaterializedMySQLLogElement::SYNC_MANAGER, MaterializedMySQLLogElement::EMPTY, storage_id.database_name);
        materialize_mysql_log->add(current_log);
    }
}

MaterializedMySQLSyncThreadManager::~MaterializedMySQLSyncThreadManager()
{
    try
    {
        stop();
    }
    catch(...)
    {
        recordException();
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

void MaterializedMySQLSyncThreadManager::clearData()
{
    /// persist CloudMaterializedMySQLLog only when manager (sync_type == Syncing)
    if (sync_type == MaterializedMySQLSyncType::Syncing)
    {
        if (auto materialize_mysql_log = getContext()->getCloudMaterializedMySQLLog())
        {
            auto current_log = MaterializedMySQL::createMaterializeMySQLLog(MaterializedMySQLLogElement::SYNC_MANAGER, MaterializedMySQLLogElement::MANUAL_STOP_SYNC, storage_id.database_name);
            materialize_mysql_log->add(current_log);
        }
    }
    setSyncType(MaterializedMySQLSyncType::ManualStopSync);
    /// Stop resync table task
    resync_table_quit = true;
    resync_table_task->deactivate();
    /// Second: stop all sync threads on worker side
    std::lock_guard lock(status_info_mutex);
    stopSyncThreads();
    threads_info.clear();
    initialized = false;
}

void MaterializedMySQLSyncThreadManager::drop()
{
    stop();

    /// try to clear meta data in Catalog
    catalog->removeMaterializedMySQLBinlogMetadata(getNamePrefixForMaterializedBinlog(storage_id.uuid));
}

void MaterializedMySQLSyncThreadManager::executeDDLQuery(const String & query, const String & sync_thread_key, const MySQLBinLogInfo & binlog)
{
    String table_name;
    try
    {
        const auto ddl_params = parseMySQLDDLQuery(query);
        /// TODO: rename table cmd may need additional logic
        if (!shouldSyncTable(ddl_params.execute_table))
            return;
        if (materialized_mysql_ptr->shouldSkipDDLCmd(query))
        {
            if (auto materialize_mysql_log = getContext()->getCloudMaterializedMySQLLog())
            {
                auto current_log = MaterializedMySQL::createMaterializeMySQLLog(MaterializedMySQLLogElement::SYNC_MANAGER, MaterializedMySQLLogElement::SKIP_DDL, storage_id.database_name);
                current_log.event_msg = query;
                materialize_mysql_log->add(current_log);
            }
            LOG_DEBUG(log, "Skip MySQL DDL command filtered by setting skip_ddl_patterns: \n {}", query);
            return;
        }
        table_name = ddl_params.execute_table;
        executeDDLQueryImpl(ddl_params, sync_thread_key, binlog);
    } catch (Exception & e)
    {
        recordException();
        if (e.code() == ErrorCodes::UNSUPPORTED_MYSQL_TABLE || e.code() == ErrorCodes::NOT_IMPLEMENTED || e.code() == ErrorCodes::SYNTAX_ERROR || e.code() == ErrorCodes::ILLEGAL_COLUMN)
        {
            LOG_WARNING(log, "Skip unsupported mysql table");
            if (!skipped_unsupported_tables.count(table_name))
                skipped_unsupported_tables.emplace(table_name);
            return;
        }
        throw;
    }
}

void MaterializedMySQLSyncThreadManager::executeDDLQueryImpl(const MySQLDDLQueryParams & ddl_params, const String & sync_thread_key, const MySQLBinLogInfo & binlog)
{
    auto query_context = MaterializedMySQL::createQueryContext(getContext());
    auto txn = createTransactionForMySQL(getContext());
    SCOPE_EXIT({
        if (txn)
            getContext()->getCnchTransactionCoordinator().finishTransaction(txn);
    });

    query_context->setCurrentTransaction(txn);

    String comment = "Materialize MySQL step 2: execute MySQL DDL for sync data";
    String mysql_database_name = materialized_mysql_ptr->getMysqlDatabaseInfo().mysql_database_name;
    String query_prefix = "EXTERNAL DDL FROM MySQL(" + backQuoteIfNeed(storage_id.database_name) + ", "
        + backQuoteIfNeed(mysql_database_name) + ") ";

    /// Almost all DDL actions need to change threads info in memory, so we hold the lock for the whole process of DDL
    /// However, this is not an efficient method as the other processes requires the lock
    /// TODO: optimize it
    std::lock_guard lock(status_info_mutex);
    switch (ddl_params.query_type)
    {
        case MySQLDDLQueryParams::CREATE_TABLE:
        {
            /// In the case of multiple workers, it is guaranteed that only one create statement is executed
            if (materialized_tables_list.find(ddl_params.execute_table) != materialized_tables_list.end())
                return;

            /// Execute CREATE TABLE query
            try
            {
                tryToExecuteQuery(query_prefix + ddl_params.query, query_context, storage_id.database_name, comment);
            }
            catch (Exception & e)
            {
                e.addMessage("While executing MYSQL_QUERY_EVENT. The query: " + ddl_params.query);

                tryLogCurrentException(log);
                throw;
            }

            Strings update_keys, update_values;
            /// Update Catalog metadata and start sync thread
            materialized_tables_list.emplace(ddl_params.execute_table);
            auto manager_metadata = catalog->getOrSetMaterializedMySQLManagerMetadata(storage_id);
            manager_metadata->clear_materialized_tables();
            for (const auto & table : materialized_tables_list)
                manager_metadata->add_materialized_tables(table);
            update_keys.emplace_back(getNameForMaterializedMySQLManager(storage_id.uuid));
            update_values.emplace_back(manager_metadata->SerializeAsString());

            update_keys.emplace_back(getNameForMaterializedBinlog(storage_id.uuid, ddl_params.execute_table));
            update_values.emplace_back(createBinlogMetadata(binlog, ddl_params.execute_table).SerializeAsString());
            catalog->updateMaterializedMySQLMetadataInBatch(update_keys, update_values, {});

            /// Add sync thread in memory
            threads_info.emplace_back();
            auto & thread_info = threads_info.back();
            thread_info = std::make_shared<SyncThreadScheduleInfo>();

            thread_info->assigned_materialized_table = ddl_params.execute_table;
            getBinlogMetadataFromCatalog(thread_info->binlog_info, ddl_params.execute_table);
            break;
        }
        case MySQLDDLQueryParams::ALTER_TABLE:
        case MySQLDDLQueryParams::DROP_TABLE:
        case MySQLDDLQueryParams::RENAME_TABLE:
        case MySQLDDLQueryParams::TRUNCATE_TABLE:
        {
            SyncThreadScheduleInfoPtr thread_info = nullptr;
            {
                for (const auto & info : threads_info)
                {
                    if (info->sync_thread_key == sync_thread_key)
                    {
                        thread_info = info;
                        break;
                    }
                }
            }
            if (!thread_info)
                throw Exception("No sync thread #" + sync_thread_key + " found in Manager", ErrorCodes::LOGICAL_ERROR);

            /// Execute DDL sql by holding the lock of thread
            {
                std::lock_guard thread_lock(thread_info->mutex);

                if (!thread_info->worker_client || !thread_info->is_running)
                    LOG_INFO(log, "Sync Thread #" + sync_thread_key + " is not running when try to execute DDL: " + ddl_params.query);
                else
                {
                    try
                    {
                        MySQLSyncThreadCommand command;
                        command.type = MySQLSyncThreadCommand::STOP_SYNC;
                        command.database_name = storage_id.database_name + "_" + thread_info->sync_thread_key;
                        command.rpc_port = getContext()->getRPCPort();
                        command.sync_thread_key = thread_info->sync_thread_key;
                        command.table = thread_info->assigned_materialized_table;
                        thread_info->worker_client->submitMySQLSyncThreadTask(command);
                    }
                    catch (...)
                    {
                        LOG_WARNING(log, "Failed to stop sync-thread before executing DDL: " + ddl_params.query + "; But we still reset thread and execute the query");
                    }

                    thread_info->worker_client = nullptr;
                    thread_info->is_running = false;
                }

                try
                {
                    bool need_password = ddl_params.query_type == MySQLDDLQueryParams::ALTER_TABLE || ddl_params.query_type == MySQLDDLQueryParams::RENAME_TABLE ||  ddl_params.query_type == MySQLDDLQueryParams::TRUNCATE_TABLE;
                    if (need_password)
                    {
                        /// Set user_name & password in case of forwarding query later;
                        /// FIXME: @renqiang Not a very reliable method anyway :(
                        const auto [user_name, password] = getContext()->getCnchInterserverCredentials();
                        auto & client_info = query_context->getClientInfo();
                        client_info.current_user = user_name;
                        client_info.current_password = password;
                        if (auto table = DatabaseCatalog::instance().getTable(StorageID(storage_id.database_name, ddl_params.execute_table), getContext()))
                            tryToExecuteQueryWithForwarding(query_prefix + ddl_params.query, query_context, storage_id.database_name, table->getStorageID(), comment/*, !is_alter*/);
                    }
                    else
                        tryToExecuteQuery(query_prefix + ddl_params.query, query_context, storage_id.database_name, comment/*, !is_alter*/);
                }
                catch (Exception & e)
                {
                    e.addMessage("While executing MYSQL_QUERY_EVENT. The query: " + ddl_params.query);

                    tryLogCurrentException(log);
                    throw;
                }

                /// Some suffix action for special DDLs: RENAME TABLE; DROP TABLE
                executeDDLQuerySuffix(sync_thread_key, binlog, ddl_params);
            }
            break;
        }
        default:
            throw Exception("Unknown MySQL DDL query: " + ddl_params.query, ErrorCodes::MYSQL_EXCEPTION);
    }
}

void MaterializedMySQLSyncThreadManager::executeDDLQuerySuffix(const String & sync_thread_key,
                                                               const MySQLBinLogInfo & binlog,
                                                               const MySQLDDLQueryParams & ddl_params)
{
    Strings update_keys, update_values, delete_keys;
    switch (ddl_params.query_type)
    {
        case MySQLDDLQueryParams::DROP_TABLE:
        {
            /// update manager metadata for materialized tables list
            materialized_tables_list.erase(ddl_params.execute_table);
            auto manager_metadata = catalog->getOrSetMaterializedMySQLManagerMetadata(storage_id);
            manager_metadata->clear_materialized_tables();
            for (const auto & table : materialized_tables_list)
                manager_metadata->add_materialized_tables(table);
            update_keys.emplace_back(getNameForMaterializedMySQLManager(storage_id.uuid));
            update_values.emplace_back(manager_metadata->SerializeAsString());

            /// update binlog metadata & threads_info in memory
            LOG_INFO(log, "Remove metadata from catalog for table {}", ddl_params.execute_table);
            delete_keys.emplace_back(getNameForMaterializedBinlog(storage_id.uuid, ddl_params.execute_table));
            /// Commit metadata to Catalog in batch
            catalog->updateMaterializedMySQLMetadataInBatch(update_keys, update_values, delete_keys);

            for (size_t idx = 0; idx < threads_info.size(); ++idx)
            {
                if (threads_info[idx]->sync_thread_key == sync_thread_key)
                {
                    LOG_INFO(log, "Erase thread info after executing {}", ddl_params.query);
                    threads_info.erase(threads_info.begin() + idx);
                    break;
                }
            }

            break;
        }
        case MySQLDDLQueryParams::RENAME_TABLE:
        {
            /// update manager metadata for materialized tables list
            materialized_tables_list.erase(ddl_params.execute_table);
            /// when rename_to_table should not be synced, just remove the metadata and thread info
            bool should_sync_rename_table = shouldSyncTable(ddl_params.rename_to_table, false, false);
            if (should_sync_rename_table)
                materialized_tables_list.emplace(ddl_params.rename_to_table);

            auto manager_metadata = catalog->getOrSetMaterializedMySQLManagerMetadata(storage_id);
            manager_metadata->clear_materialized_tables();
            for (const auto & table : materialized_tables_list)
                manager_metadata->add_materialized_tables(table);

            update_keys.emplace_back(getNameForMaterializedMySQLManager(storage_id.uuid));
            update_values.emplace_back(manager_metadata->SerializeAsString());

            delete_keys.emplace_back(getNameForMaterializedBinlog(storage_id.uuid, ddl_params.execute_table));
            if (should_sync_rename_table)
            {
                update_keys.emplace_back(getNameForMaterializedBinlog(storage_id.uuid, ddl_params.rename_to_table));
                update_values.emplace_back(createBinlogMetadata(binlog, ddl_params.rename_to_table).SerializeAsString());
            }
            /// Commit updated metadata to Catalog in batch
            catalog->updateMaterializedMySQLMetadataInBatch(update_keys, update_values, delete_keys);

            /// update threads info in memory
            for (size_t idx = 0; idx < threads_info.size(); ++idx)
            {
                if (threads_info[idx]->sync_thread_key == sync_thread_key)
                {
                    if (should_sync_rename_table)
                        threads_info[idx]->assigned_materialized_table = ddl_params.rename_to_table;
                    else
                        threads_info.erase(threads_info.begin() + idx);
                    break;
                }
            }
            break;
        }
        case MySQLDDLQueryParams::ALTER_TABLE:
        case MySQLDDLQueryParams::TRUNCATE_TABLE:
        {
            /// update binlog metadata
            for (size_t idx = 0; idx < threads_info.size(); ++idx)
            {
                if (threads_info[idx]->sync_thread_key == sync_thread_key)
                {
                    threads_info[idx]->binlog_info = binlog;

                    String binlog_meta_name = getNameForMaterializedBinlog(storage_id.uuid, ddl_params.execute_table);
                    auto binlog_metadata = catalog->getMaterializedMySQLBinlogMetadata(binlog_meta_name);
                    if (!binlog_metadata)
                        throw Exception("Cannot get binlog meta from catalog for " + binlog_meta_name, ErrorCodes::LOGICAL_ERROR);

                    binlog_metadata->set_binlog_file(binlog.binlog_file);
                    binlog_metadata->set_binlog_position(binlog.binlog_position);
                    binlog_metadata->set_executed_gtid_set(binlog.executed_gtid_set);

                    catalog->setMaterializedMySQLBinlogMetadata(binlog_meta_name, *binlog_metadata);

                    break;
                }
            }
            break;
        }
        default:
            LOG_WARNING(log, "No need to execute suffix for: " + ddl_params.query);
    }
}

void MaterializedMySQLSyncThreadManager::handleHeartbeatOfSyncThread(const String & sync_thread_key)
{
    {
        std::lock_guard lock(status_info_mutex);
        for (const auto & info : threads_info)
        {
            if (info->sync_thread_key == sync_thread_key)
            {
                std::lock_guard lock_thread(info->mutex);
                if (!info->worker_client || !info->is_running)
                    throw Exception("Sync Thread #" + sync_thread_key + " is not in running status", ErrorCodes::LOGICAL_ERROR);
                else /// Everything is ok
                    return;
            }
        }
    }

    throw Exception("No thread #" + sync_thread_key + " found in Manager", ErrorCodes::LOGICAL_ERROR);
}

void MaterializedMySQLSyncThreadManager::handleSyncFailedThread(const String & sync_thread_key)
{
    {
        std::lock_guard lock(status_info_mutex);
        for (const auto & info : threads_info)
        {
            if (info->sync_thread_key == sync_thread_key)
            {
                std::lock_guard lock_thread(info->mutex);
                if (!sync_failed_tables.count(info->assigned_materialized_table))
                    sync_failed_tables.emplace(info->assigned_materialized_table);
            }
        }
    }
}

void MaterializedMySQLSyncThreadManager::manualResyncTable(const String & table_name)
{
    if (sync_type == MaterializedMySQLSyncType::FullSync)
        throw Exception(
                ErrorCodes::UNEXPECTED_MATERIALIZED_MYSQL_STATE,
                "Database {} is in full sync/restore state, skip resync table command.",
                storage_id.database_name);

    std::lock_guard lock(resync_mutex);
    resync_tables.emplace(table_name);
}

void MaterializedMySQLSyncThreadManager::runImpl()
{
    bool should_stop = false;
    try
    {
        should_stop = iterate();
    }
    catch (...)
    {
        recordException();
        tryLogCurrentException(log, getCurrentExceptionMessage(true, true));
    }

    if (should_stop)
        return;

    const auto ITERATE_INTERVAL_MS = 10 * 1000;
    scheduled_task->scheduleAfter(ITERATE_INTERVAL_MS);
}

bool MaterializedMySQLSyncThreadManager::iterate()
{
    if (!initialized)
    {
        initialize();
        initialized = true;
    }

    /// When materialized_tables_list is empty, reinit
    if (materialized_tables_list.empty())
        initialized = false;

    /// Judge whether sync thread manager should stop according to settings & skip_sync_failed_tables/skipped_unsupported_tables
    {
        std::lock_guard lock(status_info_mutex);
        if ((!materialized_mysql_ptr->getMaterializeMySQLSettings()->skip_sync_failed_tables && !sync_failed_tables.empty())
            || (!materialized_mysql_ptr->getMaterializeMySQLSettings()->skip_unsupported_tables && !skipped_unsupported_tables.empty()))
        {
            stopSyncThreads();
            threads_info.clear();
            initialized = false;
            setSyncType(MaterializedMySQLSyncType::ExceptionStopSync);
            return true;
        }
    }
    /// Make a copy of `threads_info` for each iteration to avoid hold the lock for a long time
    /// while the DDL actions would change the `threads_info`
    std::vector<SyncThreadScheduleInfoPtr> current_threads_info;
    {
        std::lock_guard lock(status_info_mutex);
        for (auto & thread_info : threads_info)
        {
            std::lock_guard thread_lock(thread_info->mutex);
            if (shouldSyncTable(thread_info->assigned_materialized_table, false, false))
                current_threads_info.emplace_back(thread_info);
        }
    }

    setSyncType(MaterializedMySQLSyncType::Syncing);
    std::lock_guard resync_lock(resync_mutex);
    for (auto & thread_info : current_threads_info)
    {
        std::lock_guard thread_lock(thread_info->mutex);
        if (resync_tables.count(thread_info->assigned_materialized_table))
            continue;

        if (thread_info->worker_client)
            checkStatusOfSyncThread(thread_info);

        if (!thread_info->worker_client)
            scheduleSyncThreadToWorker(thread_info);
    }
    return false;
}

void MaterializedMySQLSyncThreadManager::initialize()
{
    setSyncType(MaterializedMySQLSyncType::PreparingSync);
    auto manager_metadata = catalog->getOrSetMaterializedMySQLManagerMetadata(storage_id);

    /// Update materialized_tables_list with include/exclude setting
    updateSyncTableList();

    if (manager_metadata->dumped_first_time())
    {
        updateResyncTables();
        manager_metadata->clear_materialized_tables();
        for (const auto & name: materialized_tables_list)
            manager_metadata->add_materialized_tables(name);
        catalog->updateMaterializedMySQLManagerMetadata(storage_id, *manager_metadata);
    }
    else
        initializeMaterializedMySQL(manager_metadata);

    if (materialized_tables_list.empty())
    {
        LOG_WARNING(log, "No tables found in mysql to be synced");
        return;
    }

    /// here the tables have been created and dumped for the first time
    std::lock_guard resync_lock(resync_mutex);
    std::lock_guard lock(status_info_mutex);

    threads_info.clear();
    for (const auto & table : materialized_tables_list)
    {
        threads_info.emplace_back();
        auto & thread_info = threads_info.back();
        thread_info = std::make_shared<SyncThreadScheduleInfo>();

        thread_info->assigned_materialized_table = table;

        if (manager_metadata->dumped_first_time() && !resync_tables.count(table))
            getBinlogMetadataFromCatalog(thread_info->binlog_info, table);
    }
    resync_table_task->activateAndSchedule();
}

void MaterializedMySQLSyncThreadManager::createReadonlyReplicaClient(const String & replica_info, const String & mysql_database_name)
{
    using namespace rapidjson;
    Document document;
    ParseResult ok = document.Parse(replica_info.c_str());
    if (!ok)
        throw Exception(String("JSON parse error ") + GetParseError_En(ok.Code()) + " " + toString(ok.Offset()), ErrorCodes::BAD_ARGUMENTS);

    MySQLDatabaseInfo mysql_info;
    mysql_info.mysql_database_name = mysql_database_name;
    for (const auto & member : document.GetObject())
    {
        if (!member.value.IsString())
            continue;
        auto && key = member.name.GetString();
        auto && value = member.value.GetString();
        LOG_TRACE(log, "[replica_info] {} : {}", key, value);
        if (std::string(key) == "host_name")
            mysql_info.mysql_host_name = value;
        else if (std::string(key) == "user_name")
            mysql_info.mysql_user_name = value;
        else if (std::string(key) == "port")
            mysql_info.mysql_port = std::atoi(value);
        else if (std::string(key) == "password")
            mysql_info.mysql_password = value;
        else
            LOG_ERROR(log, "Unknown mysql read-only replica info with key: {} and value: {}", key, value);
    }
    /// FIXME: check some necessary variables

    auto mysql_pool = mysqlxx::Pool(mysql_info.mysql_database_name, mysql_info.mysql_host_name, mysql_info.mysql_user_name, mysql_info.mysql_password, mysql_info.mysql_port);
    MySQLClient client(mysql_info.mysql_host_name, mysql_info.mysql_port, mysql_info.mysql_user_name, mysql_info.mysql_password);
    mysql_readonly_replica_component = std::make_shared<MySQLComponent>(std::move(mysql_pool), std::move(client));
}

void MaterializedMySQLSyncThreadManager::updateResyncTables()
{
    std::lock_guard lock(resync_mutex);

    /// Step 1: remove tables which doesn't need to resync any more.
    for (auto it = resync_tables.begin(); it != resync_tables.end(); )
    {
        bool found = false;
        for (const auto & name: materialized_tables_list)
        {
            if (*it == name)
            {
                found = true;
                break;
            }
        }
        if (!found)
            it = resync_tables.erase(it);
        else
            ++it;
    }

    mysqlxx::PoolWithFailover::Entry connection = mysql_component->pool.tryGet();
    MaterializeMetadata materialize_metadata(getContext()->getSettingsRef());
    NameSet mysql_binlog_files = materialize_metadata.getBinlogFilesFromMySQL(connection);

    /// Step 2: check new table to resync
    for (const auto & table: materialized_tables_list)
    {
        if (resync_tables.count(table) || !shouldSyncTable(table))
            continue;

        if (!DatabaseCatalog::instance().tryGetTable(StorageID(storage_id.database_name, table), getContext()))
            resync_tables.emplace(table);

        auto binlog_metadata = catalog->getMaterializedMySQLBinlogMetadata(getNameForMaterializedBinlog(storage_id.uuid, table));
        if (!binlog_metadata || !mysql_binlog_files.contains(binlog_metadata->binlog_file()))
        {
            LOG_INFO(log, "Cannot get binlog metadata/binlog not exist for " + table + ", we will resync");
            resync_tables.emplace(table);
        }
    }
}

void MaterializedMySQLSyncThreadManager::initializeMaterializedMySQL(std::shared_ptr<Protos::MaterializedMySQLManagerMetadata> & manager_metadata)
{
    mysqlxx::PoolWithFailover::Entry connection = mysql_component->pool.tryGet();
    if (connection.isNull())
        throw Exception("Unable to connect to MySQL", ErrorCodes::UNKNOWN_EXCEPTION);

    MaterializedMySQL::checkMySQLVariables(connection, getContext()->getSettingsRef());

    MaterializeMetadata materialize_metadata(getContext()->getSettingsRef());

    /// Here we need to Sync tables with MySQL for two scenarios:
    /// a) the first initialization for the new database (including retry due to errors of previous initialization
    /// b) the lag of synchronization is so large that the binlog file has been deleted on MySQL side

    const auto & mysql_database = materialized_mysql_ptr->getMysqlDatabaseInfo();
    auto original_materialized_tables_list = materialized_tables_list;

    bool is_readonly_replica = false;
    if (const auto & readonly_replica_info = materialized_mysql_ptr->getMaterializeMySQLSettings()->read_only_replica_info.value;
            !readonly_replica_info.empty())
    {
        createReadonlyReplicaClient(readonly_replica_info, mysql_database.mysql_database_name);

        connection = mysql_readonly_replica_component->pool.tryGet();
        if (connection.isNull())
            throw Exception("Unable to connect to MySQL read-only replica", ErrorCodes::UNKNOWN_EXCEPTION);
        is_readonly_replica = true;
    }

    bool opened_transaction = false;
    bool need_update_metadata = false;

    LOG_INFO(log, "Begin sync tables for mysql " + mysql_database.mysql_database_name);
    /// 1. Get {table_name, create_table_sql} list; update metadata with current binlog info of MySQL
    /// here also `START TRANSACTION`
    std::unordered_map<String, String> need_dumping_tables;
    materialize_metadata.getTablesWithCreateQueryFromMySql(connection, mysql_database.mysql_database_name, opened_transaction,
                                                            original_materialized_tables_list, need_dumping_tables, is_readonly_replica);

    {
        /// SCOPE with MySQL transaction for dumping data
        SCOPE_EXIT({
                try
                {
                    if (opened_transaction)
                        connection->query("ROLLBACK;").execute();
                }
                catch (...)
                {
                    recordException();
                    tryLogCurrentException(log, "Failed to rollback transaction for mysql");
                }

                /// Execute 'mysql.rds_start_replication' out of transaction as it will modify @@session.sql_log_bin
                try
                {
                    if (is_readonly_replica)
                        connection->query("CALL mysql.rds_start_replication;").execute();
                }
                catch (...)
                {
                    recordException();
                    tryLogCurrentException(log, "Failed to restart replication for read replica");
                }
            }
        );

        if (!need_dumping_tables.empty())
        {
            setSyncType(MaterializedMySQLSyncType::FullSync);
            LOG_DEBUG(log, "Synced {} tables need to dump", need_dumping_tables.size());
            /// 2. Clear tables in case the previous initialization failed on the way
            cleanOutdatedTables(storage_id.database_name, getContext());

            /// 3. Create tables and dump the data from mysql for the first time
            String query_prefix = "EXTERNAL DDL FROM MySQL(" + backQuoteIfNeed(storage_id.database_name) + ", "
                                    + backQuoteIfNeed(mysql_database.mysql_database_name) + ") ";

            NameSet unsupported_tables_local;
            dumpDataForTables(
                connection, need_dumping_tables, query_prefix, storage_id.database_name, mysql_database.mysql_database_name, getContext(), unsupported_tables_local);
            if (!unsupported_tables_local.empty())
            {
                std::lock_guard lock(status_info_mutex);
                for (const auto & unsupported_table: unsupported_tables_local)
                {
                    if (!skipped_unsupported_tables.count(unsupported_table))
                        skipped_unsupported_tables.emplace(unsupported_table);
                }
            }
            need_update_metadata = true;

            LOG_DEBUG(log, "Finish dump data for {} tables", need_dumping_tables.size());
        }
        else
            LOG_DEBUG(log, "No tables need to sync for mysql: " + mysql_database.mysql_database_name);

        if (opened_transaction)
            connection->query("COMMIT;").execute();
        opened_transaction = false;
    }

    if (need_update_metadata)
    {
        /// 4. After initialization finished, update metadata: set binlog metadata first
        auto binlog_metadata = createBinlogMetadata(materialize_metadata);
        for (auto iter = need_dumping_tables.begin(); iter != need_dumping_tables.end(); ++iter)
        {
            manager_metadata->add_materialized_tables(iter->first);
            LOG_INFO(log, "[Enable sync thread per table] Set binlog metadata for {}", iter->first);
            catalog->setMaterializedMySQLBinlogMetadata(
                getNameForMaterializedBinlog(storage_id.uuid, iter->first), createBinlogMetadata(materialize_metadata, iter->first));
        }

        manager_metadata->set_dumped_first_time(true);
        catalog->updateMaterializedMySQLManagerMetadata(storage_id, *manager_metadata);
    }
}

CnchWorkerClientPtr MaterializedMySQLSyncThreadManager::selectWorker()
{
    /// TODO: introduce Resource Manager
    String vw_name = materialized_mysql_ptr->getMaterializeMySQLSettings()->cnch_vw_write.value;
    vw_handle = getContext()->getVirtualWarehousePool().get(vw_name);
    return vw_handle->getWorker();
}

static void rewriteCreateQuery(String & query, const String & suffix_key, bool is_table)
{
    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "", 0, 0);
    auto & create_query = ast->as<ASTCreateQuery &>();

    if (is_table)
    {
        auto engine = std::make_shared<ASTFunction>();
        const String & engine_name = create_query.storage->engine->name;
        if (engine_name != "CnchMergeTree")
            throw Exception("Table engine of MaterializedMySQL should be CnchMergeTree, but got " + engine_name, ErrorCodes::LOGICAL_ERROR);

        engine->name = String(create_query.storage->engine->name).replace(0, strlen("Cnch"), "Cloud");
        engine->arguments = std::make_shared<ASTExpressionList>();
        engine->arguments->children.push_back(std::make_shared<ASTIdentifier>(create_query.database));
        engine->arguments->children.push_back(std::make_shared<ASTIdentifier>(create_query.table));

        create_query.storage->set(create_query.storage->engine, engine);
        create_query.database += ("_" + suffix_key);
        create_query.table = create_query.table + "_" + suffix_key;

        query = getTableDefinitionFromCreateQuery(ast, false);
    }
    else /// For database: MaterializedMySQL database should be created specially
    {
        auto & engine = create_query.storage->engine;
        auto engine_name = engine->name;
        engine->name = engine_name.replace(0, strlen("Cnch"), "Cloud");
        create_query.database += ("_" + suffix_key);
        create_query.if_not_exists = true;

        query = queryToString(create_query);
    }
}

void MaterializedMySQLSyncThreadManager::scheduleSyncThreadToWorker(SyncThreadScheduleInfoPtr & info)
{
    /// Use timestamp as unique key to identify sync thread as well as the suffix of table on Cnch Worker side
    String thread_key = toString(std::chrono::system_clock::now().time_since_epoch().count());

    /// Create task command
    MySQLSyncThreadCommand command;
    command.type = MySQLSyncThreadCommand::START_SYNC;
    command.database_name = storage_id.database_name + "_" + thread_key;
    command.sync_thread_key = thread_key;
    command.rpc_port = getContext()->getRPCPort();

    /// Rewrite create sql for database and table
    String create_database_sql = materialized_mysql_ptr->getCreateSql();
    rewriteCreateQuery(create_database_sql, thread_key, false);
    LOG_TRACE(log, "Debug create cloud database query: {}", create_database_sql);

    command.create_sqls.emplace_back(create_database_sql);

    Strings create_queries;
    {
        const auto & table_name = info->assigned_materialized_table;
        auto storage = DatabaseCatalog::instance().getTable(StorageID(storage_id.database_name, table_name), MaterializedMySQL::createQueryContext(getContext()));
        auto create_sql = storage->getCreateTableSql();

        rewriteCreateQuery(create_sql, /*table_name + "_" + */ thread_key, true);
        LOG_TRACE(log, "Debug create cloud table query: {}", create_sql);

        command.table = table_name;
        command.create_sqls.emplace_back(create_sql);
        create_queries.emplace_back(std::move(create_sql));
    }

    info->create_queries = create_queries;
    info->sync_thread_key = thread_key;
    getBinlogMetadataFromCatalog(info->binlog_info, info->assigned_materialized_table);

    command.binlog = info->binlog_info;
    LOG_TRACE(log, "Get binlog from catalog: {} for thread #{}", command.binlog.dump(), info->sync_thread_key);

    /// Schedule task to worker side
    CnchWorkerClientPtr worker_client = selectWorker();
    worker_client->submitMySQLSyncThreadTask(command);
    LOG_DEBUG(log, "Successfully to schedule sync thread #" + info->sync_thread_key + " on worker " + worker_client->getTCPAddress());

    /// Set running status after submit sync task successfully
    info->worker_client = worker_client;
    info->is_running = true;
}

void MaterializedMySQLSyncThreadManager::checkStatusOfSyncThread(SyncThreadScheduleInfoPtr & info)
{
    try
    {
        if (!info->is_running)
            throw Exception("Sync Thread " + info->sync_thread_key + " is not in running status", ErrorCodes::LOGICAL_ERROR);

        if (!info->worker_client->checkMySQLSyncThreadStatus(storage_id.database_name + "_" + info->sync_thread_key, info->sync_thread_key))
            throw Exception("Sync Thread " + info->sync_thread_key + " is not running on worker", ErrorCodes::LOGICAL_ERROR);

        LOG_DEBUG(log, "Check status of sync thread #{} successfully on worker {}", info->sync_thread_key, info->worker_client->getTCPAddress());
    }
    catch (...)
    {
        info->worker_client = nullptr;
        info->is_running = false;

        recordException();
        tryLogCurrentException(log, "Failed to check status of sync thread and reset worker client to trigger reschedule");
    }
}

void MaterializedMySQLSyncThreadManager::stopSyncThreads()
{
    try
    {
        if (threads_info.size() == 1)
        {
            stopSyncThreadOnWorker(threads_info.front());
        }
        else
        {
            ThreadPool pool(std::min(threads_info.size(), getContext()->getSettings().max_threads.value));
            for (auto & info : threads_info)
            {
                pool.scheduleOrThrowOnError([&c = info, this] { stopSyncThreadOnWorker(c); });
            }
            pool.wait();
        }
    }
    catch (...)
    {
        recordException();
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

void MaterializedMySQLSyncThreadManager::stopSyncThreadOnWorker(SyncThreadScheduleInfoPtr & info)
{
    MySQLSyncThreadCommand command;
    command.type = MySQLSyncThreadCommand::STOP_SYNC;
    command.rpc_port = getContext()->getRPCPort();

    CnchWorkerClientPtr worker_client = nullptr;
    {
        std::lock_guard lock(info->mutex);
        if (!info->is_running || !info->worker_client)
        {
            LOG_INFO(log, "Sync Thread #" + info->sync_thread_key + " is not running, don't need execute stop action");
            return;
        }

        command.database_name = storage_id.database_name + "_" + info->sync_thread_key;
        command.sync_thread_key = info->sync_thread_key;
        command.table = info->assigned_materialized_table;
        worker_client = info->worker_client;

        /// Reset status of sync thread
        info->worker_client = nullptr;
        info->is_running = false;
    }

    worker_client->submitMySQLSyncThreadTask(command);
    LOG_DEBUG(log, "Successfully to stop sync thread #" + info->sync_thread_key + " on worker " + worker_client->getTCPAddress());
}

Protos::MaterializedMySQLBinlogMetadata MaterializedMySQLSyncThreadManager::createBinlogMetadata(const MaterializeMetadata & metadata,
                                                                                                 const String & table_name)
{
    Protos::MaterializedMySQLBinlogMetadata binlog_metadata;
    binlog_metadata.set_database_name(storage_id.database_name);
    RPCHelpers::fillUUID(storage_id.uuid, *binlog_metadata.mutable_database_uuid());

    binlog_metadata.set_table(table_name);
    binlog_metadata.set_binlog_file(metadata.binlog_file);
    binlog_metadata.set_binlog_position(metadata.binlog_position);
    binlog_metadata.set_executed_gtid_set(metadata.executed_gtid_set);
    binlog_metadata.set_meta_version(metadata.meta_version);

    return binlog_metadata;
}

Protos::MaterializedMySQLBinlogMetadata MaterializedMySQLSyncThreadManager::createBinlogMetadata(const MySQLBinLogInfo & binlog,
                                                                                                 const String & table_name)
{
    Protos::MaterializedMySQLBinlogMetadata binlog_metadata;
    binlog_metadata.set_database_name(storage_id.database_name);
    RPCHelpers::fillUUID(storage_id.uuid, *binlog_metadata.mutable_database_uuid());

    binlog_metadata.set_table(table_name);
    binlog_metadata.set_binlog_file(binlog.binlog_file);
    binlog_metadata.set_binlog_position(binlog.binlog_position);
    binlog_metadata.set_executed_gtid_set(binlog.executed_gtid_set);
    binlog_metadata.set_meta_version(binlog.meta_version);

    return binlog_metadata;
}

void MaterializedMySQLSyncThreadManager::getBinlogMetadataFromCatalog(MySQLBinLogInfo & binlog, const String & table_name)
{
    auto binlog_name = getNameForMaterializedBinlog(storage_id.uuid, table_name);
    auto binlog_proto = catalog->getMaterializedMySQLBinlogMetadata(binlog_name);
    if (!binlog_proto)
        throw Exception("Cannot get binlog meta from catalog for " + storage_id.getDatabaseNameForLogs() + " #" + binlog_name, ErrorCodes::LOGICAL_ERROR);

    binlog.binlog_file = binlog_proto->binlog_file();
    binlog.binlog_position = binlog_proto->binlog_position();
    binlog.executed_gtid_set = binlog_proto->executed_gtid_set();
    binlog.meta_version = binlog_proto->meta_version();
}

MySQLSyncStatusInfo MaterializedMySQLSyncThreadManager::getSyncStatusInfo()
{
    MySQLSyncStatusInfo res;

    res.mysql_info = mysql_component->client.getConnectionInfo();
    res.mysql_database = materialized_mysql_ptr->getMysqlDatabaseInfo().mysql_database_name;
    res.sync_type = toString(sync_type.load(std::memory_order_relaxed));
    res.include_tables = materialized_mysql_ptr->getMaterializeMySQLSettings()->include_tables.value;
    res.exclude_tables = materialized_mysql_ptr->getMaterializeMySQLSettings()->exclude_tables.value;
    {
        std::lock_guard lock(resync_mutex);
        res.resync_tables = resync_tables;
    }
    res.total_position = getMasterPosition().toString();

    std::lock_guard lock(status_info_mutex);
    res.threads_num = 0;
    for (const auto & thread : threads_info)
    {
        std::lock_guard lock_thread(thread->mutex);
        if (res.resync_tables.count(thread->assigned_materialized_table))
            continue;

        res.threads_num++;
        res.materialized_tables.insert(res.materialized_tables.end(), thread->assigned_materialized_table);

        MySQLBinLogInfo binlog = thread->binlog_info;
        getBinlogMetadataFromCatalog(binlog, thread->assigned_materialized_table);
        res.binlog_infos.emplace_back(binlog.dump());

        auto thread_name = database_ptr->getDatabaseName() + "_" + thread->sync_thread_key;
        res.sync_thread_names.emplace_back(thread_name);
        if (thread->worker_client)
            res.sync_thread_clients.emplace_back(thread->worker_client->getHostWithPortsID());
        else
            res.sync_thread_clients.emplace_back("Not Scheduled");

        if (!thread->worker_client || !thread->is_running)
            res.broken_threads.insert(res.broken_threads.end(), thread->assigned_materialized_table);
    }

    {
        std::lock_guard exception_lock(last_exception_mutex);
        res.last_exception = last_exception;
    }
    res.sync_failed_tables = sync_failed_tables;
    res.skipped_unsupported_tables = skipped_unsupported_tables;

    return res;
}

Position MaterializedMySQLSyncThreadManager::getMasterPosition() const
{
    Position position;
    try
    {
        Block header{
            {std::make_shared<DataTypeString>(), "File"},
            {std::make_shared<DataTypeUInt64>(), "Position"},
            {std::make_shared<DataTypeString>(), "Executed_Gtid_Set"},
        };
        StreamSettings mysql_input_stream_settings(getContext()->getSettingsRef(), false, true);
        MySQLBlockInputStream input(mysql_component->pool.get(), "SHOW MASTER STATUS;", header, mysql_input_stream_settings);
        Block master_status = input.read();

        if (!master_status || master_status.rows() != 1)
            throw Exception("Unable to get master status from MySQL.", ErrorCodes::LOGICAL_ERROR);

        String binlog_file = (*master_status.getByPosition(0).column)[0].safeGet<String>();
        UInt64 binlog_position = (*master_status.getByPosition(1).column)[0].safeGet<UInt64>();
        String executed_gtid_set = (*master_status.getByPosition(2).column)[0].safeGet<String>();

        position.update(binlog_position, binlog_file, executed_gtid_set);
    }
    catch (...)
    {
        const_cast<MaterializedMySQLSyncThreadManager *>(this)->recordException();
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
    return position;
}

void MaterializedMySQLSyncThreadManager::updateSyncTableList()
{
    auto tables_in_mysql = MaterializeMetadata::fetchTablesInDB(mysql_component->pool.get(), materialized_mysql_ptr->getMysqlDatabaseInfo().mysql_database_name, getContext()->getSettingsRef());
    for (const auto & table_in_mysql: tables_in_mysql)
    {
        if (shouldSyncTable(table_in_mysql))
            materialized_tables_list.insert(table_in_mysql);
    }
}

bool MaterializedMySQLSyncThreadManager::shouldSyncTable(const String & table_name, bool ignore_skip, bool need_lock)
{
    auto action = [&]() {
        if (!ignore_skip)
        {
            if (!skipped_unsupported_tables.empty())
            {
                if (skipped_unsupported_tables.count(table_name))
                    return false;
            }
            if (materialized_mysql_ptr->getMaterializeMySQLSettings()->skip_sync_failed_tables && !sync_failed_tables.empty())
            {
                if (sync_failed_tables.count(table_name))
                    return false;
            }
        }
        return materialized_mysql_ptr->shouldSyncTable(table_name);
    };

    if (need_lock)
    {
        std::lock_guard lock(status_info_mutex);
        return action();
    }
    else
        return action();
}

void MaterializedMySQLSyncThreadManager::setSyncType(MaterializedMySQLSyncType new_type)
{
    if (sync_type == new_type)
        return;
    LOG_DEBUG(log, "Updated sync type from {} to {}", toString(sync_type.load(std::memory_order_relaxed)), toString(new_type));
    sync_type = new_type;
}

void MaterializedMySQLSyncThreadManager::recordException(bool create_log, const String & resync_table)
{
    {
        std::lock_guard lock(last_exception_mutex);
        last_exception = std::to_string(LocalDateTime(time(nullptr))) + " : " + getCurrentExceptionMessage(false);
    }
    MaterializedMySQL::recordException(MaterializedMySQLLogElement::SYNC_MANAGER, getContext(), storage_id.database_name, {}, create_log, resync_table);
}

void MaterializedMySQLSyncThreadManager::runResyncTableTask()
{
    try
    {
        if (isResyncTableCancelled())
            return;

        while (!resync_tables.empty() && !isResyncTableCancelled())
        {
            String table_to_sync = "";
            {
                std::lock_guard lock(resync_mutex);
                if (resync_tables.empty())
                    break;
                table_to_sync = *(resync_tables.begin());
            }
            if (sync_type == MaterializedMySQLSyncType::PreparingSync || sync_type == MaterializedMySQLSyncType::FullSync)
            {
                LOG_DEBUG(log, "Database is in PreparingSync or FullSync state, retry resync table task later.");
                break;
            }

            MaterializeMetadata materialize_metadata(getContext()->getSettingsRef());
            auto connection = mysql_component->pool.get();
            materialize_metadata.fetchMasterOrSlaveStatus(connection, false);

            {
                std::lock_guard lock(status_info_mutex);
                for (auto & info : threads_info)
                {
                    String info_table_name;
                    {
                        std::lock_guard thread_lock(info->mutex);
                        info_table_name = info->assigned_materialized_table;
                    }
                    if (info_table_name == table_to_sync)
                        stopSyncThreadOnWorker(info);   /// stop sync thread for resync table
                }
            }
            size_t failed_cnt = 0;
            while (failed_cnt < materialized_mysql_ptr->getMaterializeMySQLSettings()->resync_table_task_fail_retry_time && !isResyncTableCancelled())
            {
                try
                {
                    doResyncTable(table_to_sync);
                    {
                        std::lock_guard lock(status_info_mutex);
                        sync_failed_tables.erase(table_to_sync);
                        if (skipped_unsupported_tables.count(table_to_sync))
                            skipped_unsupported_tables.erase(table_to_sync);
                    }
                    break;
                }
                catch (Exception & e)
                {
                    tryLogCurrentException(log, "Fail to resync table " + table_to_sync);

                    /// Handle the case that table doesn't exist.
                    if (e.code() == ErrorCodes::UNKNOWN_TABLE)
                    {
                        LOG_DEBUG(log, "Resync table {} doesn't exist in mysql.", table_to_sync);
                        recordException(true, table_to_sync);
                        break;
                    }
                    /// Skip unsupported tables, currently known the following three errorcodes:
                    ///     NOT_IMPLEMENTED: no primary key issue
                    ///     SYNTAX_ERROR: ddl sql may contain unsupported syntax, like create table as
                    ///     ILLEGAL_COLUMN: Column xxx can't be used in UNIQUE KEY because its type xxx is not mem-comparable
                    /// In order to adapt to more scenarios, any error is considered as unsupported.
                    else if (e.code() == ErrorCodes::UNSUPPORTED_MYSQL_TABLE || e.code() == ErrorCodes::NOT_IMPLEMENTED || e.code() == ErrorCodes::SYNTAX_ERROR || e.code() == ErrorCodes::ILLEGAL_COLUMN)
                    {
                        {
                            std::lock_guard lock(status_info_mutex);
                            sync_failed_tables.erase(table_to_sync);
                            if (!skipped_unsupported_tables.count(table_to_sync))
                                skipped_unsupported_tables.emplace(table_to_sync);
                        }
                        e.addMessage("Skip unsupported table: {}", table_to_sync);
                        recordException(true, table_to_sync);
                        break;
                    }
                    recordException(true, table_to_sync);
                }
                catch (...)
                {
                    tryLogCurrentException(log, "Fail to resync table " + table_to_sync);
                    recordException(true, table_to_sync);
                }
                failed_cnt++;
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }

            if (failed_cnt >= materialized_mysql_ptr->getMaterializeMySQLSettings()->resync_table_task_fail_retry_time)
            {
                LOG_ERROR(
                    log,
                    "Resync table {} failed more than {} times, add it into sync failed tables.",
                    table_to_sync,
                    materialized_mysql_ptr->getMaterializeMySQLSettings()->resync_table_task_fail_retry_time);
                if (auto materialize_mysql_log = getContext()->getCloudMaterializedMySQLLog())
                {
                    auto current_log = MaterializedMySQL::createMaterializeMySQLLog(MaterializedMySQLLogElement::SYNC_MANAGER, MaterializedMySQLLogElement::ERROR, storage_id.database_name);
                    current_log.has_error = 1;
                    current_log.event_msg = "Resync table " + table_to_sync + " failed more than "
                        + toString(materialized_mysql_ptr->getMaterializeMySQLSettings()->resync_table_task_fail_retry_time.value) + " times, add it into sync_failed_tables";
                    materialize_mysql_log->add(current_log);
                }
                std::lock_guard lock(status_info_mutex);
                sync_failed_tables.emplace(table_to_sync);
            }

            if (!isResyncTableCancelled())
            {
                std::lock_guard lock(resync_mutex);
                resync_tables.erase(table_to_sync);
                LOG_INFO(log, "[Enable sync thread per table] Set binlog metadata for {}", table_to_sync);
                catalog->setMaterializedMySQLBinlogMetadata(
                    getNameForMaterializedBinlog(storage_id.uuid, table_to_sync), createBinlogMetadata(materialize_metadata, table_to_sync));
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
    resync_table_task->scheduleAfter(materialized_mysql_ptr->getMaterializeMySQLSettings()->resync_table_task_schedule_time_ms);
}

void MaterializedMySQLSyncThreadManager::doResyncTable(const String & table_name)
{
    String tmp_table_suffix = "_CHTMP";
    String query_prefix = "EXTERNAL DDL FROM MySQL(" + backQuoteIfNeed(storage_id.database_name) + ", "
                                    + backQuoteIfNeed(materialized_mysql_ptr->getMysqlDatabaseInfo().mysql_database_name) + ") ";
    Stopwatch timer;
    LOG_DEBUG(log, "Start to resync table {}", table_name);
    if (auto materialize_mysql_log = getContext()->getCloudMaterializedMySQLLog())
    {
        auto current_log = MaterializedMySQL::createMaterializeMySQLLog(MaterializedMySQLLogElement::SYNC_MANAGER, MaterializedMySQLLogElement::RESYNC_TABLE, storage_id.database_name);
        current_log.resync_table = table_name;
        materialize_mysql_log->add(current_log);
    }

    /// Check table existence
    auto connection = mysql_component->pool.get();
    if (!checkTableExistence(connection, materialized_mysql_ptr->getMysqlDatabaseInfo().mysql_database_name, table_name, getContext()->getSettingsRef()))
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Resync table {} doesn't exist in mysql.", table_name);

    String full_mysql_table_name = backQuoteIfNeed(materialized_mysql_ptr->getMysqlDatabaseInfo().mysql_database_name) + "." + backQuoteIfNeed(table_name);
    String tmp_table_name = table_name + tmp_table_suffix;
    String full_mysql_tmp_table_name = backQuoteIfNeed(materialized_mysql_ptr->getMysqlDatabaseInfo().mysql_database_name) + "." + backQuoteIfNeed(tmp_table_name);
    Block show_create_table_header{
        {std::make_shared<DataTypeString>(), "Table"},
        {std::make_shared<DataTypeString>(), "Create Table"},
    };

    StreamSettings mysql_input_stream_settings(getContext()->getSettingsRef(), false, true);
    MySQLBlockInputStream show_create_table(
        connection, "SHOW CREATE TABLE " + full_mysql_table_name, show_create_table_header, mysql_input_stream_settings);

    Block create_query_block = show_create_table.read();

    if (!create_query_block || create_query_block.rows() != 1)
        throw Exception("LOGICAL ERROR mysql show create return more rows.", ErrorCodes::LOGICAL_ERROR);

    String create_query = create_query_block.getByName("Create Table").column->getDataAt(0).toString();
    LOG_DEBUG(log, "The original create table query of resync table {} is {}", table_name, create_query);
    create_query = create_query.replace(create_query.find(table_name), table_name.length(), tmp_table_name);
    LOG_DEBUG(log, "The temp create table query of resync temp table {} is {}", tmp_table_name, create_query);

    auto query_context = MaterializedMySQL::createQueryContext(getContext());
    {
        String comment = "Materialize MySQL resync table step 1: execute MySQL DDL for dump data to tmp table";
        tryToExecuteQueryWithTxn(query_prefix + " DROP TABLE IF EXISTS " + full_mysql_tmp_table_name, query_context, storage_id.database_name, comment);
        tryToExecuteQueryWithTxn(query_prefix + " " + create_query, query_context, storage_id.database_name, comment);

        auto query_context_inter = MaterializedMySQL::createQueryContext(query_context);
        auto txn = createTransactionForMySQL(query_context);
        SCOPE_EXIT({
            if (txn)
                query_context->getCnchTransactionCoordinator().finishTransaction(txn);
        });

        query_context_inter->setCurrentTransaction(txn);

        auto out = std::make_shared<CountingBlockOutputStream>(getTableOutput(storage_id.database_name, tmp_table_name, query_context_inter));
        StreamSettings inner_mysql_input_stream_settings(getContext()->getSettingsRef());
        MySQLBlockInputStream input(
            connection, "SELECT * FROM " + full_mysql_table_name, out->getHeader(), inner_mysql_input_stream_settings);

        Stopwatch watch;
        copyData(input, *out, [this, table_name]() {
            std::lock_guard lock(resync_mutex);
            return resync_tables.count(table_name) == 0 || isResyncTableCancelled();
        });
        const Progress & progress = out->getProgress();
        LOG_INFO(getLogger("MaterializeMySQLSyncThread(" + storage_id.database_name + ")"),
            "Materialize MySQL step 1: dump {}, {} rows, {} in {} sec., {} rows/sec., {}/sec."
            , table_name, formatReadableQuantity(progress.written_rows), formatReadableSizeWithBinarySuffix(progress.written_bytes)
            , watch.elapsedSeconds(), formatReadableQuantity(static_cast<size_t>(progress.written_rows / watch.elapsedSeconds()))
            , formatReadableSizeWithBinarySuffix(static_cast<size_t>(progress.written_bytes / watch.elapsedSeconds())));
    }

    if (isResyncTableCancelled())
    {
        LOG_DEBUG(log, "Resync table action has been cancelled, skip and return.");
        return;
    }

    {
        String comment = "Materialize MySQL resync table step 2: drop table";
        tryToExecuteQueryWithTxn(query_prefix + " DROP TABLE IF EXISTS " + full_mysql_table_name, query_context, storage_id.database_name, comment);
    }

    {
        String comment = "Materialize MySQL resync table step 3: rename tmp table to normal table";
        tryToExecuteQueryWithTxn(
            query_prefix + " RENAME TABLE " + full_mysql_tmp_table_name + " TO " + full_mysql_table_name, query_context, storage_id.database_name, comment);
    }

    LOG_DEBUG(log, "Success to resync table {}, cost {} ms", table_name, timer.elapsedMilliseconds());
}

} /// namespace DB

#endif
