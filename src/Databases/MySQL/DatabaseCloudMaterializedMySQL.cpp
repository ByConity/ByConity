#include <Databases/MySQL/DatabaseCloudMaterializedMySQL.h>

#if USE_MYSQL
#include <Common/Exception.h>
#include <Databases/IDatabase.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserDropQuery.h>
#include <Parsers/parseQuery.h>

namespace DB
{

DatabaseCloudMaterializedMySQL::DatabaseCloudMaterializedMySQL(ContextPtr context_, const String & database_name_, UUID uuid_,
                                                               const MySQLDatabaseInfo & mysql_info_,
                                                               std::unique_ptr<MaterializeMySQLSettings> settings_)
    : DatabaseWithOwnTablesBase(database_name_, "DatabaseCloudMaterializedMySQL ( " + database_name_ + " )", context_)
    , db_uuid(uuid_)
    , mysql_info(mysql_info_)
    , settings(std::move(settings_))
{
}

DatabaseCloudMaterializedMySQL::~DatabaseCloudMaterializedMySQL()
{
    try
    {
        std::lock_guard lock(threads_mutex);
        if (!sync_threads.empty())
        {
            for (const auto & thread_ptr : sync_threads)
                thread_ptr.second->materialize_thread.stopSynchronization();
        }
        sync_threads.clear();
    }
    catch (...)
    {
        tryLogCurrentException(log, "~DatabaseCloudMaterializedMySQL");
    }
}

void DatabaseCloudMaterializedMySQL::startSyncThread(const String & thread_id, const String & table, const MySQLBinLogInfo & binlog_info, ContextPtr context_)
{
    /// TODO: optimize here
    auto mysql_pool = mysqlxx::Pool(mysql_info.mysql_database_name, mysql_info.mysql_host_name, mysql_info.mysql_user_name, mysql_info.mysql_password, mysql_info.mysql_port);
    MySQLClient client(mysql_info.mysql_host_name, mysql_info.mysql_port, mysql_info.mysql_user_name, mysql_info.mysql_password);

    SyncThreadInfoPtr thread_ptr;
    {
        std::lock_guard lock(threads_mutex);
        if (sync_threads.find(thread_id) != sync_threads.end())
            throw Exception("Sync Thread for " + thread_id + " already exists", ErrorCodes::LOGICAL_ERROR);

        sync_threads.emplace(thread_id, std::make_shared<SyncThreadInfo>(context_, getDatabaseName(), mysql_info.mysql_database_name, thread_id,
                                                                        std::move(mysql_pool), std::move(client), settings.get(), table));
        thread_ptr = sync_threads[thread_id];
    }

    thread_ptr->materialize_thread.setBinLogInfo(binlog_info);
    thread_ptr->materialize_thread.startSynchronization();
    thread_ptr->is_running = true;
}

void DatabaseCloudMaterializedMySQL::stopSyncThread(const String & thread_id)
{
    SyncThreadInfoPtr thread_ptr = nullptr;
    {
        std::lock_guard lock(threads_mutex);
        if (sync_threads.find(thread_id) == sync_threads.end())
        {
            LOG_INFO(log, "Sync thread: {} has stopped", thread_id);
            return;
        }

        thread_ptr = sync_threads[thread_id];
        sync_threads.erase(thread_id);
    }

    if (!thread_ptr)
        return;

    thread_ptr->is_running = false;
    thread_ptr->materialize_thread.stopSynchronization();
    /// FIXME: API removeTable would cause core; we drop table in STOP_SYNC API by InterpreterDropQuery
}

bool DatabaseCloudMaterializedMySQL::syncThreadIsRunning(const String & thread_id)
{
    std::lock_guard lock(threads_mutex);
    if (sync_threads.find(thread_id) == sync_threads.end())
        throw Exception("Sync Thread #" + thread_id + " does not exist", ErrorCodes::LOGICAL_ERROR);

    return sync_threads[thread_id]->is_running;
}

String DatabaseCloudMaterializedMySQL::getTableOfSyncThread(const String & thread_id)
{
    std::lock_guard lock(threads_mutex);
    if (sync_threads.find(thread_id) == sync_threads.end())
        return {};
    else
        return sync_threads[thread_id]->assigned_table;
}

void DatabaseCloudMaterializedMySQL::createTable(ContextPtr /*local_context*/, const String & table_name, const StoragePtr & table, const ASTPtr & /*query*/)
{
    attachTable(table_name, table, "");
}

void DatabaseCloudMaterializedMySQL::dropTable(ContextPtr /*local_context*/, const String & table_name, bool /*no_delay*/)
{
    detachTable(table_name);
}

ASTPtr DatabaseCloudMaterializedMySQL::getCreateTableQueryImpl(const String & /*name*/, ContextPtr /*local_context*/, bool /*throw_on_error*/) const
{
    throw Exception("Not support getCreateTableQuery for " + getEngineName(), ErrorCodes::NOT_IMPLEMENTED);
}

ASTPtr DatabaseCloudMaterializedMySQL::getCreateDatabaseQuery() const
{
    throw Exception("Not support getCreateDatabaseQuery for " + getEngineName(), ErrorCodes::NOT_IMPLEMENTED);
}

UUID DatabaseCloudMaterializedMySQL::tryGetTableUUID(const String & table_name) const
{
    if (auto table = tryGetTable(table_name, getContext()))
        return table->getStorageID().uuid;
    return UUIDHelpers::Nil;
}

static void recordException(ContextPtr context, String database, bool create_log = true, const String & resync_table = "")
{
    MaterializedMySQL::recordException(MaterializedMySQLLogElement::WORKER_THREAD, context, database, {}, create_log, resync_table);
}

static void executeCreateSqlForSyncThread(const std::vector<String> & create_commands, ContextPtr context)
{
    auto create_context = Context::createCopy(context);

    ParserCreateQuery parser;
    for (const auto & cmd : create_commands)
    {
        LOG_DEBUG(getLogger("MySQLSyncThreadTask"), "Try to execute CREATE query: {}", cmd);
        ASTPtr ast = parseQuery(parser, cmd, "", 0, create_context->getSettingsRef().max_parser_depth);

        InterpreterCreateQuery interpreter_tb(ast, create_context);
        interpreter_tb.execute();
    }
    LOG_DEBUG(getLogger("MySQLSyncThreadTask"), "Executed CREATE query successfully");
}

void executeSyncThreadTaskCommandImpl(const MySQLSyncThreadCommand & command, ContextMutablePtr context)
{
    if (command.type == MySQLSyncThreadCommand::START_SYNC)
    {
        executeCreateSqlForSyncThread(command.create_sqls, context);
    }

    auto database = DatabaseCatalog::instance().tryGetDatabase(command.database_name, context);
    if (!database)
        throw Exception("Database " + command.database_name + " doesn't exist, it seems CREATE failed", ErrorCodes::LOGICAL_ERROR);

    auto * materialized_mysql = dynamic_cast<DatabaseCloudMaterializedMySQL*>(database.get());
    if (!materialized_mysql)
        throw Exception("Database is not created as DatabaseCloudMaterializedMySQL, instead " + database->getEngineName(), ErrorCodes::LOGICAL_ERROR);

    switch (command.type)
    {
        case MySQLSyncThreadCommand::START_SYNC:
        {
            auto & client_info = context->getClientInfo();
            String server_host = client_info.current_address.host().toString();
            UInt16 server_rpc_port = client_info.rpc_port;
            materialized_mysql->setServerClientOfManager(HostWithPorts::fromRPCAddress(addBracketsIfIpv6(server_host) + ':' + toString(server_rpc_port)));

            materialized_mysql->startSyncThread(command.sync_thread_key, command.table, command.binlog, context);
            break;
        }
        case MySQLSyncThreadCommand::STOP_SYNC:
        {
            auto assigned_table = materialized_mysql->getTableOfSyncThread(command.sync_thread_key);
            materialized_mysql->stopSyncThread(command.sync_thread_key);

            /// Drop assigned table
            Strings drop_commands;
            ParserDropQuery parser;
            String drop_table_command = "DROP TABLE IF EXISTS " + (backQuoteIfNeed(materialized_mysql->getDatabaseName())
                                                                 + "." + backQuoteIfNeed(assigned_table + "_" + command.sync_thread_key));
            drop_commands.emplace_back(drop_table_command);
            /// Drop database itself
            drop_commands.emplace_back("DROP DATABASE IF EXISTS " + (backQuoteIfNeed(materialized_mysql->getDatabaseName())));

            for (const auto & drop_command : drop_commands)
            {
                LOG_DEBUG(getLogger("ExecuteSyncThreadCommand"), "Try to Execute DROP sql: {}", drop_command);
                try
                {
                    InterpreterDropQuery interpreter(parseQuery(parser, drop_command, "", 0, 0), context);
                    interpreter.execute();

                    LOG_DEBUG(getLogger("ExecuteSyncThreadCommand"), "Successfully executed DROP sql: {}", drop_command);
                }
                catch (...)
                {
                    /// just record exception/print log to ensure all tables to be dropped
                    recordException(context, command.database_name);
                    tryLogCurrentException(__PRETTY_FUNCTION__);
                }
            }
            break;
        }
        case MySQLSyncThreadCommand::CREATE_DATABASE:
        default:
            throw Exception("Command type " + String(MySQLSyncThreadCommand::toString(command.type))
                                + " is not supported now", ErrorCodes::NOT_IMPLEMENTED);
    }
}

void executeSyncThreadTaskCommand(const MySQLSyncThreadCommand & command, ContextMutablePtr context)
{
    try
    {
        executeSyncThreadTaskCommandImpl(command, context);
    }
    catch (...)
    {
        recordException(context, command.database_name);
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

} /// namespace DB

#endif
