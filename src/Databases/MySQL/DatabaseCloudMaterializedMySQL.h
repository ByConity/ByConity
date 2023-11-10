#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL
#include <Databases/DatabasesCommon.h>
#include <Databases/MySQL/MaterializeMySQLSettings.h>
#include <Databases/MySQL/MaterializeMySQLSyncThread.h>
#include <Databases/MySQL/MaterializedMySQLCommon.h>
#include <Core/MySQL/MySQLClient.h>

#include <common/logger_useful.h>
#include <mysqlxx/Pool.h>

namespace DB
{
/** Database for MaterializedMySQL in memory on Cnch-Worker side; Quite similar as `DatabaseMemory`
 * It will be responsible for running MaterializeMySQLSyncThread.
 * It should not support any DDL operations.
 * */
class DatabaseCloudMaterializedMySQL : public DatabaseWithOwnTablesBase
{
public:
    DatabaseCloudMaterializedMySQL(ContextPtr context, const String & database_name_, UUID uuid_,
                                  const MySQLDatabaseInfo & mysql_info_, std::unique_ptr<MaterializeMySQLSettings> settings_);

    ~DatabaseCloudMaterializedMySQL() override;

    void startSyncThread(const String & thread_id, const String & table, const MySQLBinLogInfo & binlog_info, ContextPtr context);
    void stopSyncThread(const String & thread_id);
    String getTableOfSyncThread(const String & thread_id);

    bool syncThreadIsRunning(const String & /*thread_id*/);

    void setServerClientOfManager(const HostWithPorts & server_client) { server_client_of_manager = server_client; }
    HostWithPorts getServerClientOfManager() const { return server_client_of_manager; }

    UUID getDatabaseUUID() const { return db_uuid; }

private:
    const UUID db_uuid;
    MySQLDatabaseInfo mysql_info;
    std::unique_ptr<MaterializeMySQLSettings> settings;
    HostWithPorts server_client_of_manager;

    struct SyncThreadInfo
    {
        MaterializeMySQLSyncThread materialize_thread;
        String assigned_table;
        std::atomic_bool is_running{false};

        SyncThreadInfo(ContextPtr & context,
                       const String & database_name,
                       const String & mysql_database_name,
                       const String & sync_thread_key,
                       mysqlxx::Pool && pool_,
                       MySQLClient && client_,
                       MaterializeMySQLSettings * settings_,
                       const String & assigned_table_)
        : materialize_thread(context, database_name, mysql_database_name, sync_thread_key, std::move(pool_), std::move(client_), settings_, assigned_table_)
        , assigned_table(assigned_table_)
        {}
    };
    using SyncThreadInfoPtr = std::shared_ptr<SyncThreadInfo>;

    std::unordered_map<String, SyncThreadInfoPtr> sync_threads;
    std::mutex threads_mutex;

public:
    String getEngineName() const override { return "CloudMaterializedMySQL"; }

    void createTable(ContextPtr local_context, const String & table_name, const StoragePtr & table, const ASTPtr & query) override;

    void dropTable(ContextPtr local_context, const String & table_name, bool no_delay) override;

    ASTPtr getCreateDatabaseQuery() const override;

    UUID tryGetTableUUID(const String & table_name) const override;

private:
    ASTPtr getCreateTableQueryImpl(const String & name, ContextPtr local_context, bool throw_on_error) const override;
};

void executeSyncThreadTaskCommand(const MySQLSyncThreadCommand & command, ContextMutablePtr context);
} /// namespace DB

#endif
