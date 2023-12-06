#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#include <Databases/IDatabase.h>
#include <Interpreters/Context_fwd.h>
#include <Core/MySQL/MySQLClient.h>
#include <Databases/DatabasesCommon.h>
#include <Databases/DatabaseCnch.h>
#include <Databases/MySQL/MaterializeMySQLSettings.h>
#include <Databases/MySQL/MaterializeMySQLSyncThread.h>
#include <Databases/MySQL/MaterializedMySQLCommon.h>
#include <Interpreters/Context.h>

#include <mysqlxx/Pool.h>


namespace DB
{
class DatabaseCnchMaterializedMySQL : public DatabaseCnch
{
public:
    DatabaseCnchMaterializedMySQL(ContextPtr local_context, const String & database_name_, UUID uuid_, const MySQLDatabaseInfo & mysql_info_,
                                  std::unique_ptr<MaterializeMySQLSettings> settings_, const String & create_sql_);
    ~DatabaseCnchMaterializedMySQL() override = default;

    bool supportSnapshot() const override { return false; }

    bool shouldSyncTable(const String & table_name) const;
    /// Check whether to skip ddl command based on setting skip_ddl_patterns. Maintaining the DDL skip in the manager(server side) makes it easier to obtain relevant filtering logs.
    bool shouldSkipDDLCmd(const String & query) const;
    void manualResyncTable(const String & table_name, ContextPtr local_context);

    StorageID getStorageID() const { return StorageID{getDatabaseName(), getUUID()}; }
    String getCreateSql() const { return create_sql; }
    MySQLDatabaseInfo getMysqlDatabaseInfo() const { return mysql_info; }
    MaterializeMySQLSettings * getMaterializeMySQLSettings() const { return settings.get(); }

private:
    MySQLDatabaseInfo mysql_info;
    std::unique_ptr<MaterializeMySQLSettings> settings;
    String create_sql;
    mutable std::mutex handler_mutex;

public:
    String getEngineName() const override { return "CnchMaterializedMySQL"; }

    void createTable(ContextPtr local_context, const String & table_name, const StoragePtr & table, const ASTPtr & query) override;
    void dropTable(ContextPtr local_context, const String & table_name, bool no_delay) override;
    void renameTable(
        ContextPtr context,
        const String & table_name,
        IDatabase & to_database,
        const String & to_table_name,
        bool exchange,
        bool dictionary) override;
    StoragePtr tryGetTable(const String & name, ContextPtr local_context) const override;
    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr local_context, const FilterByNameFunction & filter_by_table_name) override;

    ASTPtr getCreateDatabaseQuery() const override;

    void createEntryInCnchCatalog(ContextPtr local_context, String create_query) const;

    bool bgJobIsActive() const;

    void applySettingsChanges(const SettingsChanges & settings_changes, ContextPtr query_context) override;
};

} /// namespace DB
#endif
