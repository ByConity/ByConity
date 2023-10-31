#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#include <Core/Types.h>
#include <Core/UUID.h>
#include <Formats/MySQLBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Interpreters/MaterializedMySQLLog.h>

namespace DB
{
namespace MySQLErrorCode
{
    // USE MySQL ERROR CODE:
    // https://dev.mysql.com/doc/mysql-errors/5.7/en/server-error-reference.html
    const int ER_ACCESS_DENIED_ERROR = 1045;
    const int ER_DBACCESS_DENIED_ERROR = 1044;
    const int ER_BAD_DB_ERROR = 1049;

    // https://dev.mysql.com/doc/mysql-errors/8.0/en/client-error-reference.html
    const int CR_SERVER_LOST = 2013;
};

/** Store info of MySQL database and used to create synchronization thread, including:
 * mysql-client & mysql-pool */
struct MySQLDatabaseInfo
{
    String mysql_database_name;
    String mysql_user_name;
    String mysql_password;
    String mysql_host_name;
    UInt16 mysql_port = 3306;
};

/// This info need to be recorded in Catalog
/// And they should be transferred along with sync-thread scheduling
struct MySQLBinLogInfo
{
    String binlog_file;
    UInt64 binlog_position{0};
    String executed_gtid_set;
    size_t meta_version{2};

    String dump() const
    {
        return "BinlogFile: [" + binlog_file + "]; Position: [" + std::to_string(binlog_position) + "]; "
                + "GTID set: [" + executed_gtid_set + "]; MetaVersion: [" + std::to_string(meta_version) + "]";
    }
};

enum class MaterializedMySQLSyncType
{
    PreparingSync,
    FullSync,
    Syncing,
    ManualStopSync,
    ExceptionStopSync,
};

String toString(const MaterializedMySQLSyncType & type);

struct MySQLSyncThreadCommand
{
    enum CommandType
    {
        UNKNOWN_TYPE = 0,
        CREATE_DATABASE,
        START_SYNC,
        STOP_SYNC,
    };
    CommandType type{UNKNOWN_TYPE};
    String sync_thread_key;
    String database_name;
    String table;
    Strings create_sqls;
    MySQLBinLogInfo binlog;
    UInt16 rpc_port{0};

    static const char * toString(CommandType type)
    {
        switch (type)
        {
            case CREATE_DATABASE:   return "CREATE DATABASE";
            case START_SYNC:        return "START SYNC THREAD";
            case STOP_SYNC:         return "STOP SYNC THREAD";
            default:                return "UNKNOWN TYPE";
        }
    }
};

struct MySQLDDLQueryParams
{
    enum DDLQueryType
    {
        UNKNOWN_TYPE = 0,
        ALTER_TABLE,
        CREATE_TABLE,
        DROP_TABLE,
        RENAME_TABLE,
        TRUNCATE_TABLE,
    };

    DDLQueryType query_type{UNKNOWN_TYPE};
    String query;
    String execute_database;
    String execute_table;                   /// The table name to execute DDL query
    String rename_to_database;
    String rename_to_table;                 /// The new table name for RENAME TABLE query
};

struct MySQLSyncStatusInfo
{
    /// Sync info
    String mysql_info;
    String mysql_database;
    String sync_type;
    NameSet include_tables;
    NameSet exclude_tables;
    NameSet resync_tables;
    String total_position;
    /// Sync thread info
    size_t threads_num{0};
    Strings materialized_tables;
    Strings sync_thread_names;
    Strings binlog_infos;
    Strings sync_thread_clients;
    Strings broken_threads;
    /// Sync exception info
    String last_exception;
    NameSet sync_failed_tables;
    NameSet skipped_unsupported_tables;
};

MySQLDDLQueryParams parseMySQLDDLQuery(const String & ddl);

String getNameForMaterializedMySQLManager(const UUID & database_uuid);
String getNamePrefixForMaterializedBinlog(const UUID & database_uuid);
String getNameForMaterializedBinlog(const UUID & database_uuid, const String & table_name);

namespace MaterializedMySQL
{
    void checkMySQLVariables(const mysqlxx::Pool::Entry & connection, const Settings & settings);
    ContextMutablePtr createQueryContext(ContextPtr context, bool init_query = false);
    MaterializedMySQLLogElement createMaterializeMySQLLog(MaterializedMySQLLogElement::EventSource event_source, MaterializedMySQLLogElement::Type type, String database, NameSet tables = {});
    void recordException(MaterializedMySQLLogElement::EventSource event_source, ContextPtr context, String database, NameSet tables = {}, bool create_log = true, const String & resync_table = "");
}

} /// namespace DB

#endif
