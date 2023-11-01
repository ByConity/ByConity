#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#include <common/types.h>
#include <Core/MySQL/MySQLReplication.h>
#include <Databases/MySQL/MaterializedMySQLCommon.h>
#include <mysqlxx/Connection.h>
#include <mysqlxx/PoolWithFailover.h>
#include <Interpreters/Context.h>

namespace DB
{

/** Materialize database engine metadata
 *
 * Record data version and current snapshot of MySQL, including:
 * binlog_file  - currently executing binlog_file
 * binlog_position  - position of the currently executing binlog file
 * executed_gtid_set - currently executing gtid
 * need_dumping_tables - Table structure snapshot at the current moment(Only when database first created or executed binlog file is deleted)
 */
struct MaterializeMetadata
{
    const String persistent_path;
    const Settings settings;

    String binlog_file;
    UInt64 binlog_position;
    String binlog_do_db;
    String binlog_ignore_db;
    String executed_gtid_set;

    size_t meta_version = 2;
    String binlog_checksum = "CRC32";

    void fetchMasterOrSlaveStatus(mysqlxx::PoolWithFailover::Entry & connection, bool is_slave);

    void fetchMasterVariablesValue(const mysqlxx::PoolWithFailover::Entry & connection);

    NameSet getBinlogFilesFromMySQL(const mysqlxx::PoolWithFailover::Entry & connection);

    void transaction(const MySQLReplication::Position & position, const std::function<void()> & fun);

    /** Get materialized tables with corresponding create query from MySQL. This should only called once
     * @param connection               connection to MySQL
     * @param database                 mysql database name
     * @param expected_tables_list     a tables list set by user implying which tables they want to sync; it can be empty
     * @param tables_with_create_query result of the function with pairs {table-name, table-create-sql}
     * */
    void getTablesWithCreateQueryFromMySql(
        mysqlxx::PoolWithFailover::Entry & connection,
        const String & database,
        bool & opened_transaction,
        std::unordered_set<String> & expected_tables_list,
        std::unordered_map<String, String> & tables_with_create_query,
        bool is_readonly_replica = false);

    static std::vector<String> fetchTablesInDB(const mysqlxx::PoolWithFailover::Entry & connection, const std::string & database, const Settings & global_settings);

    explicit MaterializeMetadata(const Settings & settings_)
        : settings(settings_)
    {}

    MaterializeMetadata(const MySQLBinLogInfo & binlog_info, const Settings & settings_)
        : settings(settings_)
        , binlog_file(binlog_info.binlog_file)
        , binlog_position(binlog_info.binlog_position)
        , executed_gtid_set(binlog_info.executed_gtid_set)
        , meta_version(binlog_info.meta_version)
    {}
};

}

#endif
