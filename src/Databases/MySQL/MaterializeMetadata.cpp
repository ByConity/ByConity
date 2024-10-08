#include <Databases/MySQL/MaterializeMetadata.h>

#if USE_MYSQL

#include <Core/Block.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/MySQLBlockInputStream.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <Common/quoteString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SYNC_MYSQL_USER_ACCESS_ERROR;
}

static std::unordered_map<String, String> fetchTablesCreateQuery(
    const mysqlxx::PoolWithFailover::Entry & connection, const String & database_name,
    const std::vector<String> & fetch_tables, std::unordered_set<String> & materialized_tables_list,
    const Settings & global_settings)
{
    std::unordered_map<String, String> tables_create_query;
    for (const auto & fetch_table_name : fetch_tables)
    {
        if (!materialized_tables_list.contains(fetch_table_name))
        {
            LOG_INFO(getLogger("fetchTablesCreateQuery"), "Skip table " + fetch_table_name + " as it is not in materialized_table_list");
            continue;
        }

        Block show_create_table_header{
            {std::make_shared<DataTypeString>(), "Table"},
            {std::make_shared<DataTypeString>(), "Create Table"},
        };

        StreamSettings mysql_input_stream_settings(global_settings, false, true);
        MySQLBlockInputStream show_create_table(
            connection, "SHOW CREATE TABLE " + backQuoteIfNeed(database_name) + "." + backQuoteIfNeed(fetch_table_name),
            show_create_table_header, mysql_input_stream_settings);

        Block create_query_block = show_create_table.read();
        if (!create_query_block || create_query_block.rows() != 1)
            throw Exception("LOGICAL ERROR mysql show create return more rows.", ErrorCodes::LOGICAL_ERROR);

        tables_create_query[fetch_table_name] = create_query_block.getByName("Create Table").column->getDataAt(0).toString();
    }

    return tables_create_query;
}

std::vector<String> MaterializeMetadata::fetchTablesInDB(const mysqlxx::PoolWithFailover::Entry & connection, const std::string & database, const Settings & global_settings)
{
    Block header{{std::make_shared<DataTypeString>(), "table_name"}};
    String query = "SELECT TABLE_NAME AS table_name FROM INFORMATION_SCHEMA.TABLES  WHERE TABLE_TYPE != 'VIEW' AND TABLE_SCHEMA = " + quoteString(database);

    std::vector<String> tables_in_db;
    StreamSettings mysql_input_stream_settings(global_settings);
    MySQLBlockInputStream input(connection, query, header, mysql_input_stream_settings);

    while (Block block = input.read())
    {
        tables_in_db.reserve(tables_in_db.size() + block.rows());
        for (size_t index = 0; index < block.rows(); ++index)
            tables_in_db.emplace_back((*block.getByPosition(0).column)[index].safeGet<String>());
    }

    for (const auto & table : tables_in_db)
        LOG_INFO(getLogger("fetchTablesInDB"), "Fetched table from MySQL : " + database + "." + table);

    return tables_in_db;
}

static Block getHeaderOfMaterOrSlaveStatus(bool is_slave = false)
{
    if (is_slave)
    {
        /// See: https://dev.mysql.com/doc/refman/5.7/en/show-slave-status.html
        return Block{
                {std::make_shared<DataTypeString>(), "Master_Log_File"},
                {std::make_shared<DataTypeUInt64>(), "Exec_Master_Log_Pos"},
                {std::make_shared<DataTypeString>(), "Replicate_Do_DB"},
                {std::make_shared<DataTypeString>(), "Replicate_Ignore_DB"},
                {std::make_shared<DataTypeString>(), "Executed_Gtid_Set"},
               };
    }
    else
    {
        return Block{
                {std::make_shared<DataTypeString>(), "File"},
                {std::make_shared<DataTypeUInt64>(), "Position"},
                {std::make_shared<DataTypeString>(), "Binlog_Do_DB"},
                {std::make_shared<DataTypeString>(), "Binlog_Ignore_DB"},
                {std::make_shared<DataTypeString>(), "Executed_Gtid_Set"},
               };
    }
}

void MaterializeMetadata::fetchMasterOrSlaveStatus(mysqlxx::PoolWithFailover::Entry & connection, bool is_slave)
{
    Block header = getHeaderOfMaterOrSlaveStatus(is_slave);

     /// TODO: `SHOW REPLICA STATUS` is used for mysql-8.0,
    /// coresponding variables: Source_Log_File, Exec_Source_Log_Pos
    String fetch_status_sql;
    if (is_slave)
        fetch_status_sql = "SHOW SLAVE STATUS;";
    else
        fetch_status_sql = "SHOW MASTER STATUS;";

    StreamSettings mysql_input_stream_settings(settings, false, true);
    MySQLBlockInputStream input(connection, fetch_status_sql, header, mysql_input_stream_settings);
    Block master_status = input.read();

    if (!master_status || master_status.rows() != 1)
        throw Exception("Unable to get master status from MySQL.", ErrorCodes::LOGICAL_ERROR);

    binlog_file = (*master_status.getByPosition(0).column)[0].safeGet<String>();
    binlog_position = (*master_status.getByPosition(1).column)[0].safeGet<UInt64>();
    binlog_do_db = (*master_status.getByPosition(2).column)[0].safeGet<String>();
    binlog_ignore_db = (*master_status.getByPosition(3).column)[0].safeGet<String>();
    executed_gtid_set = (*master_status.getByPosition(4).column)[0].safeGet<String>();
}

void MaterializeMetadata::fetchMasterVariablesValue(const mysqlxx::PoolWithFailover::Entry & connection)
{
    Block variables_header{
        {std::make_shared<DataTypeString>(), "Variable_name"},
        {std::make_shared<DataTypeString>(), "Value"}
    };

    const String & fetch_query = "SHOW VARIABLES WHERE Variable_name = 'binlog_checksum'";
    StreamSettings mysql_input_stream_settings(settings, false, true);
    MySQLBlockInputStream variables_input(connection, fetch_query, variables_header, mysql_input_stream_settings);

    while (Block variables_block = variables_input.read())
    {
        ColumnPtr variables_name = variables_block.getByName("Variable_name").column;
        ColumnPtr variables_value = variables_block.getByName("Value").column;

        for (size_t index = 0; index < variables_block.rows(); ++index)
        {
            if (variables_name->getDataAt(index) == "binlog_checksum")
                binlog_checksum = variables_value->getDataAt(index).toString();
        }
    }
}

static bool checkSyncUserPrivImpl(const mysqlxx::PoolWithFailover::Entry & connection, const Settings & global_settings, WriteBuffer & out)
{
    Block sync_user_privs_header
    {
        {std::make_shared<DataTypeString>(), "current_user_grants"}
    };

    String grants_query, sub_privs;
    StreamSettings mysql_input_stream_settings(global_settings);
    MySQLBlockInputStream input(connection, "SHOW GRANTS FOR CURRENT_USER();", sync_user_privs_header, mysql_input_stream_settings);
    while (Block block = input.read())
    {
        for (size_t index = 0; index < block.rows(); ++index)
        {
            grants_query = (*block.getByPosition(0).column)[index].safeGet<String>();
            out << grants_query << "; ";
            sub_privs = grants_query.substr(0, grants_query.find(" ON "));
            if (sub_privs.find("ALL PRIVILEGES") == std::string::npos)
            {
                if ((sub_privs.find("RELOAD") != std::string::npos and
                    sub_privs.find("REPLICATION SLAVE") != std::string::npos and
                    sub_privs.find("REPLICATION CLIENT") != std::string::npos))
                    return true;
            }
            else
            {
                return true;
            }
        }
    }
    return false;
}

static void checkSyncUserPriv(const mysqlxx::PoolWithFailover::Entry & connection, const Settings & global_settings)
{
    WriteBufferFromOwnString out;

    if (!checkSyncUserPrivImpl(connection, global_settings, out))
        throw Exception("MySQL SYNC USER ACCESS ERR: mysql sync user needs "
                        "at least GLOBAL PRIVILEGES:'RELOAD, REPLICATION SLAVE, REPLICATION CLIENT' "
                        "and SELECT PRIVILEGE on MySQL Database."
                        "But the SYNC USER grant query is: " + out.str(), ErrorCodes::SYNC_MYSQL_USER_ACCESS_ERROR);
}

NameSet MaterializeMetadata::getBinlogFilesFromMySQL(const mysqlxx::PoolWithFailover::Entry & connection)
{
    checkSyncUserPriv(connection, settings);

    Block logs_header {
        {std::make_shared<DataTypeString>(), "Log_name"},
        {std::make_shared<DataTypeUInt64>(), "File_size"}
    };

    StreamSettings mysql_input_stream_settings(settings, false, true);
    MySQLBlockInputStream input(connection, "SHOW MASTER LOGS", logs_header, mysql_input_stream_settings);

    NameSet res;
    while (Block block = input.read())
    {
        for (size_t index = 0; index < block.rows(); ++index)
        {
            const auto log_name = (*block.getByPosition(0).column)[index].safeGet<String>();
            res.emplace(log_name);
        }
    }
    return res;
}

void commitMetadata(const std::function<void()> & function, const String & persistent_tmp_path, const String & persistent_path)
{
    try
    {
        function();
        fs::rename(persistent_tmp_path, persistent_path);
    }
    catch (...)
    {
        fs::remove(persistent_tmp_path);
        throw;
    }
}

void MaterializeMetadata::transaction(const MySQLReplication::Position & position, const std::function<void()> & fun)
{
    binlog_file = position.binlog_name;
    binlog_position = position.binlog_pos;
    executed_gtid_set = position.gtid_sets.toString();

    if (!persistent_path.empty())
    {
        String persistent_tmp_path = persistent_path + ".tmp";

        {
            WriteBufferFromFile out(persistent_tmp_path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_TRUNC | O_CREAT);

            /// TSV format metadata file.
            writeString("Version:\t" + toString(meta_version), out);
            writeString("\nBinlog File:\t" + binlog_file, out);
            writeString("\nExecuted GTID:\t" + executed_gtid_set, out);
            writeString("\nBinlog Position:\t" + toString(binlog_position), out);

            out.next();
            out.sync();
            out.close();
        }

        commitMetadata(std::move(fun), persistent_tmp_path, persistent_path);
    }
    else
        fun();
}

void MaterializeMetadata::getTablesWithCreateQueryFromMySql(mysqlxx::PoolWithFailover::Entry & connection,
                                                            const String & database,
                                                            bool & opened_transaction,
                                                            std::unordered_set<String> & expected_tables_list,
                                                            std::unordered_map<String, String> & tables_with_create_query,
                                                            bool is_readonly_replica)
{
    checkSyncUserPriv(connection, settings);

    if (!is_readonly_replica)
    {
        connection->query("FLUSH TABLES;").execute();
        connection->query("FLUSH TABLES WITH READ LOCK;").execute();
    }
    else
    {
        /// AWS mysql does NOT support super privilege, so we need to dump data from a read-only replica.
        /// See: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/MySQL.Procedural.Exporting.NonRDSRepl.html
        connection->query("CALL mysql.rds_set_configuration('binlog retention hours', 12);").execute();
        connection->query("CALL mysql.rds_stop_replication;").execute();
    }

    SCOPE_EXIT({
        try
        {
            if (!is_readonly_replica)
                connection->query("UNLOCK TABLES;").execute();
        }
        catch (...)
        {
            tryLogCurrentException(getLogger("GetTablesFromMysql"),
                                   "Failed to Unlock tables while getting tables list from mysql " + database);
        }
    });

    /// Get current Binlog info by `SHOW MASTER STATUS` or `SHOW SLAVE STATUS`
    fetchMasterOrSlaveStatus(connection, is_readonly_replica);

    /// Get binlog_checksum value
    /// TODO: Make sure that `SHOW VARIABLES` command works for replica
    fetchMasterVariablesValue(connection);

    connection->query("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;").execute();
    connection->query("START TRANSACTION /*!40100 WITH CONSISTENT SNAPSHOT */;").execute();

    opened_transaction = true;

    /// 1. get all tables in mysql: `fetchTablesInDB`, not include VIEW
    /// 2. filter table if it is not in un-empty `expected_tables_list`
    /// 3. get create query for each table
    tables_with_create_query = fetchTablesCreateQuery(connection, database, fetchTablesInDB(connection, database, settings),
                                                      expected_tables_list, settings);
}

}

#endif
