#pragma once

#include <Core/Defines.h>
#include <Core/BaseSettings.h>

namespace DB
{

class ASTStorage;

#define LIST_OF_MATERIALIZE_MODE_SETTINGS(M) \
    M(UInt64, max_rows_in_buffer, DEFAULT_BLOCK_SIZE, "Max rows that data is allowed to cache in memory(for single table and the cache data unable to query). when rows is exceeded, the data will be materialized", 0) \
    M(UInt64, max_bytes_in_buffer, DBMS_DEFAULT_BUFFER_SIZE, "Max bytes that data is allowed to cache in memory(for single table and the cache data unable to query). when rows is exceeded, the data will be materialized", 0) \
    M(UInt64, max_rows_in_buffers, DEFAULT_BLOCK_SIZE, "Max rows that data is allowed to cache in memory(for database and the cache data unable to query). when rows is exceeded, the data will be materialized", 0) \
    M(UInt64, max_bytes_in_buffers, DBMS_DEFAULT_BUFFER_SIZE, "Max bytes that data is allowed to cache in memory(for database and the cache data unable to query). when rows is exceeded, the data will be materialized", 0) \
    M(UInt64, max_flush_data_time, 8000, "Max milliseconds that data is allowed to cache in memory(for database and the cache data unable to query). when this time is exceeded, the data will be materialized", 0)  \
    M(Int64, max_wait_time_when_mysql_unavailable, 1000, "Retry interval when MySQL is not available (milliseconds). Negative value disable retry.", 0) \
    M(Bool, allows_query_when_mysql_lost, false, "Allow query materialized table when mysql is lost.", 0) \
    M(Int64, skip_error_count, -1, "Skip errors in the synchronization of materialized mysql data(for single table). A negative value will skip all errors. Other values will skip specific errors.", 0) \
    M(MultiRegexString, include_tables, "", "If this parameter is configured, only qualified tables will be synchronized. Tables name are separated by commas. The table name supports regular expressions. User can set either include_tables or exclude_tables, if both parameters are set, an error will be thrown. If both parameters are not set, database will synchronize all tables.", 0) \
    M(MultiRegexString, exclude_tables, "", "If this parameter is configured, all qualified tables will not be synchronized. Tables name are separated by commas. The table name supports regular expressions. User can set either include_tables or exclude_tables, if both parameters are set, an error will be thrown. If both parameters are not set, database will synchronize all tables.", 0) \
    M(MultiRegexString, skip_ddl_patterns, "", "If this parameter is configured, all qualified ddl query will not be executed. DDL patterns are separated by commas. DDL pattern supports regular expressions and is case-insensitive. If this parameter is not set or empty, all DDL queries will be executed.", 0) \
    M(Bool, skip_unsupported_tables, true, "Whether to skip unsupported tables, for example, no primary key.", 0) \
    M(Bool, skip_sync_failed_tables, false, "Whether to skip sync failed tables in order to not block whole sync process.", 0) \
    M(UInt64, resync_table_task_schedule_time_ms, 1000, "Schedule milliseconds interval of resync table task.", 0) \
    M(UInt64, resync_table_task_fail_retry_time, 5, "Retry time when resync table failed.", 0) \
    M(UInt64, sync_thread_max_heartbeat_interval_second, 30, "If the heartbeat failed for such a long time, the sync thread should be closed", 0) \
    M(UInt64, sync_thread_heartbeat_interval_ms, 5000, "Interval for heartbeat task to check", 0) \
    M(UInt64, sync_thread_schedule_interval_ms, 500, "Interval for sync-thread to schedule for each iteration", 0) \
    M(String, read_only_replica_info, "", "Used for dumpping aws rds data for initialization; Connection info (e.g host, port, user, password, etc) with Json format", 0) \
    M(String, cnch_vw_write, "vw_write", "", 0) \
    M(String, cnch_vw_default, "vw_default", "", 0) \

    DECLARE_SETTINGS_TRAITS(MaterializeMySQLSettingsTraits, LIST_OF_MATERIALIZE_MODE_SETTINGS)


/** Settings for the MaterializeMySQL database engine.
  * Could be loaded from a CREATE DATABASE query (SETTINGS clause).
  */
struct MaterializeMySQLSettings : public BaseSettings<MaterializeMySQLSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}
