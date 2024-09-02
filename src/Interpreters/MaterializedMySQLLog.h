#pragma once

#include <Core/Names.h>
#include <Core/NamesAndAliases.h>
#include <Core/NamesAndTypes.h>
#include <Columns/IColumn.h>

namespace DB
{
/** Allows to log information about materialized mysql execution:
  * - info about start of query execution;
  * - info about errors of query execution.
  */

/// A struct which will be inserted as row into cnch_materialized_mysql_log table
struct MaterializedMySQLLogElement
{
    enum Type
    {
        EMPTY = 0,
        ERROR = 1,
        START_SYNC = 2,
        MANUAL_STOP_SYNC = 3,
        EXCEPTION_STOP_SYNC = 4,
        RESYNC_TABLE = 5,
        SKIP_DDL = 6,
    };

    /**
     * Used to help determine the source of sync_failed_tables and skipped_unsupported_tables
     */
    enum EventSource
    {
        SYNC_MANAGER  = 0,
        WORKER_THREAD = 1,
    };

    String cnch_database;
    NameSet cnch_tables;
    String database;
    NameSet tables;

    Type type;
    time_t event_time{};
    String resync_table;

    /// some reserved fields, currently not used
    UInt64 duration_ms = 0;
    UInt64 metric = 0;

    UInt8 has_error = 0;
    String event_msg;
    EventSource event_source{SYNC_MANAGER};

    static std::string name() { return "MaterializedMySQLLog"; }
    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;

};

}
