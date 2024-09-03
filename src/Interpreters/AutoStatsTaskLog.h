#pragma once

#include <Interpreters/SystemLog.h>
#include <Statistics/AutoStatisticsHelper.h>
#include <Core/NamesAndAliases.h>
#include <Interpreters/SystemLog.h>
#include <Interpreters/ClientInfo.h>

/// Call this function on AutoStatsTask.


namespace DB
{

/** Information about AutoStatsTask.
  * Before AutoStatsTask we are writing info into system table for further analysis.
  */
struct AutoStatsTaskLogElement
{
    DateTime64 event_time = 0;
    UUID task_uuid;
    Statistics::AutoStats::TaskType task_type;
    UUID table_uuid;
    std::vector<String> columns_name; // empty means implicit all
    String database_name;
    String table_name;
    UInt64 stats_row_count;
    UInt64 udi;
    double priority;
    UInt64 retry;
    Statistics::AutoStats::Status status;
    String settings_json;
    String message;

    static std::string name() { return "AutoStatsTaskLog"; }
    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class AutoStatsTaskLog : public SystemLog<AutoStatsTaskLogElement>
{
    using SystemLog<AutoStatsTaskLogElement>::SystemLog;
};

class CnchAutoStatsTaskLog : public CnchSystemLog<AutoStatsTaskLogElement>
{
    using CnchSystemLog<AutoStatsTaskLogElement>::CnchSystemLog;
};

}
