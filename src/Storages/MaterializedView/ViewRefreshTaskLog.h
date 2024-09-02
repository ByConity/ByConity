#pragma once
#include <Interpreters/SystemLog.h>

namespace DB
{

struct ViewRefreshTaskLogElement
{
    String database;
    String view;
    RefreshViewTaskStatus status = RefreshViewTaskStatus::START;
    RefreshViewTaskType refresh_type = RefreshViewTaskType::NONE;
    time_t event_date;
    time_t event_time;
    String partition_map;
    UInt64 query_duration_ms;
    String drop_query;
    String insert_select_query;
    String insert_overwrite_query;
    String query_id;
    String drop_query_id;
    String insert_select_query_id;
    String insert_overwrite_query_id;
    String exception;
    static std::string name() { return "CnchViewRefreshTask"; }
    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class ViewRefreshTaskLog : public CnchSystemLog<ViewRefreshTaskLogElement>
{
    using CnchSystemLog<ViewRefreshTaskLogElement>::CnchSystemLog;
};

}

