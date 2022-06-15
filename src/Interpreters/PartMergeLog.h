#pragma once

#include <Interpreters/SystemLog.h>

namespace DB
{
struct PartMergeLogElement
{
    enum Type
    {
        MERGE_SELECT = 1,
        COMMIT = 2,
    };

    Type event_type = MERGE_SELECT;
    time_t event_time = 0;

    String database;
    String table;
    UUID uuid;

    UInt32 new_tasks = 0;
    UInt32 source_parts_in_new_tasks = 0;

    UInt64 duration_us = 0;
    UInt64 get_parts_duration_us = 0;
    UInt64 select_parts_duration_us = 0;

    String exception;

    /// extended
    bool extended = false;
    UInt32 current_parts = 0;
    UInt32 future_covered_parts = 0;
    UInt32 future_committed_parts = 0; /// current_tasks
    /// future_final_parts = current_parts - future_covered_parts + future_committed_parts

    static std::string name() { return "PartMergeLogElement"; }
    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class PartMergeLog : public SystemLog<PartMergeLogElement>
{
    using SystemLog<PartMergeLogElement>::SystemLog;
};

}
