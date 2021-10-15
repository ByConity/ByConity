#pragma once

#include <Interpreters/SystemLog.h>
#include <Core/NamesAndAliases.h>

namespace DB
{

struct MutationLogElement
{
    enum Type
    {
        MUTATION_START = 1,
        MUTATION_KILL = 2,
        MUTATION_FINISH = 3,
        MUTATION_ABORT = 4,
    };

    Type event_type = MUTATION_START;
    time_t event_time = 0;
    String database_name;
    String table_name;
    String mutation_id;
    String query_id;
    time_t create_time = 0;
    UInt64 block_number = 0;
    Strings commands;

    static std::string name() { return "MutationLog"; }

    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }

    void appendToBlock(MutableColumns & columns) const;
};

/// Instead of typedef - to allow forward declaration.
class MutationLog : public SystemLog<MutationLogElement>
{
    using SystemLog<MutationLogElement>::SystemLog;
};

}
