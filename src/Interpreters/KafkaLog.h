#pragma once

#include <Interpreters/SystemLog.h>


namespace DB
{

struct KafkaLogElement
{
    enum Type
    {
        EMPTY = 0,
        POLL = 1,
        PARSE_ERROR = 2,
        WRITE = 3,
        EXCEPTION = 4,
        EMPTY_MESSAGE = 5,
        FILTER = 6,
    };
    Type event_type = EMPTY;

    time_t event_time = 0;
    UInt32 duration_ms = 0;

    String database;
    String table;
    String consumer;

    UInt64 metric = 0;
    UInt64 bytes = 0;

    UInt8 has_error = 0;
    String last_exception;

    static std::string name() { return "KafkaLog"; }
    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

/// Instead of typedef - to allow forward declaration.
class KafkaLog : public SystemLog<KafkaLogElement>
{
    using SystemLog<KafkaLogElement>::SystemLog;
};

}
