#pragma once

#include <Core/NamesAndAliases.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/SystemLog.h>
#include <chrono>
#include <ctime>


namespace DB
{

struct RemoteReadLogElement
{
    time_t event_time{};
    Decimal64 request_time_microseconds{};
    String context;
    String query_id;
    UInt64 txn_id{};
    String thread_name;
    UInt64 thread_id{};
    String path;
    UInt64 offset{};
    Int64 size{-1}; /// -1 means unknown
    UInt64 duration_us{};

    static std::string name() { return "RemoteReadLog"; }
    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};


class RemoteReadLog : public SystemLog<RemoteReadLogElement>
{
    using SystemLog<RemoteReadLogElement>::SystemLog;

public:
    void insert(std::chrono::time_point<std::chrono::system_clock> request_time, const String & path, UInt64 offset, Int64 size, UInt64 duration_us, const String & read_context = {});
};

}
