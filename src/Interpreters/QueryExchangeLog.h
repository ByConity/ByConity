#pragma once

#include <Interpreters/SystemLog.h>
#include <Interpreters/ClientInfo.h>

namespace ProfileEvents
{
    class Counters;
}

namespace DB
{
struct QueryExchangeLogElement
{
    String initial_query_id{"-1"};
    String write_segment_id{"-1"};
    String read_segment_id{"-1"};
    String partition_id{"-1"};
    String coordinator_address{};
    time_t event_time{};

    Int32 finish_code{};
    Int8 is_modifier{};
    String message;
    String type;

    // send metric
    UInt64 send_time_ms{};
    UInt64 send_rows{};
    UInt64 send_bytes{};
    UInt64 send_uncompressed_bytes{};
    UInt64 num_send_times{};
    UInt64 ser_time_ms{};
    UInt64 send_retry{};
    UInt64 send_retry_ms{};
    UInt64 overcrowded_retry{};

    // recv metric
    UInt64 recv_time_ms{};
    UInt64 register_time_ms{};
    UInt64 recv_rows{};
    UInt64 recv_bytes{};
    UInt64 dser_time_ms{};


    std::shared_ptr<ProfileEvents::Counters> profile_counters;

    static std::string name() { return "QueryExchangeLog"; }

    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases();
    void appendToBlock(MutableColumns & columns) const;
};


class QueryExchangeLog : public SystemLog<QueryExchangeLogElement>
{
    using SystemLog<QueryExchangeLogElement>::SystemLog;
};

}
