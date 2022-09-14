#pragma once
#include <Core/Types.h>
#include <Poco/Net/IPAddress.h>
#include <Parsers/IAST.h>
#include <Interpreters/Context.h>

namespace DB
{

using QueryMetricLogState = QueryLogElementType;
struct QueryStatusInfo;
struct BlockStreamProfileInfo;

struct QueryMetricLogType
{
    enum QueryType
    {
        UNKNOWN,
        CREATE,
        DROP,
        RENAME,
        SELECT,
        INSERT,
        DELETE,
        ALTER,
        OTHER,
    };

    QueryType type;

    explicit QueryMetricLogType(QueryType type_ = UNKNOWN)
        : type(type_) {}

    explicit QueryMetricLogType(UInt8 type_)
        : type(QueryType(type_)) {}

    explicit QueryMetricLogType(const ASTPtr & ast);

    String toString() const
    {
        switch (type)
        {
            case UNKNOWN:
                return "UNKNOWN";
            case CREATE:
                return "CREATE";
            case DROP:
                return "DROP";
            case RENAME:
                return "RENAME";
            case SELECT:
                return "SELECT";
            case INSERT:
                return "INSERT";
            case DELETE:
                return "DELETE";
            case ALTER:
                return "ALTER";
            case OTHER:
                return "OTHER";
        }
        return "";
    }
};

void extractDatabaseAndTableNames(const Context & context, const ASTPtr & ast, String & database, String & table);

void insertCnchQueryMetric(
    ContextMutablePtr context,
    const String & query,
    QueryMetricLogState state,
    time_t current_time,
    const ASTPtr & ast = nullptr,
    const QueryStatusInfo * info = nullptr,
    const BlockStreamProfileInfo * stream_in_info = nullptr,
    const QueryPipeline * query_pipeline = nullptr,
    bool empty_stream = false,
    UInt8 complex_query = 0,
    UInt32 init_time = 0,
    const String & exception = {},
    const String & stack_trace = {});

}
