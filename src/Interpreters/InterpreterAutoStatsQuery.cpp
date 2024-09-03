#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterAutoStatsQuery.h>
#include <Parsers/ASTAutoStatsQuery.h>
#include <Statistics/ASTHelpers.h>
#include <Statistics/AutoStatisticsCommand.h>
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/CatalogAdaptorProxy.h>
#include <Statistics/DropHelper.h>
#include <Statistics/StatsTableBasic.h>
#include "Common/Brpc/BrpcChannelPoolOptions.h"
#include "Common/HostWithPorts.h"
#include "Common/SettingsChanges.h"
#include "Core/QueryProcessingStage.h"
#include "DataStreams/IBlockInputStream.h"
#include "DataStreams/OneBlockInputStream.h"
#include "DataStreams/RemoteQueryExecutor.h"
#include "IO/WriteBufferFromString.h"
#include "Interpreters/SegmentScheduler.h"
#include "Interpreters/executeQuery.h"
#include "Interpreters/getClusterName.h"
#include "Parsers/formatAST.h"
#include "Processors/Exchange/DataTrans/RpcChannelPool.h"
#include "Processors/Sources/RemoteSource.h"
#include "Statistics/SettingsManager.h"

namespace DB
{
using namespace Statistics;
using namespace Statistics::AutoStats;

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
    extern const int UNKNOWN_DATABASE;
    extern const int LEADERSHIP_CHANGED;
}

String formatASTAsString(const IAST & ast)
{
    WriteBufferFromOwnString buf;
    formatAST(ast, buf, false, true);
    return buf.str();
}

String constrcutScopePredicate(const StatisticsScope & scope)
{
    if (!scope.database)
    {
        return "1";
    }
    auto db_str = quoteString(scope.database.value());
    if (!scope.table)
    {
        return fmt::format("database={}", db_str);
    }
    auto tb_str = quoteString(scope.table.value());
    return fmt::format("database={} and table={}", db_str, tb_str);
}

String constructShowSql(ContextPtr context, const StatisticsScope & scope)
{
    (void) context;
    String sql = R"(
    select
        database,
        table,
        table_uuid,
        row_count,
        timestamp,
        status,
        priority,
        retry_times
    from
        (
            select
                *
            from
                system.statistics_table_view
            where
                {0}
        )
        left join (
            select
                table_uuid,
                status,
                priority,
                retry_times
            from
                cnch(server, system.auto_stats_manager_status)
            where
                {0}
        ) using table_uuid
    )";
    auto predicate = constrcutScopePredicate(scope);
    return fmt::format(sql, predicate);
}

BlockIO InterpreterAutoStatsQuery::execute()
{
    // TODO foward the query
    auto context = getContext();
    auto * manager = context->getAutoStatisticsManager();
    if (!manager)
    {
        throw Exception("auto stats manager is not initialized", ErrorCodes::NOT_IMPLEMENTED);
    }

    // auto command = AutoStatisticsCommand(getContext());
    const auto * query = query_ptr->as<ASTAutoStatsQuery>();
    using Prefix = ASTAutoStatsQuery::QueryPrefix;
    auto scope = scopeFromAST(context, query);

    AutoStatisticsCommand command(context);
    switch (query->prefix)
    {
        case Prefix::Create: {
            auto changes = query->settings_changes_opt.value_or(SettingsChanges{});
            command.create(scope, std::move(changes));
            break;
        }

        case Prefix::Drop: {
            command.drop(scope);
            break;
        }

        case Prefix::Alter: {
            command.alter(query->settings_changes_opt.value());
            break;
        }

        case Prefix::Show: {
            auto sql = constructShowSql(context, scope);
            auto io = executeQuery(sql, context, true);
            return io;
        }
        default:
            throw Exception("unimplemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    return {};
}
}
