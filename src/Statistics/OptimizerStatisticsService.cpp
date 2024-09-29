#include <Core/SettingsEnums.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/SQLBinding/SQLBindingCache.h>
#include <Processors/Exchange/DataTrans/Brpc/ReadBufferFromBrpcBuf.h>
#include <Processors/Exchange/DataTrans/Brpc/WriteBufferFromBrpcBuf.h>
#include <Protos/RPCHelpers.h>
#include <Protos/optimizer_statistics.pb.h>
#include <Statistics/AutoStatisticsManager.h>
#include <Statistics/AutoStatisticsMemoryRecord.h>
#include <Statistics/CacheManager.h>
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/CatalogAdaptorProxy.h>
#include <Statistics/OptimizerStatisticsClient.h>
#include <Statistics/OptimizerStatisticsService.h>
#include <Statistics/StatsTableIdentifier.h>
#include <Common/Stopwatch.h>
#include <Common/tests/gtest_global_context.h>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int LEADERSHIP_CHANGED;
}

namespace DB::Statistics
{
void OptimizerStatisticsService::refreshStatisticsCache(
    google::protobuf::RpcController * cntl,
    const Protos::RefreshStatisticsCacheRequest * request,
    Protos::RefreshStatisticsCacheResponse * response,
    google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    [[maybe_unused]] brpc::Controller * brpc_cntl = static_cast<brpc::Controller *>(cntl);
    try
    {
        auto context = getContext();
        using Mode = Protos::RefreshStatisticsCacheRequest::Mode;
        if (request->mode() == Mode::SyncTable)
        {
            // DO NOTHING
            return;
        }

        for (const auto & table_pb : request->tables())
        {
            auto storage_id = RPCHelpers::createStorageID(table_pb);
            if (request->mode() == Mode::Invalidate)
            {
                // high likely that table has been dropped
                // so we just invalidate without checking existence
                LOG_INFO(log, "invalidating statistics cache {} on this server", storage_id.getNameForLogs());
                CacheManager::invalidate(context, StatsTableIdentifier(storage_id));
                return;
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

void OptimizerStatisticsService::fetchStatisticsSettings(
    google::protobuf::RpcController * cntl,
    const Protos::FetchStatisticsSettingsRequest * request,
    Protos::FetchStatisticsSettingsResponse * response,
    google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    [[maybe_unused]] brpc::Controller * brpc_cntl = static_cast<brpc::Controller *>(cntl);
    try
    {
        auto context = getContext();
        (void)request;
        auto * manager = context->getAutoStatisticsManager();

        auto settings = manager->getSettingsManager().getFullStatisticsSettings();
        settings.toProto(*response->mutable_statistics_settings());
    }
    catch (...)
    {
        tryLogCurrentException(log);
        RPCHelpers::handleException(response->mutable_exception());
    }
}
}
