#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/getClusterName.h>
#include <Processors/Exchange/DataTrans/RpcChannelPool.h>
#include <Protos/RPCHelpers.h>
#include <Protos/optimizer_statistics.pb.h>
#include <Statistics/AutoStatisticsManager.h>
#include <Statistics/StatisticsSettings.h>
#include <Statistics/SubqueryHelper.h>
#include <fmt/format.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>
#include "DaemonManager/DaemonJobAutoStatistics.h"
#include "Statistics/StatsTableIdentifier.h"

namespace DB::Statistics
{
void refreshClusterStatsCache(ContextPtr context, const StatsTableIdentifier & table_identifier, bool is_drop)
{
    (void)context;
    (void)table_identifier;
    (void)is_drop;

    using Mode = Protos::RefreshStatisticsCacheRequest::Mode;

    std::vector<Mode::Enum> modes{Mode::SyncTable, Mode::Invalidate};
    for (auto mode : modes)
    {
        Protos::RefreshStatisticsCacheRequest req;
        RPCHelpers::fillStorageID(table_identifier.getStorageID(), *req.add_tables());
        auto log = getLogger("refreshClusterStatsCache");
        req.set_mode(mode);

        LOG_INFO(log, "refresh statistics on {}", table_identifier.getNameForLogs());

        auto servers = DaemonManager::DaemonJobAutoStatistics::getServerList(context);

        for (const auto & addr : servers)
        {
            auto addr_text = addr.getRPCAddress();
            try
            {
                brpc::Controller cntl;
                LOG_TRACE(log, "send refresh request to {}", addr_text);
                // auto rpc_client = RpcChannelPool::getInstance().getClient(addr_text, BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY);

                std::shared_ptr<RpcClient> rpc_client
                    = RpcChannelPool::getInstance().getClient(addr_text, BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY);

                auto stub_ptr = std::make_shared<Protos::OptimizerStatisticsService_Stub>(&rpc_client->getChannel());

                Protos::RefreshStatisticsCacheResponse resp;
                stub_ptr->refreshStatisticsCache(&cntl, &req, &resp, nullptr);

                rpc_client->assertController(cntl);
            }
            catch (...)
            {
                tryLogCurrentException(log);
                LOG_WARNING(
                    log,
                    "fail to refresh/invalidate cache on {} when updating table {}, expired cache on this server is possible",
                    addr_text,
                    table_identifier.getNameForLogs());
            }
        }
    }
}

StatisticsSettings fetchStatisticsSettings(ContextPtr context)
{
    (void)context;
    throw Exception("not implemented", ErrorCodes::NOT_IMPLEMENTED);
#if 0
    auto log = getLogger("fetchStatisticsSettings");
    Protos::FetchStatisticsSettingsRequest req;
    auto * manager = context->getAutoStatisticsManager();
    auto leader_addr = manager->getZkHelper().getLeaderAddr();
    brpc::Controller cntl;
    auto addr_text = createHostPortString(leader_addr.host, leader_addr.exchange_status_port);
    LOG_INFO(log, "send fetchStatisticsSettings request to {}", addr_text);
    auto rpc_client = RpcChannelPool::getInstance().getClient(addr_text, BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY);
    auto stub_ptr = std::make_shared<Protos::OptimizerStatisticsService_Stub>(&rpc_client->getChannel());
    Protos::FetchStatisticsSettingsResponse resp;
    stub_ptr->fetchStatisticsSettings(&cntl, &req, &resp, nullptr);
    rpc_client->assertController(cntl);
    StatisticsSettings result;
    result.fillFromProto(resp.statistics_settings());
    return result;
#endif
}

// std::map<UUID, UInt64> queryUdiCounter(ContextPtr context)
// {
//     (void)context;
//     auto log = getLogger("queryUdiCounter");

//     std::map<UUID, UInt64> result;

//     auto servers = DaemonManager::DaemonJobAutoStatistics::getServerList(context);

//     for (const auto & addr : servers)
//     {
//         auto addr_text = addr.getRPCAddress();
//         try
//         {
//             Protos::QueryUdiCounterRequest req;
//             brpc::Controller cntl;
//             auto rpc_client = RpcChannelPool::getInstance().getClient(addr_text, BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY);
//             auto stub_ptr = std::make_shared<Protos::OptimizerStatisticsService_Stub>(&rpc_client->getChannel());
//             Protos::QueryUdiCounterResponse resp;
//             stub_ptr->queryUdiCounter(&cntl, &req, &resp, nullptr);
//             LOG_INFO(log, "send queryUdiCounter request to {}", addr_text);
//             rpc_client->assertController(cntl);

//             for (const auto & record : resp.records())
//             {
//                 auto uuid = RPCHelpers::createUUID(record.uuid());
//                 result[uuid] += record.udi_count();
//             }
//         }
//         catch (...)
//         {
//             LOG_WARNING(log, "failed to query udi counter on {}, skipping", addr_text);
//             tryLogCurrentException(log);
//         }
//     }
//     return result;
// }
}
