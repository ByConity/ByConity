#include <iterator>
#include <netdb.h>
#include <DaemonManager/registerDaemons.h>
#include <DaemonManager/DaemonJob.h>
#include <DaemonManager/DaemonFactory.h>
#include <DaemonManager/DaemonJobAutoStatistics.h>
#include <CloudServices/CnchServerClient.h>
#include <CloudServices/CnchServerClientPool.h>
#include <Statistics/AutoStatisticsManager.h>
#include <Statistics/CatalogAdaptor.h>
#include <fmt/format.h>

namespace DB::ErrorCodes
{

}

namespace DB::DaemonManager
{

using namespace Statistics;
using namespace Statistics::AutoStats;
using namespace AutoStatsImpl;

bool DaemonJobAutoStatistics::executeImpl()
{
    try
    {
        auto context = getContext();
        auto servers = getServerList(context);
        // do nothing
        if (servers.empty()) {
            LOG_INFO(logger, "No server to collect statistics");
            return false;
        }

        // set primary server as server 0
        auto primary_server_cli = context->getCnchServerClientPool().get(servers[0]);

        // gather all memory record into primary server
        // then scatter them to all servers
        // this action is sync, since it should be fast
        LOG_INFO(logger, "Distribute udi counter with primary server {}", servers[0].toDebugString());
        primary_server_cli->scheduleDistributeUdiCount();

        for (auto & host_with_ports : servers)
        {
            auto server_cli = context->getCnchServerClientPool().get(host_with_ports);
            // TODO: rewrite it
            // trigger auto stats execution on each server
            // daemon manager will do nothing
            // this action is slow, so use async
            LOG_INFO(logger, "Trigger collect task on server {}", host_with_ports.toDebugString());
            server_cli->scheduleAutoStatsCollect();
        }
    }
    catch (...)
    {
        tryLogCurrentException(logger);
        return false;
    }
    return true;
}

HostWithPortsVec DaemonJobAutoStatistics::getServerList(const ContextPtr & ctx)
{
    auto topology_master = ctx->getCnchTopologyMaster();
    if (!topology_master)
    {
        throw Exception("Failed to get topology master, skip iteration", ErrorCodes::LOGICAL_ERROR);
    }

    std::list<CnchServerTopology> server_topologies = topology_master->getCurrentTopology();
    if (server_topologies.empty())
    {
        throw Exception("Server topology is empty, something wrong with topology, return empty result", ErrorCodes::LOGICAL_ERROR);
    }

    HostWithPortsVec host_ports_vec = server_topologies.back().getServerList();
    if (host_ports_vec.empty())
    {
        throw Exception("Server topology is empty, something wrong with serverList, return empty result", ErrorCodes::LOGICAL_ERROR);
    }
    return host_ports_vec;
}

void registerAutoStatisticsDaemon(DaemonFactory & factory)
{
    factory.registerLocalDaemonJob<DaemonJobAutoStatistics>("AUTO_STATISTICS");
}
}

