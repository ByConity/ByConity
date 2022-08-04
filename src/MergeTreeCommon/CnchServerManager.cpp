#include <MergeTreeCommon/CnchServerManager.h>
#include <common/getFQDNOrHostName.h>
#include <Catalog/Catalog.h>
#include <Storages/PartCacheManager.h>
#include <ServiceDiscovery/IServiceDiscovery.h>
#if BYTEJOURNAL_AVAILABLE
#include <WAL/ByteJournalCommon.h>
#include <bytejournal/sdk/client.h>
#include <bytejournal/sdk/options.h>
#endif

namespace DB
{

CnchServerManager::CnchServerManager(Context & context_)
    :context(context_)
{
    #if BYTEJOURNAL_AVAILABLE
    bj_client = context.getByteJournalClient();
    election_ns = context.getConfigRef().getString("bytejournal.namespace", "server_namespace_default");
    election_point = context.getConfigRef().getString("bytejournal.cnch_prefix", "default_cnch_ci_random_")
        + "server_election_point";
    #endif

    topology_refresh_task = context.getTopologySchedulePool().createTask("TopologyRefresher", [this](){
            try
            {
                refreshTopology();
            }
            catch (...)
            {
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
            }
            topology_refresh_task->scheduleAfter(context.getSettings().topology_refresh_interval_ms.totalMilliseconds());
        });
    topology_refresh_task->activateAndSchedule();

    lease_renew_task = context.getTopologySchedulePool().createTask("LeaseRenewer", [this](){
        try
        {
            renewLease();
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
        lease_renew_task->scheduleAfter(context.getSettings().topology_lease_renew_interval_ms.totalMilliseconds());
    });
    lease_renew_task->activateAndSchedule();

    #if BYTEJOURNAL_AVAILABLE
    leader_info_checker = context.getTopologySchedulePool().createTask("LeaseInfoChecker", [this](){
        UInt64 check_leader_info_interval = context.getConfigRef().getUInt64("server_leader_election.check_leader_info_interval_ms", 1000);
        try
        {
            checkLeaderInfo(check_leader_info_interval);
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
        leader_info_checker->scheduleAfter(check_leader_info_interval);
    });
    leader_info_checker->activateAndSchedule();

    /// start leader election
   startLeaderElection();
   #endif
}

CnchServerManager::~CnchServerManager()
{
    shutDown();
}

void CnchServerManager::startLeaderElection()
{
    #if BYTEJOURNAL_AVAILABLE
    ::bytejournal::sdk::LeaderElectionOptions election_opts;
    election_opts.addr = getIPOrFQDNOrHostName();
    election_opts.on_leader_cb = [&](String addr) { onLeader(addr); };
    election_opts.on_follower_cb = [&](String addr) { onFollower(addr); };
    election_opts.renewal_interval_ms = context.getConfigRef().getInt("server_leader_election.bytejournal.renewal_interval_ms", 1000);
    election_opts.polling_interval_ms = context.getConfigRef().getInt("server_leader_election.bytejournal.polling_interval_ms", 1000);
    election_opts.error_retry_interval_ms = context.getConfigRef().getInt("server_leader_election.bytejournal.error_retry_interval_ms", 1000);
    election_opts.lease_interval_ms = context.getConfigRef().getInt("server_leader_election.bytejournal.lease_interval_ms", 3000);
    election_opts.failover_interval_ms = context.getConfigRef().getInt("server_leader_election.bytejournal.failover_interval_ms", 5000);

    leader_runner = getResult<std::unique_ptr<::bytejournal::sdk::LeaderElectionRunner>>(bj_client->CreateLeaderElectionRunner(election_ns, election_point, election_opts));
    leader_runner->Start();
    #endif
}

void CnchServerManager::onLeader(const String & leader_addr)
{
    setLeaderElectionResult({}, false);
    waitFor(context.getConfigRef().getInt("server_leader_election.wait_before_become_leader_ms", 3000));
    try
    {
        setLeaderStatus();
    } catch (...)
    {
        LOG_ERROR(log, "Failed to set leader status when current node becoming leader.");
    }
    setLeaderElectionResult(leader_addr, true);
    LOG_INFO(log, "Current node {} has become leader. ", leader_addr);
}

void CnchServerManager::onFollower(const String & leader_addr)
{
    setLeaderElectionResult(leader_addr, false);
    leader_initialized = false;
    LOG_INFO(log, "Current node has become follower. Leader transfer to {}.", leader_addr);
}

CnchServerManager::LeaderElectionResult CnchServerManager::getLeaderElectionResult()
{
    std::unique_lock<std::mutex> lock(leader_election_mutex);
    return leader_election_result;
}

void CnchServerManager::setLeaderElectionResult(const String & leader_addr, const bool is_leader)
{
    std::unique_lock<std::mutex> lock(leader_election_mutex);
    leader_election_result.leader_host_port = leader_addr;
    leader_election_result.is_leader = is_leader;
}

void CnchServerManager::checkLeaderInfo([[maybe_unused]]const UInt64 & check_interval)
{
    #if BYTEJOURNAL_AVAILABLE
    if (!getLeaderElectionResult().is_leader || check_interval == 0)
        return;

    int max_retry_times = context.getConfigRef().getUInt64("server_leader_election.max_retry_times", 3);
    int retry_count = 0;
    while (retry_count++ < max_retry_times)
    {
        try
        {
            auto new_leader_addr = getResult(bj_client->GetLeaderInfo(election_ns, election_point)).addr;
            if (new_leader_addr != getLeaderElectionResult().leader_host_port)
            {
                LOG_DEBUG(log, "New leader [{}] has been selected, yielding leadership.", new_leader_addr);
                setLeaderElectionResult(new_leader_addr, false);
            }
            break;
        }
        catch (...)
        {
            LOG_ERROR(log, "Failed to get leader info from ByteJournal, retry for {} times.", retry_count);
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            waitFor(50);
        }
    }
    #endif
}

void CnchServerManager::refreshTopology()
{
    #if BYTEJOURNAL_AVAILABLE
    if (!getLeaderElectionResult().is_leader || !leader_initialized)
        return;
    #endif

    auto service_discovery_client = context.getServiceDiscoveryClient();
    String psm = context.getConfigRef().getString("service_discovery.server.psm", "data.cnch.server");
    HostWithPortsVec server_vector = service_discovery_client->lookup(psm, ComponentType::SERVER);
    if (server_vector.empty())
    {
        LOG_ERROR(log, "Failed to get any server from service discovery, psm : " + psm);
        return;
    }
    /// FIXME: since no leader election available, we now only support one server in cluster, remove this logic later.
    else if (server_vector.size() > 1)
    {
        LOG_ERROR(log, "More than one server in cluster is not supportted now, psm : " + psm);
        return;
    }

    /// keep the servers sorted by host address to make it comparable
    std::sort(server_vector.begin(), server_vector.end(), [](auto & lhs, auto & rhs) {
        return std::forward_as_tuple(lhs.id, lhs.host, lhs.rpc_port) < std::forward_as_tuple(rhs.id, rhs.host, rhs.rpc_port);
    });

    {
        std::unique_lock<std::mutex> lock(topology_mutex);
        if (cached_topologies.empty() || !HostWithPorts::isExactlySameVec(cached_topologies.back().getServerList(), server_vector))
            next_version_topology = Topology(UInt64{0}, std::move(server_vector));
    }
}

void CnchServerManager::renewLease()
{
    #if BYTEJOURNAL_AVAILABLE
    if (!getLeaderElectionResult().is_leader)
        return;

    if (!leader_initialized)
        setLeaderStatus();
    #endif

    UInt64 current_time_ms = context.getTimestamp()>>18;
    UInt64 lease_life_ms = context.getSettings().topology_lease_life_ms.totalMilliseconds();

    std::list<Topology> copy_topologies;
    {
        std::unique_lock<std::mutex> lock(topology_mutex);

        ///clear outdated lease
        while (!cached_topologies.empty() && cached_topologies.front().getExpiration() < current_time_ms)
        {
            LOG_DEBUG(log, "Removing expired topology : {}", cached_topologies.front().format());
            cached_topologies.pop_front();
        }

        if (cached_topologies.size() == 0)
        {
            if (next_version_topology)
            {
                next_version_topology->setExpiration(current_time_ms + lease_life_ms);
                cached_topologies.push_back(*next_version_topology);
                LOG_DEBUG(log, "Add new topology {}", cached_topologies.back().format());
            }
        }
        else if (cached_topologies.size() == 1)
        {
            if (next_version_topology)
            {
                UInt64 latest_lease_time = cached_topologies.back().getExpiration();
                next_version_topology->setExpiration(latest_lease_time + lease_life_ms);
                cached_topologies.push_back(*next_version_topology);
                LOG_DEBUG(log, "Add new topology {}", cached_topologies.back().format());
            }
            else
            {
                cached_topologies.back().setExpiration(current_time_ms + lease_life_ms);
            }
        }
        else
        {
            LOG_WARNING(log, "Cannot renew lease because there is one pending topology. Current ts : {}, current topology : {}", current_time_ms, dumpTopologies(cached_topologies));
        }

        next_version_topology.reset();
        copy_topologies = cached_topologies;
    }

    context.getCnchCatalog()->updateTopologies(copy_topologies);
}

void CnchServerManager::setLeaderStatus()
{
    std::unique_lock<std::mutex> lock(topology_mutex);
    cached_topologies = context.getCnchCatalog()->getTopologies();
    leader_initialized = true;
    LOG_DEBUG(log , "Successfully set leader status.");
}

bool CnchServerManager::waitFor(const UInt64 & period)
{
    auto event = std::make_shared<Poco::Event>();
    return event->tryWait(period);
}

void CnchServerManager::dumpServerStatus()
{
    std::stringstream ss;
    ss << "[leader selection result] : \n"
       << "is_leader : " <<  getLeaderElectionResult().is_leader << std::endl
       << "leader_info : " << getLeaderElectionResult().leader_host_port << std::endl
       << "[current cached topology] : \n";

    {
        std::unique_lock<std::mutex> lock(topology_mutex);
        ss << DB::dumpTopologies(cached_topologies);
    }
    LOG_DEBUG(log, "Dump server status : \n{}",ss.str());
}

void CnchServerManager::shutDown()
{
    try
    {
        topology_refresh_task->deactivate();
        lease_renew_task->deactivate();
        #if BYTEJOURNAL_AVAILABLE
        leader_info_checker->deactivate();
        leader_runner->Stop();
        #endif
    }
    catch (...)
    {
        LOG_ERROR(log, "Exception while shutting down.");
    }
}

}
