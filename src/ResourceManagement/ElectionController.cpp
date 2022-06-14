#include <ResourceManagement/ElectionController.h>
#include <Catalog/Catalog.h>
#include <common/getFQDNOrHostName.h>
#include <ResourceManagement/ResourceTracker.h>
#include <ResourceManagement/VirtualWarehouseManager.h>
#include <ResourceManagement/WorkerGroupManager.h>
#include <ServiceDiscovery/IServiceDiscovery.h>
#include <Storages/PartCacheManager.h>
// #include <TSO/TSOClient.h>
// #include <WAL/ByteJournalCommon.h>
// #include <bytejournal/sdk/client.h>
// #include <bytejournal/sdk/options.h>

namespace DB::ResourceManagement
{

ElectionController::ElectionController(ResourceManagerController & rm_controller_) : rm_controller(rm_controller_)
{
    #if BYTEJOURNAL_AVAILABLE
    bj_client = getContext()->getByteJournalClient();
    #endif

    auto & config = getContext()->getConfigRef();
    election_ns = config.getString("bytejournal.namespace");
    election_point = config.getString("bytejournal.cnch_prefix", "default_cnch_ci_random_") + "rm_election_point";
    LOG_DEBUG(&Poco::Logger::get("ElectionController"), 
                "Running for leader election in namespace: {}, election_point: {}",
                election_ns, election_point);
    UInt64 interval = getContext()->getConfigRef().getUInt64("resource_manager.check_leader_info_interval_ms", 1000);

    leader_info_checker = getContext()->getSchedulePool().createTask("LeaseInfoChecker", [this, interval](){
        try
        {
            checkLeaderInfo(interval);
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
        leader_info_checker->scheduleAfter(interval);
    });
    leader_info_checker->activateAndSchedule();

    /// start leader election
   startLeaderElection();
}

ElectionController::~ElectionController()
{
    shutDown();
}

ContextPtr ElectionController::getContext() const
{
     return rm_controller.getContext();
}

void ElectionController::startLeaderElection()
{
    #if BYTEJOURNAL_AVAILABLE
    ::bytejournal::sdk::LeaderElectionOptions election_opts;
    election_opts.role = bytejournal::sdk::LeaderElectionOptions::Role::kCandidate;
    auto sd = getContext()->getServiceDiscoveryClient();
    String host_port;
    int port;
    if (sd->getName() != "local")
    {
        const char * rpc_port = getenv("PORT0");
        const char * rm_host = getenv("RESOURCE_MANAGER_IP");
        if(rpc_port != NULL && rm_host != NULL)
        {
            port = atoi(rpc_port);
            host_port = createHostPortString(rm_host, port);
        }
        else
        {
            LOG_ERROR(log, "RESOURCE_MANAGER_IP and/or PORT0 not available in env");
        }
    }
    else
    {
        auto & config = getContext()->getConfigRef();
        port = config.getInt("resource_manager.port", 8989);
        host_port = createHostPortString(config.getString("service_discovery.resource_manager.node.host", "127.0.0.1"), port);

    }
    election_opts.addr = host_port;
    election_opts.on_leader_cb = [&](String addr) { onLeader(addr); };
    election_opts.on_follower_cb = [&](String addr) { onFollower(addr); };
    election_opts.renewal_interval_ms = getContext()->getConfigRef().getInt(config_prefix + ".bytejournal.renewal_interval_ms", 1000);
    election_opts.polling_interval_ms = getContext()->getConfigRef().getInt(config_prefix + ".bytejournal.polling_interval_ms", 1000);
    election_opts.error_retry_interval_ms = getContext()->getConfigRef().getInt(config_prefix + ".bytejournal.error_retry_interval_ms", 1000);
    election_opts.lease_interval_ms = getContext()->getConfigRef().getInt(config_prefix + ".bytejournal.lease_interval_ms", 3000);
    election_opts.failover_interval_ms = getContext()->getConfigRef().getInt(config_prefix + ".bytejournal.failover_interval_ms", 5000);

    leader_runner = getResult<std::unique_ptr<::bytejournal::sdk::LeaderElectionRunner>>(bj_client->CreateLeaderElectionRunner(election_ns, election_point, election_opts));
    leader_runner->Start();
    #endif
}

void ElectionController::onLeader(const String & leader_addr)
{
    LOG_INFO(log, "Starting leader callback for " + leader_addr);
    setLeaderElectionResult({}, false);
    waitFor(getContext()->getConfigRef().getInt(config_prefix + ".wait_before_become_leader_ms", 3000));
    if (!pullState())
    {
        #if BYTEJOURNAL_AVAILABLE
        LOG_DEBUG(log, "Failed to initalise, resigning.");

        // Start background thread for resignation, since synchronous execution does not work
        ThreadFromGlobalPool([this] {
            auto failover_interval_ms = getContext()->getConfigRef().getInt(config_prefix + ".bytejournal.failover_interval_ms", 5000);
            leader_runner->Resign(failover_interval_ms);
        }).detach();
        #endif
    }
    else
    {
        setLeaderElectionResult(leader_addr, true);
        LOG_INFO(log, "Current RM node " + leader_addr + " has become leader. ");
    }
}

void ElectionController::onFollower(const String & leader_addr)
{
    setLeaderElectionResult(leader_addr, false);
    LOG_INFO(log, "Current RM node has become follower. Leader is now {}", leader_addr);
}

ElectionController::LeaderElectionResult ElectionController::getLeaderElectionResult()
{
    std::unique_lock<std::mutex> lock(leader_election_mutex);
    return leader_election_result;
}

void ElectionController::setLeaderElectionResult(const String & leader_addr, const bool is_leader)
{
    std::unique_lock<std::mutex> lock(leader_election_mutex);
    leader_election_result.leader_host_port = leader_addr;
    leader_election_result.is_leader = is_leader;
}

bool ElectionController::pullState()
{
    auto retry_count = 3;
    auto success = false;
    do
    {
        try
        {
            // Clear outdated data
            auto & vw_manager = rm_controller.getVirtualWarehouseManager();
            vw_manager.clearVirtualWarehouses();
            auto & group_manager = rm_controller.getWorkerGroupManager();
            group_manager.clearWorkerGroups();
            rm_controller.initialize();
            success = true;
        }
        catch (...)
        {
            tryLogCurrentException(log);
            --retry_count;
        }
    } while (!success && retry_count > 0);

    return success;
}

void ElectionController::checkLeaderInfo(const UInt64 & check_interval)
{
    if (!getLeaderElectionResult().is_leader || check_interval == 0)
        return;

    #if BYTEJOURNAL_AVAILABLE
    int max_retry_times = getContext()->getConfigRef().getUInt64(config_prefix + ".max_retry_times", 3);
    int retry_count = 0;
    while (retry_count++ < max_retry_times)
    {
        try
        {
            auto new_leader_addr = getResult(bj_client->GetLeaderInfo(election_ns, election_point)).addr;
            if (new_leader_addr != getLeaderElectionResult().leader_host_port)
            {
                LOG_DEBUG(log, "New leader [" << new_leader_addr << "] has been selected, yielding leadership.");
                setLeaderElectionResult(new_leader_addr, false);
            }
            break;
        }
        catch (...)
        {
            LOG_DEBUG(log, "Failed to get leader info from ByteJournal, retry for " << retry_count << " times.");
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            waitFor(50);
        }
    }
    #endif
}

bool ElectionController::waitFor(const UInt64 & period)
{
    auto event = std::make_shared<Poco::Event>();
    return event->tryWait(period);
}

void ElectionController::shutDown()
{
    try
    {
        leader_info_checker->deactivate();
        #if BYTEJOURNAL_AVAILABLE
        leader_runner->Resign(0);
        leader_runner->Stop();
        #endif
    }
    catch (...)
    {
        LOG_ERROR(log, "Exception while shutting down.");
    }
}

}
