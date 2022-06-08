#pragma once

#include <Interpreters/Context.h>
#include <MergeTreeCommon/CnchServerTopology.h>
#include <Core/BackgroundSchedulePool.h>

#if BYTEJOURNAL_AVAILABLE
#include <bytejournal/sdk/leader_election_runner.h>
#endif

namespace DB
{

/***
 * CnchServerManager is used to synchronize topology of current cluster in metastore with that in consul.
 * It contains two kind of background tasks:
 * 1. Topology refresh task. This task periodically get current servers topology from consul.
 * 2. Lease renew task. This task is responsible for periodically update the topology to metastore.
 *
 * Leader election is required to make sure only one CnchServerManager can update server topology at a time.
 */
class CnchServerManager
{

struct  LeaderElectionResult
{
    String leader_host_port {};
    bool is_leader{false};
};

using Topology = CnchServerTopology;

public:
    CnchServerManager(Context & context_);

    ~CnchServerManager();

    void dumpServerStatus();

    void shutDown();

private:
    void startLeaderElection();

    void initByteJournalClient();
    void onLeader(const String & leader_addr);
    void onFollower(const String & leader_addr);
    LeaderElectionResult getLeaderElectionResult();
    void setLeaderElectionResult(const String & leader_addr, const bool is_leader);
    void checkLeaderInfo(const UInt64 & check_interval);

    void refreshTopology();
    void renewLease();

    bool waitFor(const UInt64 & period);

    /// set topology status when becoming leader. may runs in background tasks.
    void setLeaderStatus();

    Poco::Logger * log = &Poco::Logger::get("CnchServerManager");
    Context & context;
    #if BYTEJOURNAL_AVAILABLE
    ByteJournalClientPtr bj_client;
    std::unique_ptr<::bytejournal::sdk::LeaderElectionRunner> leader_runner;
    #endif
    BackgroundSchedulePool::TaskHolder topology_refresh_task;
    BackgroundSchedulePool::TaskHolder lease_renew_task;
    BackgroundSchedulePool::TaskHolder leader_info_checker;
    LeaderElectionResult leader_election_result;
    std::optional<Topology> next_version_topology;
    std::list<Topology> cached_topologies;
    mutable std::mutex topology_mutex;
    mutable std::mutex leader_election_mutex;

    String election_ns;
    String election_point;

    std::atomic_bool leader_initialized {false};
};

using CnchServerManagerPtr = std::shared_ptr<CnchServerManager>;

}

