#pragma once

#include <Interpreters/Context_fwd.h>
#include <Core/BackgroundSchedulePool.h>
#include <MergeTreeCommon/CnchServerTopology.h>
#include <Coordination/LeaderElectionBase.h>

namespace zkutil
{
    class LeaderElection;
    class ZooKeeper;
    using ZooKeeperPtr = std::shared_ptr<ZooKeeper>;
}

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
class CnchServerManager: public WithContext, public LeaderElectionBase
{
using Topology = CnchServerTopology;

public:
    explicit CnchServerManager(ContextPtr context_);

    ~CnchServerManager() override;

    bool isLeader() {return is_leader;}

    void shutDown();
    void partialShutdown();

private:
    void onLeader() override;
    void exitLeaderElection() override;
    void enterLeaderElection() override;

    void refreshTopology();
    void renewLease();

    /// set topology status when becoming leader. may runs in background tasks.
    void setLeaderStatus();

    Poco::Logger * log = &Poco::Logger::get("CnchServerManager");

    BackgroundSchedulePool::TaskHolder topology_refresh_task;
    BackgroundSchedulePool::TaskHolder lease_renew_task;

    std::optional<Topology> next_version_topology;
    std::list<Topology> cached_topologies;
    mutable std::mutex topology_mutex;

    std::atomic_bool need_stop{false};
    std::atomic_bool is_leader{false};
    std::atomic_bool leader_initialized{false};
};

using CnchServerManagerPtr = std::shared_ptr<CnchServerManager>;

}
